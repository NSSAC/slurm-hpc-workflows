"""Manage Slurm Jobs."""

import os
import re
import time
import subprocess
from pathlib import Path
from typing import Generator
from datetime import datetime

import jinja2
from pydantic import BaseModel

from .utils import Closeable, find_sbatch

COMMAND_TIMEOUT = 120

PRESERVE_ENV = ["USER", "HOME", "PATH", "PYTHONPATH"]

SBATCH_OUTPUT_REGEX = re.compile(r"Submitted batch job (?P<id>\S*)")

SQUEUE_CHECK_INTERVAL = 5  # seconds

ENVIRONMENT = jinja2.Environment(
    undefined=jinja2.StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
)


def get_running_jobids(squeue_exe: str, slurm_user: str, timeout: int) -> set[int]:
    """Get the running Slurm job IDs for the given Slurm user."""
    cmd = [squeue_exe, "-u", slurm_user, "--noheader", "-o", "%A"]

    proc = subprocess.run(
        cmd,
        capture_output=True,
        check=True,
        text=True,
        timeout=timeout,
    )
    job_ids = proc.stdout.strip().split()
    job_ids = set(int(j) for j in job_ids)
    return job_ids


def cancel_jobs(scancel_exe: str, job_ids: list[int], timeout: int):
    """Run scancel command for the given jobids."""
    if not job_ids:
        return

    cmd = [scancel_exe] + [str(id) for id in job_ids]
    subprocess.run(cmd, capture_output=True, check=True, text=True, timeout=timeout)


class SlurmJob(BaseModel):
    """A submitted slurm job."""

    name: str
    sbatch_args: list[str]
    script: str

    job_id: int
    is_running: bool
    output_file: Path


SCRIPT_TEMPLATE_TEXT = r"""
#!/bin/bash
#SBATCH --job-name "{{ name }}"
{% for sbatch_arg in sbatch_args %}
#SBATCH {{ sbatch_arg }}
{% endfor %}
#SBATCH --output "{{ output_file }}"

{{ script }}
"""
SCRIPT_TEMPLATE = ENVIRONMENT.from_string(SCRIPT_TEMPLATE_TEXT.strip())


def submit_sbatch_job(
    sbatch_exe: str,
    name: str,
    sbatch_args: list[str],
    script: str,
    work_dir: Path,
    preserve_env: list[str],
    timeout: int,
) -> SlurmJob:
    """Submit a sbatch job."""

    # Create clean environment
    env = {}
    for key in preserve_env:
        if key in os.environ:
            env[key] = os.environ[key]

    # Figure out the output and error file names.
    output_file = str(work_dir / f"{name}-%j.out")

    # Create the sbatch script
    script_path = work_dir / f"{name}.sh"
    script_text = SCRIPT_TEMPLATE.render(
        name=name,
        sbatch_args=sbatch_args,
        script=script,
        output_file=output_file,
    )
    script_path.write_text(script_text)
    os.chmod(script_path, mode=0o755)

    # Run sbatch
    proc = subprocess.run(
        [sbatch_exe, str(script_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )

    # Extract job id
    match = SBATCH_OUTPUT_REGEX.match(proc.stdout.strip())
    if match is None:
        raise RuntimeError("Failed to parse sbatch output", proc, match)
    job_id = match.group("id")
    job_id = int(job_id)

    # Resolve the file names
    output_file = Path(output_file.replace("%j", str(job_id)))

    return SlurmJob(
        name=name,
        sbatch_args=sbatch_args,
        script=script,
        job_id=job_id,
        is_running=True,
        output_file=output_file,
    )


class SlurmJobManager(Closeable):
    """Manage slurm jobs."""

    def __init__(
        self,
        work_dir: Path | str,
        sbatch_exe: Path | str | None = None,
        slurm_user: str | None = None,
        command_timeout: int = COMMAND_TIMEOUT,
        preserve_env: list[str] = PRESERVE_ENV,
    ):
        """Initialize.

        Args:
            work_dir: Path where log files and scripts will be created.
            sbatch_exe: Path to sbatch executable.
                If None, will lookup location in PATH.
            slurm_user: Username of the Slurm user.
                If None, will use USER environment variable.
            command_timeout: Timeout for executing commands.
            preserve_env: Enviroment variables to preseve in slurm jobs environment.
                Enviroment varimables not specified will be removed.
                This is necessary so that Slurm generated variables from parent job
                do not conflict with Slurm generated variables in the child job.
        """
        work_dir = Path(work_dir)

        sbatch_exe = find_sbatch(sbatch_exe)
        self.sbatch_exe = str(sbatch_exe)
        self.squeue_exe = str(sbatch_exe.parent / "squeue")
        self.scancel_exe = str(sbatch_exe.parent / "scancel")

        if slurm_user is None:
            slurm_user = os.environ["USER"]
        self.slurm_user = slurm_user

        self.command_timeout = command_timeout

        self.preserve_env = preserve_env

        self.work_dir = work_dir
        if not self.work_dir.exists():
            work_dir.mkdir(parents=True, exist_ok=True)

        # job name -> type
        self.jobs: dict[str, SlurmJob] = {}

        # job_id -> type (only for running jobs)
        self.running_jobs: dict[int, SlurmJob] = {}

        self.running_job_ids: set[int] = get_running_jobids(
            self.squeue_exe, self.slurm_user, self.command_timeout
        )
        self.last_check = datetime.now()

    def submit(self, name: str, sbatch_args: list[str], script: str) -> SlurmJob:
        """Submit a Slurm job.

        Args:
            name: Name of the job. All jobs should have a unique name.
            sbatch_args: The arguments to sbatch.
                Each element in the list will be put on a separate line.
            script: Body of the job script.
        """
        if name in self.jobs:
            raise ValueError(f"Job '{name}' is already defined.")

        job = submit_sbatch_job(
            sbatch_exe=self.sbatch_exe,
            name=name,
            sbatch_args=sbatch_args,
            script=script,
            work_dir=self.work_dir,
            preserve_env=self.preserve_env,
            timeout=self.command_timeout,
        )

        self.jobs[name] = job
        self.running_jobs[job.job_id] = job
        return job

    def _sleep_until_next_check_time(self) -> None:
        now = datetime.now()
        seconds_since_last_check = (now - self.last_check).total_seconds()
        sleep_time = SQUEUE_CHECK_INTERVAL - seconds_since_last_check
        if sleep_time > 0:
            time.sleep(sleep_time)

    def _update_running_jobs(self):
        self.running_job_ids = get_running_jobids(
            self.squeue_exe, self.slurm_user, self.command_timeout
        )
        self.last_check = datetime.now()

        for job_id in list(self.running_jobs):
            if job_id not in self.running_job_ids:
                self.running_jobs[job_id].is_running = False
                del self.running_jobs[job_id]

    def poll(self) -> int:
        """Return the number of running jobs."""
        self._sleep_until_next_check_time()
        self._update_running_jobs()
        return len(self.running_jobs)

    def wait(self, jobs: list[SlurmJob] | None = None) -> None:
        """Wait until the given list of jobs are done."""
        if jobs is None:
            jobs = list(self.running_jobs.values())

        running_jobs = [job for job in jobs if job.is_running]
        while running_jobs:
            self._sleep_until_next_check_time()
            self._update_running_jobs()

            running_jobs = [job for job in running_jobs if job.is_running]

    def as_completed(
        self, jobs: list[SlurmJob] | None = None
    ) -> Generator[SlurmJob, None, None]:
        """Yield jobs as they are completed."""
        if jobs is None:
            jobs = list(self.running_jobs.values())

        for job in jobs:
            if not job.is_running:
                yield job
        running_jobs = [job for job in jobs if job.is_running]

        while running_jobs:
            self._sleep_until_next_check_time()
            self._update_running_jobs()

            for job in running_jobs:
                if not job.is_running:
                    yield job
            running_jobs = [job for job in running_jobs if job.is_running]

    def cancel(self, job: SlurmJob) -> None:
        """Stop a given job."""
        cancel_jobs(self.scancel_exe, [job.job_id], self.command_timeout)

    def close(self):
        """Shutdown all running jobs."""
        self._update_running_jobs()

        running_job_ids: list[int] = []
        for job in self.jobs.values():
            if job.is_running:
                running_job_ids.append(job.job_id)

        cancel_jobs(self.scancel_exe, running_job_ids, self.command_timeout)
