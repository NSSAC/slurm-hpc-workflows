"""Manage Slurm Jobs."""

import os
import re
import subprocess
from pathlib import Path
from dataclasses import dataclass

from .utils import find_sbatch
from .templates import render_template

COMMAND_TIMEOUT = 120

SBATCH_OUTPUT_REGEX = re.compile(r"Submitted batch job (?P<id>\S*)")

# SQUEUE_CHECK_INTERVAL = 5  # seconds

SLURM_USER = os.environ["USER"]

CLEAN_ENVIRON: dict

SBATCH_EXE: str
SQUEUE_EXE: str
SCANCEL_EXE: str


def _get_clean_environ() -> dict[str, str]:
    """Create environment dict without SLURM set variables.

    This is an issue when submitting Slurm jobs from within Slurm jobs.
    """
    sanitized_env: dict[str, str] = {}
    for k, v in os.environ.items():
        if (
            k.startswith("PMI_")
            or k.startswith("SLURM_")
            or k.startswith("SLURMD_")
            or k.startswith("SRUN_")
        ):
            continue

        sanitized_env[k] = v
    return sanitized_env


def _init_globals():
    global CLEAN_ENVIRON
    global SBATCH_EXE, SQUEUE_EXE, SCANCEL_EXE

    CLEAN_ENVIRON = _get_clean_environ()

    sbatch_exe = find_sbatch(None)
    SBATCH_EXE = str(sbatch_exe)
    SQUEUE_EXE = str(sbatch_exe.parent / "squeue")
    SCANCEL_EXE = str(sbatch_exe.parent / "scancel")


_init_globals()


def set_sbatch_exe_path(sbatch_exe: Path | str):
    global SBATCH_EXE, SQUEUE_EXE, SCANCEL_EXE

    sbatch_exe = find_sbatch(sbatch_exe)
    SBATCH_EXE = str(sbatch_exe)
    SQUEUE_EXE = str(sbatch_exe.parent / "squeue")
    SCANCEL_EXE = str(sbatch_exe.parent / "scancel")


def set_slurm_user(slurm_user: str):
    global SLURM_USER

    SLURM_USER = slurm_user


def set_command_timeout(timeout: int):
    global COMMAND_TIMEOUT

    COMMAND_TIMEOUT = timeout


def get_running_jobids() -> set[int]:
    """Get the running Slurm job IDs for the given Slurm user."""
    cmd = [SQUEUE_EXE, "-u", SLURM_USER, "--noheader", "-o", "%A"]

    proc = subprocess.run(
        cmd,
        capture_output=True,
        check=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
    )
    job_ids = proc.stdout.strip().split()
    job_ids = set(int(j) for j in job_ids)
    return job_ids


def cancel_jobs(
    job_ids: list[int],
    term: bool = False,
    batch: bool = False,
    full: bool = False,
):
    """Run scancel command for the given job ids."""
    if not job_ids:
        return

    cmd = [SCANCEL_EXE]
    if term:
        cmd.append("--signal=TERM")
    if batch:
        cmd.append("--batch")
    if full:
        cmd.append("--full")
    cmd.extend([str(id) for id in job_ids])

    subprocess.run(
        cmd, capture_output=True, check=True, text=True, timeout=COMMAND_TIMEOUT
    )


@dataclass
class SlurmJob:
    """A submitted Slurm job."""

    name: str
    sbatch_args: list[str]
    script: str

    job_id: int
    output_file: Path


def submit_sbatch_job(
    name: str,
    sbatch_args: list[str],
    script: str,
    work_dir: Path,
) -> SlurmJob:
    """Submit a sbatch job."""
    # Figure out the output and error file names.
    output_file = str(work_dir / f"{name}-%j.out")

    # Create the sbatch script
    script_path = work_dir / f"{name}.sh"
    script_text = render_template(
        "slurm_job_manager:script_template",
        name=name,
        sbatch_args=sbatch_args,
        script=script,
        output_file=output_file,
    )
    script_path.write_text(script_text)
    os.chmod(script_path, mode=0o755)

    # Run sbatch
    proc = subprocess.run(
        [SBATCH_EXE, str(script_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT,
        env=CLEAN_ENVIRON,
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
        output_file=output_file,
    )


# class SlurmJobManager(Closeable):
#     """Manage Slurm jobs."""
#
#     def __init__(
#         self,
#         work_dir: Path | str | None = None,
#         cancel_on_close: bool = True,
#     ):
#         """Initialize.
#
#         Args:
#             work_dir: Path where log files and scripts will be created.
#             cancel_on_close: If True (default) kill all running jobs on exit.
#         """
#         if work_dir is None:
#             now = datetime.now().isoformat()
#             work_dir = platformdirs.user_cache_path(appname=f"sjm-work-dir-{now}")
#
#         work_dir = Path(work_dir)
#         work_dir.mkdir(parents=True, exist_ok=True)
#         self.work_dir = work_dir
#
#         self.cancel_on_close = cancel_on_close
#
#         # job name -> type
#         self.jobs: dict[str, SlurmJob] = {}
#
#         # job_id -> type (only for running jobs)
#         self.running_jobs: dict[int, SlurmJob] = {}
#
#         self.running_job_ids: set[int] = get_running_jobids()
#         self.last_check = datetime.now()
#
#     def submit(self, name: str, sbatch_args: list[str], script: str) -> SlurmJob:
#         """Submit a Slurm job.
#
#         Args:
#             name: Name of the job. All jobs should have a unique name.
#             sbatch_args: The arguments to sbatch.
#                 Each element in the list will be put on a separate line.
#             script: Body of the job script.
#         """
#         if name in self.jobs:
#             raise ValueError(f"Job '{name}' is already defined.")
#
#         job = submit_sbatch_job(
#             name=name,
#             sbatch_args=sbatch_args,
#             script=script,
#             work_dir=self.work_dir,
#         )
#
#         self.jobs[name] = job
#         self.running_jobs[job.job_id] = job
#         return job
#
#     def _sleep_until_next_check_time(self) -> None:
#         now = datetime.now()
#         seconds_since_last_check = (now - self.last_check).total_seconds()
#         sleep_time = SQUEUE_CHECK_INTERVAL - seconds_since_last_check
#         if sleep_time > 0:
#             time.sleep(sleep_time)
#
#     def _update_running_jobs(self):
#         self.running_job_ids = get_running_jobids()
#         self.last_check = datetime.now()
#
#         for job_id in list(self.running_jobs):
#             if job_id not in self.running_job_ids:
#                 self.running_jobs[job_id].is_running = False
#                 del self.running_jobs[job_id]
#
#     def poll(self) -> int:
#         """Return the number of running jobs."""
#         self._sleep_until_next_check_time()
#         self._update_running_jobs()
#         return len(self.running_jobs)
#
#     def wait(self, jobs: list[SlurmJob] | None = None) -> None:
#         """Wait until the given list of jobs are done."""
#         if jobs is None:
#             jobs = list(self.running_jobs.values())
#
#         running_jobs = [job for job in jobs if job.is_running]
#         while running_jobs:
#             self._sleep_until_next_check_time()
#             self._update_running_jobs()
#
#             running_jobs = [job for job in running_jobs if job.is_running]
#
#     def as_completed(self, jobs: list[SlurmJob] | None = None) -> Iterator[SlurmJob]:
#         """Yield jobs as they are completed."""
#         if jobs is None:
#             jobs = list(self.running_jobs.values())
#
#         for job in jobs:
#             if not job.is_running:
#                 yield job
#         running_jobs = [job for job in jobs if job.is_running]
#
#         while running_jobs:
#             self._sleep_until_next_check_time()
#             self._update_running_jobs()
#
#             for job in running_jobs:
#                 if not job.is_running:
#                     yield job
#             running_jobs = [job for job in running_jobs if job.is_running]
#
#     def cancel(
#         self, job: SlurmJob, term: bool = False, batch: bool = False, full: bool = False
#     ) -> None:
#         """Stop a given job."""
#         cancel_jobs(
#             [job.job_id],
#             term=term,
#             batch=batch,
#             full=full,
#         )
#
#     def close(self):
#         """Shutdown all running jobs."""
#         if self.cancel_on_close:
#             self._update_running_jobs()
#
#             running_job_ids: list[int] = []
#             for job in self.jobs.values():
#                 if job.is_running:
#                     running_job_ids.append(job.job_id)
#
#             cancel_jobs(running_job_ids)
