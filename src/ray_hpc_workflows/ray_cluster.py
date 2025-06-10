"""Start a Ray cluster on Rivanna."""

import os
import atexit
import shlex
import socket
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict

import platformdirs

from .utils import (
    Closeable,
    data_address,
    arbitrary_free_port,
    ignoring_sigint,
    find_ray,
    find_setup_script,
    terminate_gracefully,
)
from .slurm_job_manager import SlurmJob, SlurmJobManager
from .templates import render_template


@dataclass
class WorkerConfig:
    sbatch_args: list[str]
    num_cpus: int
    num_gpus: int
    resources: dict[str, int]
    use_srun: bool
    num_nodes: int


_KNOWN_WORKER: dict[str, WorkerConfig]


def define_worker_config(
    name: str,
    sbatch_args: list[str],
    num_cpus: int,
    num_gpus: int,
    resources: dict[str, int],
    use_srun: bool = False,
    num_nodes: int = 1,
):
    """Define a new worker config."""
    if name is _KNOWN_WORKER:
        raise RuntimeError(f"Worker config for '{name}' is aleady defined")

    _KNOWN_WORKER[name] = WorkerConfig(
        sbatch_args, num_cpus, num_gpus, resources, use_srun, num_nodes
    )


class RayCluster(Closeable):
    """Run a Ray cluster."""

    def __init__(
        self,
        work_dir: Path | str | None = None,
        sjm: SlurmJobManager | None = None,
        python_paths: list[str] | None = None,
        head_num_cpus: int | None = None,
        head_num_gpus: int | None = None,
        head_resources: dict[str, int] | None = None,
        add_cwd_to_python_paths: bool = True,
        verbose: bool = False,
        ray_executable: Path | str | None = None,
        setup_script: Path | str | None = None,
    ):
        """Initialize."""
        user = os.environ["USER"]
        address = data_address(None)
        self.ray_executable = find_ray(ray_executable)
        self.setup_script = find_setup_script(setup_script)

        if work_dir is None:
            now = datetime.now().isoformat()
            work_dir = platformdirs.user_cache_path(appname=f"ray-work-dir-{now}")
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        if sjm is None:
            sjm = SlurmJobManager(self.work_dir / "sjm", cancel_on_close=False)
        self.sjm = sjm

        self.host = address
        self.port = arbitrary_free_port(address)
        self.client_server_port = arbitrary_free_port(address)
        self.dashboard_host = socket.gethostname()
        self.dashboard_port = arbitrary_free_port("")
        self.dashboard_agent_listen_port = arbitrary_free_port("")

        self.plasma_dir = Path("/dev/shm") / user / f"ray_plasma_dir"
        self.plasma_dir.mkdir(parents=True, exist_ok=True)

        self.temp_dir = Path("/tmp") / user / "ray_temp_dir"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        cluster_python_path = []
        if add_cwd_to_python_paths:
            cluster_python_path.append(str(Path.cwd()))
        if python_paths is not None:
            cluster_python_path.extend(python_paths)
        if "PYTHONPATH" in os.environ:
            cluster_python_path.extend(os.environ["PYTHONPATH"].split(":"))
        cluster_python_path = ":".join(cluster_python_path)
        self.cluster_python_path = cluster_python_path

        print("Starting Ray head service ...")
        cmd = render_template(
            "ray_cluster:head_command",
            ray_executable=self.ray_executable,
            host=self.host,
            port=self.port,
            client_server_port=self.client_server_port,
            dashboard_port=self.dashboard_port,
            plasma_dir=self.plasma_dir,
            temp_dir=self.temp_dir,
            dashboard_agent_listen_port=self.dashboard_agent_listen_port,
            num_cpus=head_num_cpus,
            num_gpus=head_num_gpus,
            resources=head_resources,
        )
        if verbose:
            print(f"executing: {cmd}")
        cmd = shlex.split(cmd)

        env = dict(os.environ)
        env["RAY_scheduler_spread_threshold"] = "0.0"
        env["PYTHONPATH"] = self.cluster_python_path

        self._head_proc: subprocess.Popen | None
        with open(self.work_dir / "head.log", "at") as fobj:
            with ignoring_sigint():
                self._head_proc = subprocess.Popen(
                    cmd,
                    stdout=fobj,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=env,
                )

        self.next_worker_index: dict[str, int] = defaultdict(int)
        self.workers: dict[str, list[SlurmJob]] = defaultdict(list)

        self.head_address = f"{self.host}:{self.port}"
        self.client_address = f"ray://{self.host}:{self.client_server_port}"
        self.dashboard_url = f"http://{self.dashboard_host}:{self.dashboard_port}"

        atexit.register(self.close)

        print(f"Ray dashboard URL: {self.dashboard_url}")

    def add_worker(self, name: str) -> None:
        """Add a worker of the given type."""
        worker_config = _KNOWN_WORKER[name]

        worker_index = self.next_worker_index[name]
        self.next_worker_index[name] += 1
        worker_name = f"ray_worker.{name}.{worker_index}"

        worker_script = render_template(
            "ray_cluster:worker_script",
            use_srun=worker_config.use_srun,
            worker_name=worker_name,
            address=self.head_address,
            work_dir=self.work_dir,
            temp_dir=self.temp_dir,
            plasma_dir=self.plasma_dir,
            setup_script=self.setup_script,
            dashboard_agent_listen_port=self.dashboard_agent_listen_port,
            num_cpus=worker_config.num_cpus,
            num_gpus=worker_config.num_gpus,
            resources=worker_config.resources,
            ray_executable=self.ray_executable,
            cluster_python_path=self.cluster_python_path,
        )

        print(f"Starting worker {worker_name} ...")
        job = self.sjm.submit(worker_name, worker_config.sbatch_args, worker_script)
        self.workers[name].append(job)

    def scale_workers(self, worker_type_name: str, num_workers: int):
        """Ensure given number of HPC workers are running."""
        # If number of workers is less that what we have, we scale up
        while len(self.workers[worker_type_name]) < num_workers:
            self.add_worker(worker_type_name)

        # If number of workers is more than what we need, we scale down
        while len(self.workers[worker_type_name]) > num_workers:
            worker = self.workers[worker_type_name].pop()

            print(f"Stopping worker {worker.name} ...")
            self.sjm.cancel(worker, term=True, full=True)

    def close(self):
        """Shutdown the ray cluster and connections."""
        print("Closing ray cluster.")

        for worker_type_name in _KNOWN_WORKER:
            self.scale_workers(worker_type_name, 0)

        if self._head_proc is not None:
            terminate_gracefully(self._head_proc, proc_name="Ray head process")
            self._head_proc = None

        if self.plasma_dir.exists():
            shutil.rmtree(self.plasma_dir)

        if self.temp_dir.exists():
            print("Saving head logs ...")
            log_dir = self.work_dir / "ray-logs" / "head"
            log_dir.mkdir(parents=True, exist_ok=True)

            cmd = [
                "rsync",
                "-av",
                f"{self.temp_dir}/session_latest/logs/",
                f"{log_dir}/",
            ]
            subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL)

            shutil.rmtree(self.temp_dir)
