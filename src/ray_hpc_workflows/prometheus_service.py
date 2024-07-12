"""Run Prometheus systems and service monitoring platform."""

import shlex
import socket
import subprocess
from pathlib import Path
from datetime import datetime

import platformdirs

from .utils import (
    Closeable,
    cmd_str,
    find_prometheus,
    ignoring_sigint,
    terminate_gracefully,
)

DEFAULT_CONFIG = """
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
- job_name: '{metrics_job_name}'
  file_sd_configs:
  - files:
    - '{metrics_service_discovery_file!s}'
"""


class PrometheusService(Closeable):
    """Prometheus server runner."""

    def __init__(
        self,
        metrics_job_name: str,
        metrics_service_discovery_file: Path,
        storage_path: Path | str | None = None,
        prometheus_exe: Path | str | None = None,
        port: int = 9090,
        verbose: bool = False,
    ):
        if storage_path is None:
            now = datetime.now().isoformat()
            storage_path = platformdirs.user_cache_path(appname=f"prometheus-{now}")

        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

        self.prometheus_exe = find_prometheus(prometheus_exe)
        self.port = port
        self.config_file = self.storage_path / "prometheus.yml"

        config = DEFAULT_CONFIG.format(
            metrics_job_name=metrics_job_name,
            metrics_service_discovery_file=metrics_service_discovery_file,
        )
        self.config_file.write_text(config)

        # Start dashboard in the background
        print("Starting Prometheus service ...")
        cmd = f"""
        '{self.prometheus_exe!s}'
            --config.file='{self.config_file!s}'
            --web.listen-address='0.0.0.0:{self.port}'
            --storage.tsdb.path='{self.storage_path!s}'
        """
        if verbose:
            print("executing:", cmd_str(cmd))
        cmd = shlex.split(cmd)

        self._proc: subprocess.Popen | None
        with open(self.storage_path / "prometheus.log", "wb") as fobj:
            with ignoring_sigint():
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=fobj,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                )

        hostname = socket.gethostname()
        self.web_url = f"http://{hostname}:{self.port}"
        print(f"Prometheus service URL: {self.web_url}")

    def close(self):
        if self._proc is not None:
            terminate_gracefully(self._proc, proc_name="Prometheus server")
            self._proc = None
