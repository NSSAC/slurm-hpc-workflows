"""Run Optuna Dashboard."""

import shlex
import shutil
import socket
import subprocess
from pathlib import Path
from datetime import datetime

import platformdirs

from .utils import (
    Closeable,
    cmd_str,
    find_optuna_dashboard,
    ignoring_sigint,
    terminate_gracefully,
)


class OptunaDashboard(Closeable):
    """Optuna dashboard runner."""

    def __init__(
        self,
        storage: str,
        artifact_dir: Path | str | None = None,
        dashboard_exe: Path | str | None = None,
        port: int = 8080,
        delete_if_exists: bool = False,
        verbose: bool = False,
    ):
        if artifact_dir is None:
            now = datetime.now().isoformat()
            artifact_dir = platformdirs.user_cache_path(
                appname=f"optuna-dashboard-{now}"
            )

        artifact_dir = Path(artifact_dir)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        dashboard_exe = find_optuna_dashboard(dashboard_exe)

        if delete_if_exists and artifact_dir.exists():
            shutil.rmtree(artifact_dir)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        # Start dashboard in the background
        print("Starting dashboard ...")
        cmd = f"""
        '{dashboard_exe!s}'
            --host 0.0.0.0
            --port {port}
            --artifact-dir '{artifact_dir!s}'
            '{storage}'
        """
        if verbose:
            print("executing:", cmd_str(cmd))
        cmd = shlex.split(cmd)

        self._proc: subprocess.Popen | None
        with ignoring_sigint():
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
            )

        hostname = socket.gethostname()
        self._dashboard_url = f"http://{hostname}:{port}"
        print(f"Dashboard url: {self._dashboard_url}")

    def dashboard_url(self) -> str:
        return self._dashboard_url

    def close(self):
        if self._proc is not None:
            terminate_gracefully(self._proc)
            self._proc = None
