"""Common utilities."""

import os
import signal
import shutil
import socket
import subprocess
from pathlib import Path
from textwrap import dedent
from functools import partial
from abc import ABC, abstractmethod
from contextlib import closing, contextmanager

import netifaces


def _find_executable(name: str, path: Path | str | None = None) -> Path:
    """Find a given executable."""
    if path is not None:
        if not isinstance(path, Path):
            path = Path(path)
        if not path.exists():
            raise ValueError("Explicitly provided path doesn't exist.")
        return path

    env_var = name.upper() + "_EXECUTABLE"
    if env_var in os.environ:
        if Path(os.environ[env_var]).exists():
            return Path(os.environ[env_var])

    executable = shutil.which(name)
    if executable is None:
        raise RuntimeError(f"Unable to find '{name!s}' in PATH.")

    return Path(executable)


find_sbatch = partial(_find_executable, "sbatch")
find_ray = partial(_find_executable, "ray")
find_jupyter = partial(_find_executable, "jupyter")
find_postgres = partial(_find_executable, "postgres")
find_optuna_dashboard = partial(_find_executable, "optuna-dashboard")
find_prometheus = partial(_find_executable, "prometheus")
find_grafana = partial(_find_executable, "grafana")


def find_setup_script(path: Path | str | None) -> Path:
    """Find the setup script."""
    if path is None:
        path = Path.home() / "default-env.sh"

    if not isinstance(path, Path):
        path = Path(path)

    if not path.exists():
        raise RuntimeError("Can't find setup script.")

    return path


def data_address(interface: str | None, default: str = "0.0.0.0") -> str:
    """Find the network address for data transfer."""
    if interface is None:
        if "ib0" in netifaces.interfaces():
            interface = "ib0"

    if interface is None:
        return default

    return netifaces.ifaddresses(interface)[netifaces.AF_INET][0]["addr"]


def arbitrary_free_port(host: str) -> int:
    """Request a free port from OS."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind((host, 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock.getsockname()[1]


def cmd_str(cmd: str) -> str:
    """Clean up an indented string."""
    return dedent(cmd.strip())


@contextmanager
def ignoring_sigint():
    """SIGINT is ignored inside this context manager."""
    handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, handler)


def terminate_gracefully(
    proc: subprocess.Popen, timeout: int = 5, proc_name: str = "process"
):
    """Terminal a process gracefully."""
    if proc.poll() is None:
        print(f"Terminating {proc_name} ...", flush=True)
        proc.terminate()
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            print(f"Killing {proc_name} ...", flush=True)
            proc.kill()


class Closeable(ABC):
    """Base class for objects that require cleanup."""

    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        exc_type, exc_val, exc_tb = exc_type, exc_val, exc_tb
        self.close()

    def __del__(self):
        self.close()
