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


def _find_executable(name: Path | str | None, default: str) -> Path:
    """Find a given executable."""
    if isinstance(name, Path):
        return name

    if name is None:
        name = default

    if Path(name).exists():
        return Path(name)

    env_var = name.upper() + "_EXECUTABLE"
    if env_var in os.environ:
        if Path(os.environ[env_var]).exists():
            return Path(os.environ[env_var])

    executable = shutil.which(name)
    if executable is None:
        raise RuntimeError(f"Unable to find '{name!s}' in PATH.")

    return Path(executable)


find_sbatch = partial(_find_executable, default="sbatch")
find_ray = partial(_find_executable, default="ray")


def find_setup_script(file: Path | str | None) -> Path:
    """Find the setup script."""
    if file is None:
        file = Path.home() / "default-env.sh"
    if isinstance(file, str):
        file = Path(file)

    if not file.exists():
        raise RuntimeError("Can't find setup script.")

    return file


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


def terminate_gracefully(proc: subprocess.Popen, timeout: int = 5):
    """Terminal a process gracefully."""
    if proc.poll() is None:
        print("Terminating process ...", flush=True)
        proc.terminate()
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            print("Killing process ...", flush=True)
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
