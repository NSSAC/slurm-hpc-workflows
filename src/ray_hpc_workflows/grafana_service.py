"""Run Grafana dashboard."""

import shlex
import socket
import subprocess
from pathlib import Path
from datetime import datetime

import platformdirs

from .utils import (
    Closeable,
    cmd_str,
    find_grafana,
    ignoring_sigint,
    terminate_gracefully,
)

CURDIR = Path(__file__).parent
DASHBOARDS_DIR = CURDIR / "config/grafana_dashboards"

# provisioning/grafana.ini
DEFAULT_CONFIG = """
[security]
allow_embedding = true

[auth.anonymous]
enabled = true
org_name = Default Org.
org_role = Viewer

[server]
protocol = http
http_port = {port}
domain = {hostname}

[paths]
provisioning = {provisioning_dir!s}
"""

# provisioning/datasources/default.yml
DATASOURCES_DEFAULT = """
apiVersion: 1

datasources:
  - name: Prometheus
    url: "{prometheus_url!s}"
    type: prometheus
    isDefault: true
    access: proxy
"""

# provisioning/dashboards/default.yml
DASHBOARDS_DEFAULT = f"""
apiVersion: 1

providers:
  - name: Ray
    folder: Ray
    type: file
    options:
      path: "{DASHBOARDS_DIR!s}"
"""


class GrafanaService(Closeable):
    """Grafana dashboard runner."""

    def __init__(
        self,
        prometheus_url: str,
        provisioning_dir: Path | str | None = None,
        grafana_exe: Path | str | None = None,
        port: int = 9090,
        verbose: bool = False,
    ):
        if provisioning_dir is None:
            now = datetime.now().isoformat()
            provisioning_dir = platformdirs.user_cache_path(appname=f"prometheus-{now}")

        self.provisioning_dir = Path(provisioning_dir)
        self.provisioning_dir.mkdir(parents=True, exist_ok=True)
        (self.provisioning_dir / "datasources").mkdir(exist_ok=True)
        (self.provisioning_dir / "dashboards").mkdir(exist_ok=True)

        self.grafana_exe = find_grafana(grafana_exe)
        grafana_home_dir = self.grafana_exe.parent.parent

        hostname = socket.gethostname()
        self.port = port

        config = DEFAULT_CONFIG.format(
            hostname=hostname, port=self.port, provisioning_dir=self.provisioning_dir
        ).strip()
        self.config_file = self.provisioning_dir / "grafana.ini"
        self.config_file.write_text(config)

        datasources = DATASOURCES_DEFAULT.format(prometheus_url=prometheus_url).strip()
        self.datasources_file = self.provisioning_dir / "datasources/default.yml"
        self.datasources_file.write_text(datasources)

        dashboards = DASHBOARDS_DEFAULT.strip()
        self.dashboards_file = self.provisioning_dir / "dashboards/default.yml"
        self.dashboards_file.write_text(dashboards)

        print("Starting grafana ...")
        cmd = f"""
        '{self.grafana_exe!s}' server
            --homepath '{grafana_home_dir!s}'
            --config '{self.config_file!s}'
            web
        """
        if verbose:
            print("executing:", cmd_str(cmd))
        cmd = shlex.split(cmd)

        self._proc: subprocess.Popen | None
        with open(self.provisioning_dir / "grafana.log", "wb") as fobj:
            with ignoring_sigint():
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=fobj,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                )

        self.dashboard_url = f"http://{hostname}:{self.port}"
        print(f"Grafana dashboard URL: {self.dashboard_url}")

    def close(self):
        if self._proc is not None:
            terminate_gracefully(self._proc, proc_name="Grafana server")
            self._proc = None
