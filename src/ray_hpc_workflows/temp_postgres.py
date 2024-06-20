"""Run a temporary postgres database."""

import json
import time
import shlex
import subprocess
import multiprocessing
from typing import cast
from pathlib import Path
from datetime import datetime

import psutil
import psycopg2
import platformdirs

from .utils import (
    Closeable,
    cmd_str,
    data_address,
    find_postgres,
    terminate_gracefully,
    ignoring_sigint,
)

PG_HBA_DEFAULT = """
# PostgreSQL Client Authentication Configuration File
# ===================================================
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Allow replication connections from localhost, by a user with the replication privilege.
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust

# Config to be used inside a trusted cluster only
host    all             all             0.0.0.0/0               password
"""

PG_CONFIG_FORMAT = """
# Postgresql configuration
# ========================

# Connections and Authentication

listen_addresses = '*'
unix_socket_directories = ''

max_connections = {max_connections}
superuser_reserved_connections = 0
max_wal_senders = 0

# Resource Consumption

shared_buffers = {shared_buffers_mb}MB
work_mem = 1MB
maintenance_work_mem = {maintenance_work_mem_mb}MB
huge_pages = {huge_pages}

dynamic_shared_memory_type = posix

effective_io_concurrency = 2

max_worker_processes = {num_cpus}
max_parallel_workers = {num_cpus}
max_parallel_workers_per_gather = {num_cpus_by2}
max_parallel_maintenance_workers = {num_cpus_by2}

# Write Ahead Log

wal_level = minimal
fsync = true
wal_sync_method = fdatasync

min_wal_size = 1GB
max_wal_size = 4GB

checkpoint_completion_target = 0.9
wal_buffers = 16MB

# Query Planning
seq_page_cost = 1.0
random_page_cost = 4.0
effective_cache_size = {effective_cache_size_mb}MB
default_statistics_target = 100

# Logging

log_min_messages = info
log_min_error_statement = error

log_connections = on
log_disconnections = on
log_lock_waits = on

log_timezone = 'America/New_York'

# - Locale and Formatting -

datestyle = 'iso, mdy'

timezone = 'America/New_York'

lc_messages = 'en_US.UTF-8'
lc_monetary = 'en_US.UTF-8'
lc_numeric = 'en_US.UTF-8'
lc_time = 'en_US.UTF-8'

default_text_search_config = 'pg_catalog.english'
"""


def wait_for_db_start(dsn: dict, server: subprocess.Popen) -> None:
    print("Waiting for database to start .", end="", flush=True)
    while True:
        if server.poll() is not None:
            raise RuntimeError("Server is not running.")

        try:
            with psycopg2.connect(**dsn):
                print("")
                return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(1)


def make_postgres_config(
    mem_gb: int | None, num_cpus: int | None, max_connections: int = 100
) -> str:
    if mem_gb is None:
        # If not specified use half system memory
        mem_gb = int(psutil.virtual_memory().total / 2**30 / 2)
    if num_cpus is None:
        # If not specified use half the CPUs
        num_cpus = int(multiprocessing.cpu_count() / 2)

    if mem_gb > 32:
        huge_pages = "try"
    else:
        huge_pages = "off"

    shared_buffers_mb = int(mem_gb / 4.0 * 1000)
    effective_cache_size_mb = int(mem_gb * 3.0 / 4.0 * 1000)
    maintenance_work_mem_mb = int(mem_gb / 16.0 * 1000)
    num_cpus_by2 = num_cpus // 2

    return PG_CONFIG_FORMAT.format(
        num_cpus=num_cpus,
        num_cpus_by2=num_cpus_by2,
        max_connections=max_connections,
        huge_pages=huge_pages,
        shared_buffers_mb=shared_buffers_mb,
        effective_cache_size_mb=effective_cache_size_mb,
        maintenance_work_mem_mb=maintenance_work_mem_mb,
    )


def do_config_db(
    db_dir: Path,
    username: str,
    password: str,
    mem_gb: int | None,
    num_cpus: int | None,
    max_connections: int,
):
    db_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = db_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Copy over the optimized postgres config
    with open(db_dir / "postgres/postgresql.conf", "wt") as fobj:
        fobj.write(make_postgres_config(mem_gb, num_cpus, max_connections))

    with open(db_dir / "postgres/pg_hba.conf", "wt") as fobj:
        fobj.write(PG_HBA_DEFAULT)

    # Save username and password used to create the db
    connect_config = dict(username=username, password=password)
    connect_file = db_dir / "connect_config.json"
    connect_file.write_text(json.dumps(connect_config))


def do_init_db(
    db_dir: Path,
    initdb_exe: Path,
    username: str,
    password: str,
    mem_gb: int | None,
    num_cpus: int | None,
    max_connections: int,
    verbose: bool,
):
    db_dir.mkdir(parents=True, exist_ok=True)

    tmp_dir = db_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Create the password file
    pass_file = db_dir / "pass.txt"
    pass_file.write_text(password)

    # Initialize the database directory
    cmd = f"""
    '{initdb_exe!s}'
        -A trust
        -D '{db_dir!s}/postgres'
        -U '{username}'
        --pwfile '{pass_file!s}'
    """
    if verbose:
        print("executing:", cmd_str(cmd))
    cmd = shlex.split(cmd)

    subprocess.run(
        cmd,
        check=True,
        stdin=subprocess.DEVNULL,
    )

    # Copy over the optimized postgres config
    with open(db_dir / "postgres/postgresql.conf", "wt") as fobj:
        fobj.write(make_postgres_config(mem_gb, num_cpus, max_connections))

    with open(db_dir / "postgres/pg_hba.conf", "wt") as fobj:
        fobj.write(PG_HBA_DEFAULT)

    # Save username and password used to create the db
    connect_config = dict(username=username, password=password)
    connect_file = db_dir / "connect_config.json"
    connect_file.write_text(json.dumps(connect_config))


def do_create_db(
    createdb_exe: Path,
    host: str,
    port: int,
    username: str,
    dbname: str,
    verbose: bool,
):
    cmd = f"'{createdb_exe!s}' -h {host} -p {port} -U {username} {dbname}"
    if verbose:
        print("executing:", cmd_str(cmd))

    cmd = shlex.split(cmd)
    subprocess.run(
        cmd,
        check=True,
        stdin=subprocess.DEVNULL,
    )


class PostgresDB(Closeable):
    """Run a postgres database locally"""

    def __init__(
        self,
        db_dir: Path | str | None,
        postgres_exe: Path | str | None = None,
        interface: str | None = None,
        port: int = 5432,
        username: str | None = None,
        password: str | None = None,
        dbname: str = "default",
        connect_timeout: int = 10,
        mem_gb: int | None = None,
        num_cpus: int | None = None,
        max_connections: int = 100,
        verbose: bool = False,
    ):
        if db_dir is None:
            now = datetime.now().isoformat()
            db_dir = platformdirs.user_cache_path(appname=f"temp-postgres-db-{now}")

        db_dir = Path(db_dir)
        self.db_dir = db_dir

        connect_file = db_dir / "connect_config.json"
        if connect_file.exists():
            connect_config = json.loads(connect_file.read_text())
            username = cast(str, connect_config["username"])
            password = cast(str, connect_config["password"])
        if username is None:
            username = "postgres"
        if password is None:
            password = "postgres"

        postgres_exe = find_postgres(postgres_exe)
        address = data_address(interface)

        self.postgres_exe = postgres_exe

        self.host = address
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.connect_timeout = connect_timeout
        self.verbose = verbose

        self.mem_gb = mem_gb
        self.num_cpus = num_cpus
        self.max_connections = max_connections

        self._proc: subprocess.Popen | None = None

    def get_host(self) -> str:
        return self.host

    def get_dsn(self) -> dict:
        return dict(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.dbname,
            connect_timeout=self.connect_timeout,
        )

    def get_sqlalchemy_url(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"

    def getattr(self, key: str):
        return getattr(self, key)

    def config_db(self):
        assert self.db_dir.exists()

        print(f"Configuring database directory {self.db_dir!s} ...")
        do_config_db(
            self.db_dir,
            self.username,
            self.password,
            self.mem_gb,
            self.num_cpus,
            self.max_connections,
        )

    def start_db(self):
        assert self.db_dir.exists()

        print("Starting postgres server ...")
        cmd = f"""
        '{self.postgres_exe!s}'
            -p {self.port}
            -k '{self.db_dir!s}/tmp'
            -D '{self.db_dir!s}/postgres'
        """
        if self.verbose:
            print("executing:", cmd_str(cmd))
        cmd = shlex.split(cmd)

        assert self._proc is None
        with ignoring_sigint():
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
            )

        init_dsn = dict(
            host="localhost",
            port=self.port,
            user=self.username,
            password=self.password,
            dbname="template1",
            connect_timeout=self.connect_timeout,
        )
        wait_for_db_start(init_dsn, self._proc)

    def create_db(self):
        initdb_exe = self.postgres_exe.parent / "initdb"
        createdb_exe = self.postgres_exe.parent / "createdb"

        print(f"Initializing database directory {self.db_dir!s} ...")
        do_init_db(
            self.db_dir,
            initdb_exe,
            self.username,
            self.password,
            self.mem_gb,
            self.num_cpus,
            self.max_connections,
            self.verbose,
        )

        self.start_db()

        print(f"Creating database {self.dbname} ...")
        do_create_db(
            createdb_exe,
            "localhost",
            self.port,
            self.username,
            self.dbname,
            self.verbose,
        )

    def close(self):
        if self._proc is not None:
            terminate_gracefully(self._proc)
            self._proc = None
