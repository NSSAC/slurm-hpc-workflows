"""Pilot workers for slurm."""

from __future__ import annotations, generator_stop

import os
import time
import json
import pickle
import socket
import logging
import random
import string
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from collections import deque
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Callable, Iterable, Any

import tqdm
import grpc
import click
import platformdirs
import cloudpickle
from more_itertools import chunked
from typeguard import typechecked

from .slurm_job_manager import (
    get_running_jobids,
    cancel_jobs,
    submit_sbatch_job,
    SlurmJob,
)
from .slurm_pilot_pb2 import (
    Empty,
    WorkerProcessID,
    TaskDefn,
    TaskAssignment,
    TaskResult,
)
from .slurm_pilot_pb2_grpc import (
    CoordinatorServicer,
    CoordinatorStub,
    add_CoordinatorServicer_to_server,
)
from .utils import data_address, find_setup_script, arbitrary_free_port
from .templates import render_template

INTER_SQUEUE_CALL_TIME_S: float = 5.0
NEXT_TASK_RETRY_TIME_S: float = 1.0
LOG_FORMAT: str = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"
LOG_LEVEL = logging.INFO


def gen_error_id() -> str:
    return "ERROR_" + "".join(
        random.choices(string.ascii_lowercase + string.digits, k=32)
    )


def gen_task_id() -> str:
    return f"TASK_" + "".join(
        random.choices(string.ascii_lowercase + string.digits, k=32)
    )


@typechecked
def wait_with_progress(
    futs: list[Future], desc: str | None = None, unit: str = "it"
) -> None:
    it = as_completed(futs)
    it = tqdm.tqdm(it, desc=desc, total=len(futs), unit=unit)
    for _ in it:
        pass


class PilotProcess:
    def __init__(
        self,
        type: str,
        name: str,
        server_address: str,
        work_dir: Path,
        slurm_job_id: int,
        hostname: str,
        pid: int,
    ):
        self._process_id = WorkerProcessID(
            type=type,
            name=name,
            slurm_job_id=slurm_job_id,
            hostname=hostname,
            pid=pid,
        )

        self._server_address = server_address
        self._work_dir = work_dir
        self._exit_flag = False
        self._logger = logging.getLogger("worker_process")

        client_service_config = json.dumps(
            {
                "methodConfig": [
                    {
                        "name": [{}],
                        "retryPolicy": {
                            "maxAttempts": 100,
                            "initialBackoff": "1s",
                            "maxBackoff": "15s",
                            "backoffMultiplier": 2,
                            "retryableStatusCodes": ["UNAVAILABLE"],
                        },
                    }
                ]
            }
        )
        options = [
            # Keep alive stuff
            ("grpc.keepalive_time_ms", 8000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
            # Retry stuff
            ("grpc.enable_retries", 1),
            ("grpc.service_config", client_service_config),
            ("grpc-node.retry_max_attempts_limit", 100),
        ]

        self._channel = grpc.insecure_channel(self._server_address, options=options)
        self._stub = CoordinatorStub(self._channel)

    def close(self):
        self._channel.close()

    def RegisterWorkerProcess(self) -> None:
        self._stub.RegisterWorkerProcess(self._process_id)

    def UnregisterWorkerProcess(self) -> None:
        self._stub.UnregisterWorkerProcess(self._process_id)

    def GetNextTask(self) -> TaskDefn | None:
        assignment: TaskAssignment = self._stub.GetNextTask(self._process_id)
        if assignment.exit_flag:
            self._exit_flag = True
            return None
        elif assignment.task_available:
            return assignment.task
        return None

    def SetTaskResult(self, result: TaskResult) -> None:
        self._stub.SetTaskResult(result)

    def do_run_task(self, task: TaskDefn) -> TaskResult:
        try:
            function = cloudpickle.loads(task.function)
            args = cloudpickle.loads(task.args)
            kwargs = cloudpickle.loads(task.kwargs)

            return_ = function(*args, **kwargs)
            return_ = cloudpickle.dumps(return_)
            result = TaskResult(
                task_id=task.task_id,
                task_success=True,
                return_=return_,
                process_id=self._process_id,
            )
        except Exception as e:
            eid = gen_error_id()
            self._logger.exception("Error executing %s: %s: %s", task.task_id, eid, e)
            result = TaskResult(
                task_id=task.task_id,
                task_success=False,
                error=f"{type(e)}: {e}",
                error_id=eid,
                process_id=self._process_id,
            )

        return result

    def main(self):
        self.RegisterWorkerProcess()
        try:
            while True:
                task = self.GetNextTask()
                if task is not None:
                    result = self.do_run_task(task)
                    self.SetTaskResult(result)

                if self._exit_flag:
                    return
                else:
                    time.sleep(NEXT_TASK_RETRY_TIME_S)
        finally:
            self.UnregisterWorkerProcess()
            self.close()


@click.command()
@click.option("--type", type=str, required=True, help="Worker type")
@click.option("--name", type=str, required=True, help="Worker job ID")
@click.option("--server-address", type=str, required=True, help="Pilot server address")
@click.option(
    "--work-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Work directory",
)
def slurm_pilot_process(type: str, name: str, server_address: str, work_dir: Path):
    """Start a slurm pilot worker."""
    slurm_job_id = int(os.environ.get("SLURM_JOB_ID", -1))
    hostname = socket.gethostname()
    pid = os.getpid()
    log_file = work_dir / f"{name}-{slurm_job_id}-{hostname}-{pid}.log"
    logging.basicConfig(filename=log_file, format=LOG_FORMAT, level=LOG_LEVEL)

    worker = PilotProcess(
        type=type,
        name=name,
        server_address=server_address,
        work_dir=work_dir,
        slurm_job_id=slurm_job_id,
        hostname=hostname,
        pid=pid,
    )
    worker.main()


@dataclass
class TaskDefnWithFut:
    defn: TaskDefn
    fut: Future


@dataclass
class WorkerProcess:
    key: str
    process_id: WorkerProcessID
    running_task: TaskDefnWithFut | None = None


@dataclass
class Worker:
    name: str
    slurm_job: SlurmJob
    exit_flag: bool = False
    is_running: bool = True
    processes: dict[str, WorkerProcess] = field(default_factory=dict)
    is_new: bool = True

    def has_active_processes(self) -> bool:
        return len(self.processes) > 0

    def has_running_tasks(self) -> bool:
        return any(p.running_task is not None for p in self.processes.values())


@dataclass
class WorkerGroup:
    type: str
    sbatch_args: list[str]
    is_batch_worker: bool
    workers: dict[str, Worker] = field(default_factory=dict)
    next_worker_index: int = 0
    task_queue: deque[TaskDefnWithFut] = field(default_factory=deque)

    # Available workers: workers with exit_flag == False
    def available_workers(self) -> int:
        return sum(1 for w in self.workers.values() if w.exit_flag)


def _map_chunk(fn, args_list):
    results = []
    for args in args_list:
        results.append(fn(*args))
    return results


class SlurmPilotExecutor(CoordinatorServicer):
    def __init__(
        self,
        work_dir: Path | str | None = None,
        setup_script: Path | str | None = None,
    ):
        self.setup_script = find_setup_script(setup_script)

        if work_dir is None:
            now = datetime.now().isoformat()
            work_dir = platformdirs.user_cache_path(appname=f"slurm-pilot") / now
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger("pilot_coordinator")
        self._logger.setLevel(LOG_LEVEL)
        handler = logging.FileHandler(self.work_dir / "coordinator.log", delay=True)
        handler.setLevel(LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        self._lock = threading.Lock()
        self._groups: dict[str, WorkerGroup] = {}
        self._exit_flag = threading.Event()
        self._queue_monitor_thread: threading.Thread | None = None

        self._server_address: str | None = None
        self._server: grpc.Server | None = None

    def _cleanup_process(
        self, group: WorkerGroup, worker: Worker, process: WorkerProcess
    ):
        if process.running_task is not None:
            group.task_queue.appendleft(process.running_task)
            process.running_task = None
        del worker.processes[process.key]

    def _cleanup_worker(self, group: WorkerGroup, worker: Worker):
        self._logger.info(f"Cleaning up worker %s", worker.name)
        for key in list(worker.processes):
            process = worker.processes[key]
            self._cleanup_process(group, worker, process)
        del group.workers[worker.name]

    def _cleanup_finished_workers(self):
        try:
            job_ids = get_running_jobids()
        except subprocess.CalledProcessError as cp:
            self._logger.warning(
                "Failed to get running slurm job ids: returncode=%s", cp.returncode
            )
            if cp.stdout.strip():
                self._logger.warning("stdout=%s", cp.stdout)
            if cp.stderr.strip():
                self._logger.warning("stderr=%s", cp.stderr)
            job_ids = None
        except Exception:
            self._logger.exception("Failed to get running slurm job ids")
            job_ids = None

        if job_ids is None:
            return

        to_cancel_jobs: list[int] = []
        with self._lock:
            for group in self._groups.values():
                for name in list(group.workers):
                    worker = group.workers[name]
                    if worker.is_new:
                        worker.is_new = False
                        continue
                    elif not worker.slurm_job.job_id in job_ids:
                        self._cleanup_worker(group, worker)
                    elif not worker.has_active_processes() and worker.exit_flag:
                        to_cancel_jobs.append(worker.slurm_job.job_id)
                        self._cleanup_worker(group, worker)

        if to_cancel_jobs:
            try:
                cancel_jobs(to_cancel_jobs)
            except subprocess.CalledProcessError as cp:
                self._logger.warning(
                    "Failed to cancel jobs: returncode=%s", cp.returncode
                )
                if cp.stdout.strip():
                    self._logger.warning("stdout=%s", cp.stdout)
                if cp.stderr.strip():
                    self._logger.warning("stderr=%s", cp.stderr)
            except Exception:
                self._logger.exception("Failed to get running slurm job ids")

    def _cleanup_all_workers(self):
        try:
            job_ids = get_running_jobids()
        except subprocess.CalledProcessError as cp:
            print(f"Failed to get running slurm job ids: returncode={cp.returncode}")
            if cp.stdout.strip():
                print(cp.stdout)
            if cp.stderr.strip():
                print(cp.stderr)
            job_ids = None
        except Exception:
            self._logger.exception("Failed to get running slurm job ids")
            job_ids = None

        if job_ids is None:
            return

        still_running_job_ids: list[int] = []
        with self._lock:
            for group in self._groups.values():
                for name in list(group.workers):
                    worker = group.workers[name]
                    if worker.slurm_job.job_id in job_ids:
                        still_running_job_ids.append(worker.slurm_job.job_id)
                    self._cleanup_worker(group, worker)

        if still_running_job_ids:
            try:
                cancel_jobs(still_running_job_ids)
            except subprocess.CalledProcessError as cp:
                print(f"Failed to cancel slurm jobs: returncode={cp.returncode}")
                if cp.stdout.strip():
                    print(cp.stdout)
                if cp.stderr.strip():
                    print(cp.stderr)
            except Exception:
                self._logger.exception("Failed to cancel slurm jobs")

    def _queue_monitor_main(self):
        while True:
            self._cleanup_finished_workers()
            if self._exit_flag.wait(timeout=INTER_SQUEUE_CALL_TIME_S):
                break

    @typechecked
    def define_worker(
        self, type: str, sbatch_args: list[str], is_batch_worker: bool = False
    ) -> None:
        with self._lock:
            group = WorkerGroup(
                type=type, sbatch_args=sbatch_args, is_batch_worker=is_batch_worker
            )

            if group.type in self._groups:
                assert self._groups[group.type] == group
            else:
                self._groups[group.type] = group

    def _add_worker(self, group: WorkerGroup) -> None:
        assert self._server_address is not None

        worker_index = group.next_worker_index
        group.next_worker_index += 1
        name = f"slurm_pilot_worker.{group.type}.{worker_index}"

        worker_script = render_template(
            "slurm_pilot:worker_script",
            type=group.type,
            name=name,
            server_address=self._server_address,
            work_dir=str(self.work_dir),
            setup_script=str(self.setup_script),
        )
        worker_script_path = self.work_dir / f"{name}.sh"
        worker_script_path.write_text(worker_script)
        worker_script_path.chmod(0o755)

        worker_sbatch_script = render_template(
            "slurm_pilot:worker_sbatch_script",
            is_batch_worker=group.is_batch_worker,
            worker_script_path=worker_script_path,
        )

        print(f"Starting worker {name}")
        try:
            slurm_job = submit_sbatch_job(
                name=name,
                sbatch_args=group.sbatch_args,
                script=worker_sbatch_script,
                work_dir=self.work_dir,
            )
            group.workers[name] = Worker(name=name, slurm_job=slurm_job)
        except subprocess.CalledProcessError as cp:
            print(f"Failed to cancel slurm jobs: returncode={cp.returncode}")
            if cp.stdout.strip():
                print(cp.stdout)
            if cp.stderr.strip():
                print(cp.stdout)
            raise cp

    @typechecked
    def scale_workers(self, type: str, count: int) -> None:
        with self._lock:
            assert type in self._groups, "Unknown worker type"

            group = self._groups[type]
            if len(group.workers) < count:
                to_hire = count - len(group.workers)
                for _ in range(to_hire):
                    self._add_worker(group)

            if len(group.workers) > count:
                to_retire = len(group.workers) - count

                # First we try to retire
                # without active processes
                for worker in group.workers.values():
                    if (
                        to_retire > 0
                        and not worker.exit_flag
                        and not worker.has_active_processes()
                    ):
                        print(f"Setting exit_flag for worker: {worker.name}")
                        worker.exit_flag = True
                        to_retire -= 1

                # Next we try to retire
                # without running tasks
                for worker in group.workers.values():
                    if (
                        to_retire > 0
                        and not worker.exit_flag
                        and not worker.has_running_tasks()
                    ):
                        print(f"Setting exit_flag for worker: {worker.name}")
                        worker.exit_flag = True
                        to_retire -= 1

                # Next we retire active workers
                for worker in group.workers.values():
                    if to_retire > 0 and not worker.exit_flag:
                        print(f"Setting exit_flag for worker: {worker.name}")
                        worker.exit_flag = True
                        to_retire -= 1

    def _submit(self, type: str, fn: Callable, *args, **kwargs) -> Future:
        task_id = gen_task_id()
        defn = TaskDefn(
            task_id=task_id,
            function=cloudpickle.dumps(fn, protocol=pickle.HIGHEST_PROTOCOL),
            args=cloudpickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL),
            kwargs=cloudpickle.dumps(kwargs, protocol=pickle.HIGHEST_PROTOCOL),
            type=type,
        )
        task = TaskDefnWithFut(defn=defn, fut=Future())

        with self._lock:
            group = self._groups[type]
            group.task_queue.append(task)

        return task.fut

    @typechecked
    def submit(self, type: str, fn: Callable, *args, **kwargs) -> Future:
        with self._lock:
            assert type in self._groups, "Unknown worker type"

        return self._submit(type, fn, *args, **kwargs)

    @typechecked
    def map(
        self,
        type: str,
        fn: Callable,
        *iterables: Iterable,
        chunksize: int = 1,
        unit: str = "it",
        desc: str | None = None,
    ) -> list[Any]:
        with self._lock:
            assert type in self._groups, "Unknown worker type"

        futs: list[Future] = []
        args_list_chunks = chunked(zip(*iterables), chunksize)
        for arg_list_chunk in args_list_chunks:
            futs.append(self._submit(type, _map_chunk, fn, arg_list_chunk))

        it = as_completed(futs)
        it = tqdm.tqdm(it, desc=desc, total=len(futs), unit=unit)
        for _ in it:
            pass

        results = []
        for fut in futs:
            results.extend(fut.result())

        return results

    def num_groups(self):
        with self._lock:
            return len(self._groups)

    def num_workers(self, detail: bool = False):
        with self._lock:
            if detail:
                return {g.type: len(g.workers) for g in self._groups.values()}
            else:
                return sum(len(g.workers) for g in self._groups.values())

    def num_processes(self, detail: bool = False):
        with self._lock:
            if detail:
                return {
                    g.type: {w.name: len(w.processes) for w in g.workers.values()}
                    for g in self._groups.values()
                }
            else:
                return sum(
                    len(w.processes)
                    for g in self._groups.values()
                    for w in g.workers.values()
                )

    def RegisterWorkerProcess(
        self, request: WorkerProcessID, context: grpc.ServicerContext
    ) -> Empty:
        try:
            with self._lock:
                group = self._groups[request.type]
                worker = group.workers[request.name]
                process_key = f"{request.slurm_job_id}:{request.hostname}:{request.pid}"
                process = WorkerProcess(
                    key=process_key,
                    process_id=request,
                )

                if process_key in worker.processes:
                    assert worker.processes[process_key] == process
                else:
                    worker.processes[process_key] = process

                return Empty()
        except Exception as e:
            eid = gen_error_id()
            self._logger.exception("Unexpected exception: %s: %s", eid, e)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Unexpected exception: %s: %s" % (eid, e))
            raise grpc.RpcError(context)

    def UnregisterWorkerProcess(
        self, request: WorkerProcessID, context: grpc.ServicerContext
    ) -> Empty:
        try:
            with self._lock:
                group = self._groups[request.type]
                worker = group.workers[request.name]
                process_key = f"{request.slurm_job_id}:{request.hostname}:{request.pid}"
                process = worker.processes[process_key]
                self._cleanup_process(group, worker, process)

                return Empty()
        except Exception as e:
            eid = gen_error_id()
            self._logger.exception("Unexpected exception: %s: %s", eid, e)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Unexpected exception: %s: %s" % (eid, e))
            raise grpc.RpcError(context)

    def GetNextTask(
        self, request: WorkerProcessID, context: grpc.ServicerContext
    ) -> TaskAssignment:
        try:
            with self._lock:
                group = self._groups[request.type]
                worker = group.workers[request.name]
                process_key = f"{request.slurm_job_id}:{request.hostname}:{request.pid}"
                process = worker.processes[process_key]
                assert process.running_task is None

                if group.task_queue and not worker.exit_flag:
                    task = group.task_queue.popleft()
                    process.running_task = task
                    return TaskAssignment(
                        exit_flag=worker.exit_flag, task_available=True, task=task.defn
                    )
                else:
                    return TaskAssignment(
                        exit_flag=worker.exit_flag,
                        task_available=False,
                    )
        except Exception as e:
            eid = gen_error_id()
            self._logger.exception("Unexpected exception: %s: %s", eid, e)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Unexpected exception: %s: %s" % (eid, e))
            raise grpc.RpcError(context)

    def SetTaskResult(
        self, request: TaskResult, context: grpc.ServicerContext
    ) -> Empty:
        try:
            with self._lock:
                group = self._groups[request.process_id.type]
                worker = group.workers[request.process_id.name]
                process_key = f"{request.process_id.slurm_job_id}:{request.process_id.hostname}:{request.process_id.pid}"
                process = worker.processes[process_key]
                assert process.running_task is not None

                if request.task_success:
                    process.running_task.fut.set_result(
                        cloudpickle.loads(request.return_)
                    )
                else:
                    err = RuntimeError(
                        "Error running task: %s: %s" % (request.error, request.error_id)
                    )
                    process.running_task.fut.set_exception(err)
                process.running_task = None

            return Empty()
        except Exception as e:
            eid = gen_error_id()
            self._logger.exception("Unexpected exception: %s: %s", eid, e)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details("Unexpected exception: %s: %s" % (eid, e))
            raise grpc.RpcError(context)

    def start(self):
        assert self._queue_monitor_thread is None
        assert self._server is None

        self._exit_flag.clear()
        self._queue_monitor_thread = threading.Thread(target=self._queue_monitor_main)
        self._queue_monitor_thread.start()

        options = [
            ("grpc.keepalive_time_ms", 20000),
            ("grpc.keepalive_timeout_ms", 10000),
            ("grpc.http2.min_ping_interval_without_data_ms", 5000),
            ("grpc.max_connection_idle_ms", 10000),
            ("grpc.max_connection_age_ms", 30000),
            ("grpc.max_connection_age_grace_ms", 5000),
            ("grpc.http2.max_pings_without_data", 5),
            ("grpc.keepalive_permit_without_calls", 1),
        ]

        host = data_address(None)
        port = arbitrary_free_port(host)
        self._server_address = f"{host}:{port}"
        self._server = grpc.server(ThreadPoolExecutor(max_workers=1), options=options)
        add_CoordinatorServicer_to_server(self, self._server)
        self._server.add_insecure_port(self._server_address)
        self._server.start()

    def close(self):
        if self._server is not None:
            self._server.stop(None)
            self._server = None

        if self._queue_monitor_thread is not None:
            self._exit_flag.set()
            self._queue_monitor_thread.join()
            self._queue_monitor_thread = None

            self._cleanup_all_workers()

    def stop(self):
        self.close()
