"""Pilot workers for slurm."""

from __future__ import annotations

import time
import heapq
import pickle
import logging
import random
import string
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Callable, Iterable, Any
from functools import total_ordering

import tqdm
import grpc
import platformdirs
import cloudpickle
from more_itertools import chunked
from typeguard import typechecked
from runstats import Statistics

from .slurm_utils import (
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
    FunctionCall,
)
from .slurm_pilot_pb2_grpc import (
    CoordinatorServicer,
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


@typechecked
def wait_with_progress(
    futs: list[Future], desc: str | None = None, unit: str = "it"
) -> None:
    it = as_completed(futs)
    it = tqdm.tqdm(it, desc=desc, total=len(futs), unit=unit)
    for _ in it:
        pass


@total_ordering
@dataclass(eq=False, kw_only=True)
class TaskMeta:
    defn: TaskDefn
    fut: Future
    priority: float
    groups: list[str]
    is_assigned: bool = False
    submit_time: float = field(default_factory=time.perf_counter)
    wait_duration: float = float("inf")

    @property
    def id(self) -> str:
        return self.defn.task_id

    def update_wait_duration(self):
        self.wait_duration = time.perf_counter() - self.submit_time

    def __eq__(self, other) -> bool:
        if isinstance(other, TaskMeta):
            return self.priority == other.priority
        return NotImplemented

    def __lt__(self, other) -> bool:
        if isinstance(other, TaskMeta):
            return self.priority < other.priority
        return NotImplemented


@dataclass
class TaskQueue:
    queue: list[TaskMeta] = field(default_factory=list)

    def push(self, task: TaskMeta):
        assert task.is_assigned == False
        heapq.heappush(self.queue, task)

    def pop(self) -> TaskMeta | None:
        while self.queue:
            task = heapq.heappop(self.queue)
            if not task.is_assigned:
                task.is_assigned = True
                task.update_wait_duration()
                return task

        return None


@dataclass
class WorkerProcess:
    key: str
    process_id: WorkerProcessID
    running_task: TaskMeta | None = None


@dataclass
class Worker:
    name: str
    slurm_job: SlurmJob
    exit_flag: bool = False
    is_running: bool = True
    is_new: bool = True
    processes: dict[str, WorkerProcess] = field(default_factory=dict)
    submit_time: float = field(default_factory=time.perf_counter)

    def has_active_processes(self) -> bool:
        return len(self.processes) > 0

    def has_running_tasks(self) -> bool:
        return any(p.running_task is not None for p in self.processes.values())


@dataclass
class WorkerGroup:
    name: str
    sbatch_args: list[str]
    is_batch_worker: bool
    workers: dict[str, Worker] = field(default_factory=dict)
    next_worker_index: int = 0
    task_queue: TaskQueue = field(default_factory=TaskQueue)

    # Available workers: workers with exit_flag == False
    def available_workers(self) -> int:
        return sum(1 for w in self.workers.values() if w.exit_flag)


def _map_chunk(fn: Callable, args_list: list):
    results = []
    for args in args_list:
        results.append(fn(*args))
    return results


@dataclass(slots=True)
class ProcessMetrics:
    process_key: str
    group: str
    init_duration: float


@dataclass(slots=True)
class TaskMetrics:
    task_id: str
    wait_duration: float
    group: str
    process_key: str
    loads_input_duration: float
    run_duration: float
    dumps_output_duration: float


@dataclass
class Metrics:
    start_time: float = float("nan")
    stop_time: float = float("nan")
    queue_access_duration: Statistics = field(default_factory=Statistics)
    process_metrics: list[ProcessMetrics] = field(default_factory=list)
    task_metrics: list[TaskMetrics] = field(default_factory=list)

    def run_time(self) -> float:
        return self.stop_time - self.start_time


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

        self.metrics = Metrics()

    def _queue_task(self, task: TaskMeta):
        task.is_assigned = False
        for group in task.groups:
            self._groups[group].task_queue.push(task)

    def _cleanup_process(self, worker: Worker, process: WorkerProcess):
        if process.running_task is not None:
            self._queue_task(process.running_task)
            process.running_task = None
        del worker.processes[process.key]

    def _cleanup_worker(self, group: WorkerGroup, worker: Worker):
        self._logger.info(f"Cleaning up worker %s", worker.name)
        for key in list(worker.processes):
            process = worker.processes[key]
            self._cleanup_process(worker, process)
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

        to_cancel_jobs: list[int] = []
        with self._lock:
            for group in self._groups.values():
                for name in list(group.workers):
                    worker = group.workers[name]
                    if worker.slurm_job.job_id in job_ids:
                        to_cancel_jobs.append(worker.slurm_job.job_id)
                    self._cleanup_worker(group, worker)

        if to_cancel_jobs:
            try:
                cancel_jobs(to_cancel_jobs)
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
        self, name: str, sbatch_args: list[str], is_batch_worker: bool = False
    ) -> None:
        with self._lock:
            group = WorkerGroup(
                name=name, sbatch_args=sbatch_args, is_batch_worker=is_batch_worker
            )

            if group.name in self._groups:
                assert self._groups[group.name] == group
            else:
                self._groups[group.name] = group

    def _add_worker(self, group: WorkerGroup) -> None:
        assert self._server_address is not None

        worker_index = group.next_worker_index
        group.next_worker_index += 1
        name = f"slurm_pilot_worker.{group.name}.{worker_index}"

        worker_script = render_template(
            "slurm_pilot:worker_script",
            group=group.name,
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

        self._logger.info("Starting worker %s", name)
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
    def scale_workers(self, name: str, count: int) -> None:
        with self._lock:
            assert name in self._groups, "Unknown worker type"

            group = self._groups[name]
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
                        self._logger.info(
                            "Setting exit_flag for worker: %s", worker.name
                        )
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
                        self._logger.info(
                            "Setting exit_flag for worker: %s", worker.name
                        )
                        worker.exit_flag = True
                        to_retire -= 1

                # Next we retire active workers
                for worker in group.workers.values():
                    if to_retire > 0 and not worker.exit_flag:
                        self._logger.info(
                            "Setting exit_flag for worker: %s", worker.name
                        )
                        worker.exit_flag = True
                        to_retire -= 1

    def _submit(
        self,
        task_id: str,
        groups: list[str],
        priority: float,
        fn: Callable,
        *args,
        **kwargs,
    ) -> Future:
        defn = TaskDefn(
            task_id=task_id,
            function_call=FunctionCall(
                function=cloudpickle.dumps(fn, protocol=pickle.HIGHEST_PROTOCOL),
                args=cloudpickle.dumps(args, protocol=pickle.HIGHEST_PROTOCOL),
                kwargs=cloudpickle.dumps(kwargs, protocol=pickle.HIGHEST_PROTOCOL),
            ),
        )
        task = TaskMeta(defn=defn, fut=Future(), priority=priority, groups=groups)

        with self._lock:
            self._queue_task(task)

        return task.fut

    @typechecked
    def submit(
        self, task_id: str, group: str | list[str], fn: Callable, *args, **kwargs
    ) -> Future:
        if isinstance(group, str):
            groups = [group]
        else:
            groups = group
        priority = time.perf_counter()

        return self._submit(task_id, groups, priority, fn, *args, **kwargs)

    @typechecked
    def map(
        self,
        task_id: str,
        group: str | list[str],
        fn: Callable,
        *iterables: Iterable,
        chunksize: int = 1,
        unit: str = "it",
        desc: str | None = None,
    ) -> list[Any]:
        if isinstance(group, str):
            groups = [group]
        else:
            groups = group
        priority = time.perf_counter()

        futs: list[Future] = []
        args_list_chunks = chunked(zip(*iterables), chunksize)
        for i, arg_list_chunk in enumerate(args_list_chunks, 1):
            tid = f"{task_id}:chunk-{i}"
            futs.append(
                self._submit(tid, groups, priority, _map_chunk, fn, arg_list_chunk)
            )

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
                return {g.name: len(g.workers) for g in self._groups.values()}
            else:
                return sum(len(g.workers) for g in self._groups.values())

    def num_processes(self, detail: bool = False):
        with self._lock:
            if detail:
                return {
                    g.name: {w.name: len(w.processes) for w in g.workers.values()}
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
                group = self._groups[request.group]
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
                    self.metrics.process_metrics.append(
                        ProcessMetrics(
                            process_key=process_key,
                            group=request.group,
                            init_duration=time.perf_counter() - worker.submit_time,
                        )
                    )

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
                group = self._groups[request.group]
                worker = group.workers[request.name]
                process_key = f"{request.slurm_job_id}:{request.hostname}:{request.pid}"
                process = worker.processes[process_key]
                self._cleanup_process(worker, process)

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
        start_time = time.perf_counter()
        try:
            with self._lock:
                group = self._groups[request.group]
                worker = group.workers[request.name]
                process_key = f"{request.slurm_job_id}:{request.hostname}:{request.pid}"
                process = worker.processes[process_key]
                assert process.running_task is None

                if worker.exit_flag:
                    return TaskAssignment(exit_flag=True)
                else:
                    task = group.task_queue.pop()
                    self.metrics.queue_access_duration.push(
                        time.perf_counter() - start_time
                    )

                    if task is not None:
                        process.running_task = task
                        return TaskAssignment(task=task.defn)
                    else:
                        return TaskAssignment()
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
                group = self._groups[request.process_id.group]
                worker = group.workers[request.process_id.name]
                process_key = f"{request.process_id.slurm_job_id}:{request.process_id.hostname}:{request.process_id.pid}"
                process = worker.processes[process_key]
                assert process.running_task is not None

                match request.WhichOneof("result"):
                    case "retval":
                        process.running_task.fut.set_result(
                            cloudpickle.loads(request.retval)
                        )
                    case "error":
                        err = RuntimeError(
                            "Error running task: %s: %s"
                            % (request.error.message, request.error.error_id)
                        )
                        process.running_task.fut.set_exception(err)
                    case _ as unexpected:
                        raise RuntimeError(f"Unexpected: {unexpected!r}")

                self.metrics.task_metrics.append(
                    TaskMetrics(
                        task_id=process.running_task.id,
                        wait_duration=process.running_task.wait_duration,
                        group=request.process_id.group,
                        process_key=process_key,
                        loads_input_duration=request.loads_input_duration,
                        run_duration=request.run_duration,
                        dumps_output_duration=request.dumps_output_duration,
                    )
                )
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

        self.metrics.start_time = time.perf_counter()

    def close(self):
        if self._server is not None:
            self._server.stop(None)
            self._server = None

            self.metrics.stop_time = time.perf_counter()

        if self._queue_monitor_thread is not None:
            self._exit_flag.set()
            self._queue_monitor_thread.join()
            self._queue_monitor_thread = None

            self._cleanup_all_workers()

    def stop(self):
        self.close()
