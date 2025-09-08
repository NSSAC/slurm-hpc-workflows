"""Pilot workers for Slurm pilot."""

import os
import sys
import json
import time
import pickle
import socket
import logging
from pathlib import Path

import grpc
import click
import cloudpickle

from .slurm_pilot_pb2 import (
    WorkerProcessID,
    TaskDefn,
    TaskAssignment,
    TaskResult,
    Error,
)
from .slurm_pilot_pb2_grpc import CoordinatorStub
from .utils import gen_error_id, LOG_FORMAT, LOG_LEVEL, GRPC_CLIENT_OPTIONS

NEXT_TASK_RETRY_TIME_S: float = 1.0


class PilotProcess:
    def __init__(
        self,
        group: str,
        name: str,
        server_address: str,
        work_dir: Path,
        slurm_job_id: int,
        hostname: str,
        pid: int,
    ):
        self._process_id = WorkerProcessID(
            group=group,
            name=name,
            slurm_job_id=slurm_job_id,
            hostname=hostname,
            pid=pid,
        )

        self._server_address = server_address
        self._work_dir = work_dir
        self._exit_flag = False
        self._logger = logging.getLogger("worker_process")

        self._channel = grpc.insecure_channel(
            self._server_address, options=GRPC_CLIENT_OPTIONS
        )
        self._stub = CoordinatorStub(self._channel)

    def close(self):
        self._channel.close()

    def RegisterWorkerProcess(self) -> None:
        self._stub.RegisterWorkerProcess(self._process_id)

    def UnregisterWorkerProcess(self) -> None:
        self._stub.UnregisterWorkerProcess(self._process_id)

    def GetNextTask(self) -> TaskDefn | None:
        assignment: TaskAssignment = self._stub.GetNextTask(self._process_id)
        match assignment.WhichOneof("assignment"):
            case "exit_flag":
                assert assignment.exit_flag
                self._exit_flag = True
                return None
            case "task":
                return assignment.task
            case None:
                return None
            case _ as unexpected:
                raise RuntimeError(f"Unexpected: {unexpected!r}")

    def SetTaskResult(self, result: TaskResult) -> None:
        self._stub.SetTaskResult(result)

    def do_run_task(self, task: TaskDefn) -> TaskResult:
        loads_input_duration = float("nan")
        run_duration = float("nan")
        dumps_output_duration = float("nan")
        try:
            self._logger.info(
                "task_id=%s: Deserializing function and inputs ...", task.task_id
            )
            start_time = time.perf_counter()
            function = cloudpickle.loads(task.function_call.function)
            args = cloudpickle.loads(task.function_call.args)
            kwargs = cloudpickle.loads(task.function_call.kwargs)
            loads_input_duration = time.perf_counter() - start_time

            self._logger.info("task_id=%s: Executing ...", task.task_id)
            start_time = time.perf_counter()
            retval = function(*args, **kwargs)
            run_duration = time.perf_counter() - start_time

            self._logger.info("task_id=%s: Serializng output ...", task.task_id)
            start_time = time.perf_counter()
            retval = cloudpickle.dumps(retval, protocol=pickle.HIGHEST_PROTOCOL)
            dumps_output_duration = time.perf_counter() - start_time

            self._logger.info("task_id=%s: Complete ...", task.task_id)
            result = TaskResult(
                task_id=task.task_id,
                retval=retval,
                process_id=self._process_id,
                loads_input_duration=loads_input_duration,
                run_duration=run_duration,
                dumps_output_duration=dumps_output_duration,
            )
        except Exception as e:
            eid = gen_error_id()
            self._logger.exception("Error executing %s: %s: %s", task.task_id, eid, e)
            result = TaskResult(
                task_id=task.task_id,
                error=Error(
                    message=f"{type(e)}: {e}",
                    error_id=eid,
                ),
                process_id=self._process_id,
                loads_input_duration=loads_input_duration,
                run_duration=run_duration,
                dumps_output_duration=dumps_output_duration,
            )

        return result

    def main(self):
        self._logger.info("Registering worker process.")
        self.RegisterWorkerProcess()
        while True:
            task = self.GetNextTask()
            if task is not None:
                result = self.do_run_task(task)
                self.SetTaskResult(result)

            if self._exit_flag:
                self._logger.info("Unregistering worker process.")
                self.UnregisterWorkerProcess()
                self.close()

                # Once we receive the exit flag we no longer receive any tasks.
                # However, other processes for this job
                # may be still executing tasks.
                # If this process exits, slurm will kill the other processes too.
                # So we enter into a infinite loop.
                # The coordinator is responsible for killing this job
                # when none of the processes belonging to this job
                # are processing any tasks.
                while True:
                    time.sleep(60.0)

            else:
                time.sleep(NEXT_TASK_RETRY_TIME_S)


@click.command()
@click.option("--group", type=str, required=True, help="Worker group")
@click.option("--name", type=str, required=True, help="Worker job name")
@click.option("--server-address", type=str, required=True, help="Pilot server address")
@click.option(
    "--work-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Work directory",
)
@click.option(
    "--python-paths-json",
    type=str,
    required=True,
    help="JSON encoded Python paths",
)
def slurm_pilot_worker(
    group: str, name: str, server_address: str, work_dir: Path, python_paths_json: str
):
    """Start a slurm pilot worker."""
    slurm_job_id = int(os.environ.get("SLURM_JOB_ID", -1))
    hostname = socket.gethostname()
    pid = os.getpid()
    log_file = work_dir / f"{name}-{slurm_job_id}-{hostname}-{pid}.log"

    print(f"Redirecting standard output and standard error to {log_file}")
    sys.stdout.flush()
    sys.stderr.flush()

    with open(log_file, "wt") as fout:
        sys.stdout = fout
        sys.stderr = fout

        logging.basicConfig(stream=fout, format=LOG_FORMAT, level=LOG_LEVEL)

        os.environ["PILOT_WORKER_NAME"] = name
        os.environ["PILOT_WORKER_GROUP"] = group

        python_paths: list[str] = json.loads(python_paths_json)
        for path in python_paths:
            sys.path.insert(0, path)

        worker = PilotProcess(
            group=group,
            name=name,
            server_address=server_address,
            work_dir=work_dir,
            slurm_job_id=slurm_job_id,
            hostname=hostname,
            pid=pid,
        )
        worker.main()
