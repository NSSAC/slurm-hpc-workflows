"""Pilot workers for Slurm pilot."""

import os
import sys
import time
import json
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
)
from .slurm_pilot_pb2_grpc import (
    CoordinatorStub,
)
from .slurm_pilot_executor import gen_error_id

NEXT_TASK_RETRY_TIME_S: float = 1.0
LOG_FORMAT: str = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"
LOG_LEVEL = logging.INFO


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
        loads_input_duration = float("nan")
        run_duration = float("nan")
        dumps_output_duration = float("nan")
        try:
            self._logger.info(
                "task_id=%s: Deserializing function and inputs ...", task.task_id
            )
            start_time = time.perf_counter()
            function = cloudpickle.loads(task.function)
            args = cloudpickle.loads(task.args)
            kwargs = cloudpickle.loads(task.kwargs)
            loads_input_duration = time.perf_counter() - start_time

            self._logger.info("task_id=%s: Executing ...", task.task_id)
            start_time = time.perf_counter()
            return_ = function(*args, **kwargs)
            run_duration = time.perf_counter() - start_time

            self._logger.info("task_id=%s: Serializng output ...", task.task_id)
            start_time = time.perf_counter()
            return_ = cloudpickle.dumps(return_, protocol=pickle.HIGHEST_PROTOCOL)
            dumps_output_duration = time.perf_counter() - start_time

            self._logger.info("task_id=%s: Complete ...", task.task_id)
            result = TaskResult(
                task_id=task.task_id,
                task_success=True,
                return_=return_,
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
                task_success=False,
                error=f"{type(e)}: {e}",
                error_id=eid,
                process_id=self._process_id,
                loads_input_duration=loads_input_duration,
                run_duration=run_duration,
                dumps_output_duration=dumps_output_duration,
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
@click.option("--name", type=str, required=True, help="Worker job name")
@click.option("--server-address", type=str, required=True, help="Pilot server address")
@click.option(
    "--work-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Work directory",
)
def slurm_pilot_worker(type: str, name: str, server_address: str, work_dir: Path):
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

        os.environ["PILOT_PROCESS_NAME"] = name
        os.environ["PILOT_PROCESS_TYPE"] = type

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
