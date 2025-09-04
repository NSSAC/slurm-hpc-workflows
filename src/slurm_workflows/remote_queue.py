"""Remote queue."""

import time
import queue
import pickle
from typing import Any

import grpc
import cloudpickle

from slurm_workflows.slurm_pilot_pb2 import (
    QueueID,
    QueueGetResponse,
    QueuePutRequest,
    QueuePutResponse,
    SHUTDOWN,
)

from .slurm_pilot_pb2_grpc import CoordinatorStub
from .utils import GRPC_CLIENT_OPTIONS

INTER_QUEUE_REQUEST_TIME: float = 0.1


class RemoteQueue:
    """Remote queue."""

    def __init__(self, server_address: str, queue: str):
        self.server_address = server_address
        self.queue = queue

        self.channel = grpc.insecure_channel(
            self.server_address, options=GRPC_CLIENT_OPTIONS
        )
        self.stub = CoordinatorStub(self.channel)

    def __getstate__(self):
        return self.server_address, self.queue

    def __setstate__(self, state):
        self.server_address, self.queue = state

        self.channel = grpc.insecure_channel(
            self.server_address, options=GRPC_CLIENT_OPTIONS
        )
        self.stub = CoordinatorStub(self.channel)

    def close(self):
        self.channel.close()

    def put(self, item, block: bool = True, timeout: float | None = None):
        item_pkl = cloudpickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
        start_time = time.perf_counter()

        while True:
            response: QueuePutResponse = self.stub.QueueTryPut(
                QueuePutRequest(queue=self.queue, item=item_pkl)
            )
            if response.HasField("error"):
                if response.error == SHUTDOWN:
                    raise queue.ShutDown()
            else:
                return

            if not block:
                raise queue.Full()

            if timeout is not None and time.perf_counter() - start_time > timeout:
                raise queue.Full()

            time.sleep(INTER_QUEUE_REQUEST_TIME)

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        start_time = time.perf_counter()

        while True:
            response: QueueGetResponse = self.stub.QueueTryGet(
                QueueID(queue=self.queue)
            )
            if response.HasField("item"):
                return cloudpickle.loads(response.item)
            elif response.HasField("error"):
                if response.error == SHUTDOWN:
                    raise queue.ShutDown()

            if not block:
                raise queue.Empty()

            if timeout is not None and time.perf_counter() - start_time > timeout:
                raise queue.Empty()

            time.sleep(INTER_QUEUE_REQUEST_TIME)

    def shutdown(self):
        self.stub.QueueShutdown(QueueID(queue=self.queue))
