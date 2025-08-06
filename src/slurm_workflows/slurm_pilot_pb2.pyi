from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WorkerProcessID(_message.Message):
    __slots__ = ("type", "name", "slurm_job_id", "hostname", "pid")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLURM_JOB_ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    PID_FIELD_NUMBER: _ClassVar[int]
    type: str
    name: str
    slurm_job_id: int
    hostname: str
    pid: int
    def __init__(self, type: _Optional[str] = ..., name: _Optional[str] = ..., slurm_job_id: _Optional[int] = ..., hostname: _Optional[str] = ..., pid: _Optional[int] = ...) -> None: ...

class TaskDefn(_message.Message):
    __slots__ = ("task_id", "function", "args", "kwargs")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    function: bytes
    args: bytes
    kwargs: bytes
    def __init__(self, task_id: _Optional[str] = ..., function: _Optional[bytes] = ..., args: _Optional[bytes] = ..., kwargs: _Optional[bytes] = ...) -> None: ...

class TaskAssignment(_message.Message):
    __slots__ = ("exit_flag", "task_available", "task")
    EXIT_FLAG_FIELD_NUMBER: _ClassVar[int]
    TASK_AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    exit_flag: bool
    task_available: bool
    task: TaskDefn
    def __init__(self, exit_flag: bool = ..., task_available: bool = ..., task: _Optional[_Union[TaskDefn, _Mapping]] = ...) -> None: ...

class TaskResult(_message.Message):
    __slots__ = ("task_id", "task_success", "return_", "error", "error_id", "process_id")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_SUCCESS_FIELD_NUMBER: _ClassVar[int]
    RETURN__FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESS_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    task_success: bool
    return_: bytes
    error: str
    error_id: str
    process_id: WorkerProcessID
    def __init__(self, task_id: _Optional[str] = ..., task_success: bool = ..., return_: _Optional[bytes] = ..., error: _Optional[str] = ..., error_id: _Optional[str] = ..., process_id: _Optional[_Union[WorkerProcessID, _Mapping]] = ...) -> None: ...
