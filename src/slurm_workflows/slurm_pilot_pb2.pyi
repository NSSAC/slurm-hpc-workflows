from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WorkerProcessID(_message.Message):
    __slots__ = ("group", "name", "slurm_job_id", "hostname", "pid")
    GROUP_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SLURM_JOB_ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    PID_FIELD_NUMBER: _ClassVar[int]
    group: str
    name: str
    slurm_job_id: int
    hostname: str
    pid: int
    def __init__(self, group: _Optional[str] = ..., name: _Optional[str] = ..., slurm_job_id: _Optional[int] = ..., hostname: _Optional[str] = ..., pid: _Optional[int] = ...) -> None: ...

class TaskDefn(_message.Message):
    __slots__ = ("task_id", "function_call")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_CALL_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    function_call: FunctionCall
    def __init__(self, task_id: _Optional[str] = ..., function_call: _Optional[_Union[FunctionCall, _Mapping]] = ...) -> None: ...

class FunctionCall(_message.Message):
    __slots__ = ("function", "args", "kwargs")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    function: bytes
    args: bytes
    kwargs: bytes
    def __init__(self, function: _Optional[bytes] = ..., args: _Optional[bytes] = ..., kwargs: _Optional[bytes] = ...) -> None: ...

class TaskAssignment(_message.Message):
    __slots__ = ("exit_flag", "task")
    EXIT_FLAG_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    exit_flag: bool
    task: TaskDefn
    def __init__(self, exit_flag: bool = ..., task: _Optional[_Union[TaskDefn, _Mapping]] = ...) -> None: ...

class TaskResult(_message.Message):
    __slots__ = ("task_id", "retval", "error", "process_id", "loads_input_duration", "run_duration", "dumps_output_duration")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    RETVAL_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    PROCESS_ID_FIELD_NUMBER: _ClassVar[int]
    LOADS_INPUT_DURATION_FIELD_NUMBER: _ClassVar[int]
    RUN_DURATION_FIELD_NUMBER: _ClassVar[int]
    DUMPS_OUTPUT_DURATION_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    retval: bytes
    error: Error
    process_id: WorkerProcessID
    loads_input_duration: float
    run_duration: float
    dumps_output_duration: float
    def __init__(self, task_id: _Optional[str] = ..., retval: _Optional[bytes] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., process_id: _Optional[_Union[WorkerProcessID, _Mapping]] = ..., loads_input_duration: _Optional[float] = ..., run_duration: _Optional[float] = ..., dumps_output_duration: _Optional[float] = ...) -> None: ...

class Error(_message.Message):
    __slots__ = ("message", "error_id")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_ID_FIELD_NUMBER: _ClassVar[int]
    message: str
    error_id: str
    def __init__(self, message: _Optional[str] = ..., error_id: _Optional[str] = ...) -> None: ...
