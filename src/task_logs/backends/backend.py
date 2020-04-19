import abc
import dataclasses
import enum
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast


@dataclasses.dataclass
class JobDetails:
    queue: str
    task_path: Optional[str]
    execute_at: Optional[datetime]
    args: List[Any]
    kwargs: Dict[str, Any]
    options: Dict[str, Any]


class LogType(str, enum.Enum):
    ENQUEUED = "enqueued"
    DEQUEUED = "dequeued"
    COMPLETED = "completed"
    EXCEPTION = "exception"
    FAILED = "failed"


@dataclasses.dataclass
class Task:
    id: str


@dataclasses.dataclass
class Log:
    type: LogType
    timestamp: datetime
    job_id: str
    task_id: str


@dataclasses.dataclass
class EnqueuedLog(Log):
    job: JobDetails

    def __post_init__(self) -> None:
        if isinstance(self.job, dict):
            self.job = JobDetails(**self.job)


@dataclasses.dataclass
class DequeuedLog(Log):
    pass


@dataclasses.dataclass
class CompletedLog(Log):
    result: Any


@dataclasses.dataclass
class ExceptionLog(Log):
    exception: Union[BaseException, str]


@dataclasses.dataclass
class FailedLog(Log):
    pass


class WriterBackend(abc.ABC):
    @abc.abstractmethod
    def write(self, log: Log) -> None:
        raise NotImplementedError

    def write_enqueued(self, *, job_id: str, task_id: str, job: JobDetails) -> None:
        self.write(
            EnqueuedLog(
                type=LogType.ENQUEUED,
                job=job,
                job_id=job_id,
                task_id=task_id,
                timestamp=datetime.now(),
            )
        )

    def write_dequeued(self, *, job_id: str, task_id: str) -> None:
        self.write(
            DequeuedLog(
                type=LogType.DEQUEUED,
                job_id=job_id,
                task_id=task_id,
                timestamp=datetime.now(),
            )
        )

    def write_completed(self, *, job_id: str, task_id: str, result: Any) -> None:
        self.write(
            CompletedLog(
                type=LogType.COMPLETED,
                job_id=job_id,
                task_id=task_id,
                result=result,
                timestamp=datetime.now(),
            )
        )

    def write_exception(
        self, *, job_id: str, task_id: str, exception: Union[BaseException, str]
    ) -> None:
        self.write(
            ExceptionLog(
                type=LogType.EXCEPTION,
                job_id=job_id,
                task_id=task_id,
                exception=exception,
                timestamp=datetime.now(),
            )
        )


class ReaderBackend(abc.ABC):
    def enqueued(self) -> List[EnqueuedLog]:
        return cast(List[EnqueuedLog], self.logs_by_type(LogType.ENQUEUED))

    def dequeued(self) -> List[DequeuedLog]:
        return cast(List[DequeuedLog], self.logs_by_type(LogType.DEQUEUED))

    def completed(self) -> List[CompletedLog]:
        return cast(List[CompletedLog], self.logs_by_type(LogType.COMPLETED))

    def exception(self) -> List[ExceptionLog]:
        return cast(List[ExceptionLog], self.logs_by_type(LogType.EXCEPTION))

    def all(self) -> List[Log]:
        return self.logs_by_type(None)

    @abc.abstractmethod
    def find_job(self, job_id: str) -> List[Log]:
        raise NotImplementedError

    @abc.abstractmethod
    def logs_by_type(self, type: Optional[str]) -> List[Log]:
        raise NotImplementedError

    @abc.abstractmethod
    def search(self, query: str) -> List[Log]:
        raise NotImplementedError

    def list_task(self) -> List[Task]:
        raise NotImplementedError
