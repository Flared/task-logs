import abc
import dataclasses
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast

from typing_extensions import Literal


@dataclasses.dataclass
class JobDetails:
    queue: str
    task_path: Optional[str]
    execute_at: Optional[datetime]
    args: List[Any]
    kwargs: Dict[str, Any]
    options: Dict[str, Any]


LogType = Union[
    Literal["enqueued"],
    Literal["dequeued"],
    Literal["completed"],
    Literal["exception"],
]


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
                type="enqueued",
                job=job,
                job_id=job_id,
                task_id=task_id,
                timestamp=datetime.now(),
            )
        )

    def write_dequeued(self, *, job_id: str, task_id: str) -> None:
        self.write(
            DequeuedLog(
                type="dequeued",
                job_id=job_id,
                task_id=task_id,
                timestamp=datetime.now(),
            )
        )

    def write_completed(self, *, job_id: str, task_id: str, result: Any) -> None:
        self.write(
            CompletedLog(
                type="completed",
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
                type="exception",
                job_id=job_id,
                task_id=task_id,
                exception=exception,
                timestamp=datetime.now(),
            )
        )


class ReaderBackend(abc.ABC):
    def enqueued(self) -> List[EnqueuedLog]:
        return cast(List[EnqueuedLog], self.logs_by_type("enqueued"))

    def dequeued(self) -> List[DequeuedLog]:
        return cast(List[DequeuedLog], self.logs_by_type("dequeued"))

    def completed(self) -> List[CompletedLog]:
        return cast(List[CompletedLog], self.logs_by_type("completed"))

    def exception(self) -> List[ExceptionLog]:
        return cast(List[ExceptionLog], self.logs_by_type("exception"))

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
