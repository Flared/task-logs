import abc

from datetime import datetime

from typing import Union, List, Dict, Any, Optional
from typing_extensions import TypedDict, Literal


class Task(TypedDict):
    queue: str
    task_id: str
    task_name: str
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
    Literal["failed"],
]


class Log(TypedDict):
    type: LogType
    timestamp: datetime
    task_id: str


class EnqueuedLog(Log):
    task: Task


class DequeuedLog(Log):
    pass


class CompletedLog(Log):
    result: Any


class ExceptionLog(Log):
    exception: Union[BaseException, str]


class FailedLog(Log):
    pass


class WriterBackend(abc.ABC):
    @abc.abstractmethod
    def write(self, log: Log) -> None:
        raise NotImplementedError

    def write_enqueued(self, task: Task) -> None:
        self.write(
            EnqueuedLog(
                type="enqueued",
                task=task,
                task_id=task["task_id"],
                timestamp=datetime.now(),
            )
        )

    def write_dequeued(self, task_id: str) -> None:
        self.write(
            DequeuedLog(type="dequeued", task_id=task_id, timestamp=datetime.now())
        )

    def write_completed(self, task_id: str, *, result: Any) -> None:
        self.write(
            CompletedLog(
                type="completed",
                task_id=task_id,
                result=result,
                timestamp=datetime.now(),
            )
        )

    def write_exception(
        self, task_id: str, *, exception: Union[BaseException, str]
    ) -> None:
        self.write(
            ExceptionLog(
                type="exception",
                task_id=task_id,
                exception=exception,
                timestamp=datetime.now(),
            )
        )

    def write_failed(self, task_id: str) -> None:
        self.write(FailedLog(type="failed", task_id=task_id, timestamp=datetime.now()))


class ReaderBackend(abc.ABC):
    @abc.abstractmethod
    def enqueued(self) -> List[EnqueuedLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def dequeued(self) -> List[DequeuedLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def completed(self) -> List[CompletedLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def exception(self) -> List[ExceptionLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def failed(self) -> List[FailedLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def search(self, query: str) -> List[Log]:
        raise NotImplementedError

    @abc.abstractmethod
    def find_task(self, task_id: str) -> List[Log]:
        raise NotImplementedError
