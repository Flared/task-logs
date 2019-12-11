import traceback
from typing import List, cast, Union, Optional
from .backend import (
    ReaderBackend,
    WriterBackend,
    TaskDetails,
    Log,
    EnqueuedLog,
    DequeuedLog,
    CompletedLog,
    ExceptionLog,
    FailedLog,
)


class StubBackend(ReaderBackend, WriterBackend):
    def __init__(self) -> None:
        self.logs: List[Log] = []

    def write(self, log: Log) -> None:
        self.logs.append(log)

    def write_enqueued(
        self, *, task_id: str, task_name: str, task: TaskDetails
    ) -> None:
        if task.get("args") is not None:
            task["args"] = list(task["args"])

        super().write_enqueued(task_id=task_id, task_name=task_name, task=task)

    def write_exception(
        self, *, task_id: str, task_name: str, exception: Union[BaseException, str]
    ) -> None:
        if isinstance(exception, BaseException):
            exception = "\n".join(
                traceback.format_exception(
                    etype=type(exception), value=exception, tb=exception.__traceback__
                )
            )
        return super().write_exception(
            task_id=task_id, task_name=task_name, exception=exception
        )

    def search(self, query: str) -> List[Log]:
        return [l for l in self.logs if query in str(l)]

    def find_task(self, task_id: str) -> List[Log]:
        return [l for l in self.logs if l["task_id"] == task_id]

    def logs_by_type(self, type: Optional[str]) -> List[Log]:
        if type:
            return [l for l in self.logs if l["type"] == type]
        return self.logs
