from typing import List, cast
from .backend import (
    ReaderBackend,
    WriterBackend,
    Task,
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

    def write_enqueued(self, task: Task) -> None:
        if task.get("args") is not None:
            task["args"] = list(task["args"])

        super().write_enqueued(task)

    def search(self, query: str) -> List[Log]:
        return [l for l in self.logs if query in str(l)]

    def find_task(self, task_id: str) -> List[Log]:
        return [l for l in self.logs if l["task_id"] == task_id]

    def logs_by_type(self, type: str) -> List[Log]:
        return [l for l in self.logs if l["type"] == type]
