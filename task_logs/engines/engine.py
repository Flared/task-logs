from datetime import datetime

from typing import Union, List, Dict, Any, Optional
from typing_extensions import TypedDict


class Task(TypedDict):
    queue: str
    task_id: str
    task_name: str
    task_path: Optional[str]
    scheduled_at: Optional[datetime]
    args: List[Any]
    kwargs: Dict[str, Any]
    options: Dict[str, Any]


class WriteEngine:
    def log_enqueued(self, task: Task) -> None:
        raise NotImplementedError

    def log_dequeued(self, task: Task) -> None:
        raise NotImplementedError

    def log_completed(self, task: Task, result: Any) -> None:
        raise NotImplementedError

    def log_exception(self, task: Task, exception: Union[BaseException, str]) -> None:
        raise NotImplementedError

    def log_failed(self, task: Task) -> None:
        raise NotImplementedError


class ReadEngine:
    def enqueued(self) -> List[Task]:
        raise NotImplementedError

    def dequeued(self) -> List[Task]:
        raise NotImplementedError

    def completed(self) -> List[Task]:
        raise NotImplementedError

    def exception(self) -> List[Task]:
        raise NotImplementedError

    def failed(self) -> List[Task]:
        raise NotImplementedError

    def search(self, query: str) -> List[Task]:
        raise NotImplementedError

    def find_task(self, task_id: str) -> Optional[Task]:
        raise NotImplementedError
