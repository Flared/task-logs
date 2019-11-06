import json
from datetime import datetime
from typing import IO, Any, Union, Iterable, List, Optional, Dict

from . import json_encoder
from .engine import WriteEngine, ReadEngine, Task


class FileWriteEngine(WriteEngine):
    def __init__(self, logfile: IO[str]):
        self.logfile = logfile

    def log_enqueued(self, task: Task) -> None:
        self.logfile.write(
            json.dumps(
                {"type": "enqueued", "@timestamp": datetime.now(), "task": task},
                default=json_encoder,
            )
            + "\n"
        )

    def log_dequeued(self, task_id: str) -> None:
        self.logfile.write(
            json.dumps(
                {"type": "dequeued", "@timestamp": datetime.now(), "task_id": task_id},
                default=json_encoder,
            )
            + "\n"
        )

    def log_completed(self, task_id: str, result: Any) -> None:
        self.logfile.write(
            json.dumps(
                {
                    "type": "completed",
                    "@timestamp": datetime.now(),
                    "task_id": task_id,
                    "result": result,
                },
                default=json_encoder,
            )
            + "\n"
        )

    def log_exception(self, task_id: str, exception: Union[BaseException, str]) -> None:
        self.logfile.write(
            json.dumps(
                {
                    "type": "exception",
                    "@timestamp": datetime.now(),
                    "task_id": task_id,
                    "exception": exception,
                },
                default=json_encoder,
            )
            + "\n"
        )

    def log_failed(self, task_id: str) -> None:
        self.logfile.write(
            json.dumps(
                {"type": "failed", "@timestamp": datetime.now(), "task_id": task_id},
                default=json_encoder,
            )
            + "\n"
        )


class FileReadEngine(ReadEngine):
    def __init__(self, logfile: IO[str]):
        self.logfile = logfile
        self.tasks: Dict[str, List[Task]] = {}
        self.tasks_index: Dict[str, Task] = {}

        self._reload()

    def enqueued(self) -> List[Task]:
        return self.tasks["enqueued"]

    def dequeued(self) -> List[Task]:
        return self.tasks["dequeued"]

    def completed(self) -> List[Task]:
        return self.tasks["completed"]

    def exception(self) -> List[Task]:
        return self.tasks["exception"]

    def failed(self) -> List[Task]:
        return self.tasks["failed"]

    def search(self, query: str) -> List[Task]:
        raise ValueError("Search not supported for FileEngine")

    def find_task(self, task_id: str) -> Optional[Task]:
        return self.tasks_index.get(task_id)

    @staticmethod
    def _sorted(tasks: Iterable[Task]) -> List[Task]:
        return list(sorted(tasks, key=lambda t: t["enqueued_at"]))

    def _reload(self) -> None:
        self.logfile.seek(0)

        tasks: Dict[str, Dict[str, Task]] = {
            "enqueued": {},
            "dequeued": {},
            "completed": {},
            "exception": {},
            "failed": {},
        }
        tasks_state: Dict[str, str] = {}
        self.tasks = {}
        self.tasks_index = {}

        for line in self.logfile:
            log = self._load_task(line)
            task_type = log["type"]
            task = log["task"]
            task_id = task["task_id"]

            if task_id in tasks_state:
                tasks[tasks_state[task_id]].pop(task_id, None)
            tasks_state[task["task_id"]] = task_type

            if task_type == "dequeued":
                task["dequeued_at"] = log[""]

            tasks[task_type][task_id] = task
            self.tasks_index[task_id] = task

        for task_type in tasks:
            self.tasks[task_type] = self._sorted(tasks[task_type].values())

    @staticmethod
    def _load_task(blob: str) -> Task:
        log = json.loads(blob)
        if "task" in log:
            task = log["task"]
            for date_key in ("enqueued_at", "execute_at"):
                if task.get(date_key):
                    value = datetime.strptime(task[date_key], "%Y-%m-%dT%H:%M:%S")
                    task[date_key] = value
        return log
