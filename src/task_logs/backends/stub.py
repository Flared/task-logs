import traceback
from typing import List, Optional, Union

from .backend import JobDetails, Log, ReaderBackend, Task, WriterBackend


class StubBackend(ReaderBackend, WriterBackend):
    def __init__(self) -> None:
        self.logs: List[Log] = []

    def write(self, log: Log) -> None:
        self.logs.insert(0, log)

    def write_enqueued(self, *, job_id: str, task_id: str, job: JobDetails) -> None:
        if job.args is not None:
            job.args = list(job.args)

        super().write_enqueued(job_id=job_id, task_id=task_id, job=job)

    def write_exception(
        self, *, job_id: str, task_id: str, exception: Union[BaseException, str]
    ) -> None:
        if isinstance(exception, BaseException):
            exception = "\n".join(
                traceback.format_exception(
                    etype=type(exception), value=exception, tb=exception.__traceback__
                )
            )
        return super().write_exception(
            job_id=job_id, task_id=task_id, exception=exception
        )

    def search(self, query: str) -> List[Log]:  # pragma: no cover
        return [l for l in self.logs if query in str(l)]

    def find_job(self, job_id: str) -> List[Log]:  # pragma: no cover
        return [l for l in self.logs if l.job_id == job_id]

    def logs_by_type(self, type: Optional[str]) -> List[Log]:  # pragma: no cover
        if type:
            return [l for l in self.logs if l.type == type]
        return self.logs

    def list_task(self) -> List[Task]:
        return list({Task(id=l.task_id) for l in self.logs})
