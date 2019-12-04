import json
from datetime import datetime
from typing import IO, Any, Union, Iterable, List, Optional, Dict, overload, cast
from typing_extensions import Literal

from elasticsearch import Elasticsearch

from .backend import (
    WriterBackend,
    ReaderBackend,
    Log,
    EnqueuedLog,
    DequeuedLog,
    CompletedLog,
    ExceptionLog,
    FailedLog,
)

INDEX_PREFIX = "task-logs-"

TASK_LOGS_MAPPING = {
    "dynamic": "strict",
    "properties": {
        "timestamp": {"type": "date"},
        "task_id": {"type": "keyword"},
        "type": {"type": "keyword"},
        "result": {"enabled": False, "type": "object"},
        "task": {
            "properties": {
                "queue": {"type": "keyword"},
                "task_id": {"type": "keyword"},
                "task_name": {"type": "keyword"},
                "task_path": {"type": "keyword"},
                "execute_at": {"type": "date"},
                "args": {"type": "keyword"},
                "kwargs": {"dynamic": True, "enabled": False, "properties": {}},
                "options": {"dynamic": True, "enabled": False, "properties": {}},
            }
        },
    },
}

TASK_LOGS_TEMPLATE = {
    "index_patterns": [INDEX_PREFIX + "*"],
    "mappings": TASK_LOGS_MAPPING,
    "version": 1,
}


class ElasticsearchBackend(WriterBackend, ReaderBackend):
    def __init__(
        self,
        connections: Any,
        *,
        index_postfix: str = "%Y.%m.%d",
        force_refresh: bool = False,
        **options: Any
    ) -> None:
        self.es = Elasticsearch(connections, **options)
        self.index_postfix = index_postfix
        self.force_refresh = force_refresh

        self._init()

    def write(self, log: Log) -> None:
        index = INDEX_PREFIX + log["timestamp"].strftime(self.index_postfix)
        self.es.index(index=index, body=log, refresh=self.force_refresh)

    def _init(self) -> None:
        self.es.indices.put_template(name="task-logs-template", body=TASK_LOGS_TEMPLATE)

    def enqueued(self) -> List[EnqueuedLog]:
        return cast(List[EnqueuedLog], self._search_by_type("enqueued"))

    def dequeued(self) -> List[DequeuedLog]:
        return self._search_by_type("dequeued")

    def completed(self) -> List[CompletedLog]:
        return cast(List[CompletedLog], self._search_by_type("completed"))

    def exception(self) -> List[ExceptionLog]:
        return cast(List[ExceptionLog], self._search_by_type("exception"))

    def failed(self) -> List[FailedLog]:
        return self._search_by_type("failed")

    def search(self, query: str) -> List[Log]:
        raise NotImplementedError

    def find_task(self, task_id: str) -> List[Log]:
        raise NotImplementedError

    def _search_by_type(self, type: str) -> List[Log]:
        response = self.es.search(
            index=INDEX_PREFIX + "*",
            body={
                "query": {"term": {"type": type}},
                "sort": [{"timestamp": {"order": "desc"}}],
            },
        )

        logs: List[Log] = []
        for hit in response["hits"].get("hits", []):
            log = self._load_datetimes(hit["_source"])
            logs.append(log)

        return logs

    @staticmethod
    def _load_datetimes(hit: Dict) -> Log:
        hit["timestamp"] = datetime.strptime(hit["timestamp"], "%Y-%m-%dT%H:%M:%S")
        task = hit.get("task")
        if task and task.get("execute_at"):
            task["execute_at"] = datetime.strptime(
                task["execute_at"], "%Y-%m-%dT%H:%M:%S"
            )
        return cast(Log, hit)
