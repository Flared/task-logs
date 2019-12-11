import traceback
from datetime import datetime
from typing import IO, Any, Union, Iterable, List, Optional, Dict, overload, cast
from typing_extensions import Literal

from elasticsearch import Elasticsearch
from elasticsearch.serializer import JSONSerializer

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
        "exception": {"enabled": False, "type": "object"},
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


class JSONSerializerWithError(JSONSerializer):
    def default(self, data: Any) -> Any:
        if isinstance(data, BaseException):
            return "\n".join(
                traceback.format_exception(
                    etype=type(data), value=data, tb=data.__traceback__
                )
            )
        return super().default(data)


class ElasticsearchBackend(WriterBackend, ReaderBackend):
    def __init__(
        self,
        connections: Any,
        *,
        index_postfix: str = "%Y.%m.%d",
        force_refresh: bool = False,
        **options: Any
    ) -> None:
        self.es = Elasticsearch(
            connections, serializer=JSONSerializerWithError(), **options
        )
        self.index_postfix = index_postfix
        self.force_refresh = force_refresh

        self._init()

    def write(self, log: Log) -> None:
        index = INDEX_PREFIX + log["timestamp"].strftime(self.index_postfix)
        self.es.index(index=index, body=log, refresh=self.force_refresh)

    def _init(self) -> None:
        self.es.indices.put_template(name="task-logs-template", body=TASK_LOGS_TEMPLATE)

    def search(self, query: str) -> List[Log]:
        raise NotImplementedError

    def find_task(self, task_id: str) -> List[Log]:
        raise NotImplementedError

    def logs_by_type(self, type: str) -> List[Log]:
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
        hit["timestamp"] = datetime.strptime(hit["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
        task = hit.get("task")
        if task and task.get("execute_at"):
            task["execute_at"] = datetime.strptime(
                task["execute_at"], "%Y-%m-%dT%H:%M:%S"
            )
        return cast(Log, hit)
