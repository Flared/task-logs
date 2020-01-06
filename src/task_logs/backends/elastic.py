import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

from elasticsearch import Elasticsearch
from elasticsearch.serializer import JSONSerializer

from .backend import Log, ReaderBackend, WriterBackend

INDEX_PREFIX = "task-logs-"

TASK_LOGS_MAPPING = {
    "dynamic": "strict",
    "properties": {
        "timestamp": {"type": "date"},
        "task_id": {"type": "keyword"},
        "task_name": {"type": "keyword"},
        "type": {"type": "keyword"},
        "result": {"enabled": False, "type": "object"},
        "exception": {"type": "text"},
        "task": {
            "dynamic": "false",
            "properties": {
                "queue": {"type": "keyword"},
                "task_path": {"type": "keyword"},
                "execute_at": {"type": "date"},
                "args": {"enabled": False, "type": "object"},
                "kwargs": {"enabled": False, "type": "object"},
                "options": {"enabled": False, "type": "object"},
            },
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
        response = self.es.search(
            index=INDEX_PREFIX + "*",
            body={
                "query": {"query_string": {"query": query}},
                "sort": [{"timestamp": {"order": "desc"}}],
            },
        )

        return self._load_response(response)

    def find_task(self, task_id: str) -> List[Log]:
        response = self.es.search(
            index=INDEX_PREFIX + "*",
            body={
                "query": {"term": {"task_id": task_id}},
                "sort": [{"timestamp": {"order": "desc"}}],
            },
        )

        return self._load_response(response)

    def logs_by_type(self, type: Optional[str]) -> List[Log]:
        query: Dict[str, Any] = {"match_all": {}}
        if type is not None:
            query = {"term": {"type": type}}

        response = self.es.search(
            index=INDEX_PREFIX + "*",
            body={"query": query, "sort": [{"timestamp": {"order": "desc"}}]},
        )

        return self._load_response(response)

    @classmethod
    def _load_response(cls, response: Dict[str, Any]) -> List[Log]:
        logs: List[Log] = []
        for hit in response["hits"].get("hits", []):
            log = cls._load_datetimes(hit["_source"])
            logs.append(log)

        return logs

    @staticmethod
    def _load_datetimes(hit: Dict[str, Any]) -> Log:
        hit["timestamp"] = datetime.strptime(hit["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
        task = hit.get("task")
        if task and task.get("execute_at"):
            task["execute_at"] = datetime.strptime(
                task["execute_at"], "%Y-%m-%dT%H:%M:%S"
            )
        return cast(Log, hit)
