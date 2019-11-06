import json
from datetime import datetime
from typing import IO, Any, Union, Iterable, List, Optional, Dict

from elasticsearch import Elasticsearch

from .engine import WriteEngine, ReadEngine, Task

TASK_LOGS_MAPPING = {
    "dynamic": "strict",
    "properties": {
        "@timestamp": {"type": "date"},
        "type":  {"type": "keyword"},
        "task": {
            "queue": {"type": "keyword"},
            "task_id": {"type": "keyword"},
            "task_name": {"type": "keyword"},
            "task_path": {"type": "keyword"},
            "scheduled_at": {"type": "date"},

            "args": {"type": "keyword"},
            "kwargs": {
                "dynamic": True
            },
            "options": {
                "dynamic": True
            },
        }
    },
    "dynamic_templates": [{
        "kwargs": {
            "match_mapping_type": "string",
            "path_match":   "kwargs.*",
            "mapping": {
                "type": "keyword"
            }
        },
        "options": {
            "match_mapping_type": "string",
            "path_match":   "options.*",
            "mapping": {
                "type": "keyword"
            }
        },
    }]
}

TASK_LOGS_TEMPLATE = {
    "index_patterns": ["task-logs-*"],
    "mappings": TASK_LOGS_MAPPING,
    "version": 1
}

class ElasticsearchWriteEngine(WriteEngine):
    def __init__(self, connections):
        self.es = Elasticsearch(
                connections,
                sniff_on_start=True,
                sniff_on_connection_fail=True,
                sniffer_timeout=60)

        self._init()

    def log_enqueued(self, task: Task) -> None:
        self.es.index(
            index="task-logs",
            id=task.task_id,
            body={
                "@timestamp": datetime.now(),
                "type": "enqueued",
                "task": task
            })

    def _init(self):
        self.es.put_template(
            name="task-logs-template",
            body=TASK_LOGS_TEMPLATE)
