from typing import Any, List

import pytest

from task_logs.backends import ElasticsearchBackend, StubBackend

from .config import CI, ELASTICSEARCH_URL


def check_elastic(connections: List[str]) -> None:
    try:
        from elasticsearch import Elasticsearch

        es = Elasticsearch(connections)
        es.cluster.health()
    except Exception:
        if CI:
            raise
        raise pytest.skip("No connection to Elasticsearch server.")
    else:
        es.indices.delete("task-logs-*")


@pytest.fixture
def elastic_backend() -> ElasticsearchBackend:
    return _elastic_backend()


def _elastic_backend() -> ElasticsearchBackend:
    connections = [ELASTICSEARCH_URL]
    check_elastic(connections)
    return ElasticsearchBackend(connections, force_refresh=True)


def stub_backend() -> StubBackend:
    return StubBackend()


@pytest.fixture
def backends() -> Any:
    return {
        "elastic": _elastic_backend,
        "stub": stub_backend,
    }


@pytest.fixture(params=["elastic", "stub"])
def backend(request: Any, backends: Any) -> Any:
    return backends[request.param]()
