import pytest
from .config import ELASTICSEARCH_URL, CI
from task_logs.backends import ElasticsearchBackend, StubBackend


def check_elastic(connections):
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
def elastic_backend():
    connections = [ELASTICSEARCH_URL]
    check_elastic(connections)
    return ElasticsearchBackend(connections, force_refresh=True)


def stub_backend():
    return StubBackend()


@pytest.fixture
def backends():
    return {
        "elastic": elastic_backend,
        "stub": stub_backend,
    }


@pytest.fixture(params=["elastic", "stub"])
def backend(request, backends):
    return backends[request.param]()

