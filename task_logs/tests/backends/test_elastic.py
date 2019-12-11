import pytest
from ..utils import fake_factory


def _ids(tasks):
    return [task["task_id"] for task in tasks]


def _types(tasks):
    return [task["type"] for task in tasks]


def test_elastic_backend(elastic_backend):
    fake_factory(elastic_backend)

    assert len(elastic_backend.enqueued()) == 3
    assert len(elastic_backend.dequeued()) == 4
    assert len(elastic_backend.completed()) == 2
    assert len(elastic_backend.exception()) == 2

    assert _ids(elastic_backend.search("timestamp:[2000-01-01T00:05:00Z TO *]")) == [
        "bbed01b8-226c-411e-9d0f-5e4fa4445bf7"
    ]

    assert (
        _ids(elastic_backend.search('"bbed01b8-226c-411e-9d0f-5e4fa4445bf7"'))
        == ["bbed01b8-226c-411e-9d0f-5e4fa4445bf7"] * 7
    )

    assert (
        _ids(elastic_backend.search("task_id:bbed01b8-226c-411e-9d0f-5e4fa4445bf7"))
        == ["bbed01b8-226c-411e-9d0f-5e4fa4445bf7"] * 7
    )

    assert (
        _ids(elastic_backend.search("ValueError"))
        == ["bbed01b8-226c-411e-9d0f-5e4fa4445bf7"] * 2
    )

    assert _ids(elastic_backend.search("test_queue")) == [
        "e308282a-5f6a-4553-a2c0-8612368ab917",
        "bbed01b8-226c-411e-9d0f-5e4fa4445bf7",
        "2fffe3e4-144d-40e1-9014-34a298c65bfc",
    ]

    assert _ids(elastic_backend.search("simple_task")) == [
        "e308282a-5f6a-4553-a2c0-8612368ab917",
        "2fffe3e4-144d-40e1-9014-34a298c65bfc",
    ]

    assert _ids(elastic_backend.search("type:dequeued")) == [
        "bbed01b8-226c-411e-9d0f-5e4fa4445bf7"
    ] * 3 + ["2fffe3e4-144d-40e1-9014-34a298c65bfc"]

    assert _types(
        elastic_backend.find_task("2fffe3e4-144d-40e1-9014-34a298c65bfc")
    ) == ["completed", "dequeued", "enqueued"]
