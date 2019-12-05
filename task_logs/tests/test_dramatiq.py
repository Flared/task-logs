import tempfile
from datetime import datetime

import pytest
import dramatiq
from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker
from freezegun import freeze_time

from task_logs.dramatiq import TaskLogsMiddleware


@pytest.fixture()
def broker(backend):
    stub_broker = StubBroker(middleware=[TaskLogsMiddleware(backend=backend)])
    stub_broker.emit_after("process_boot")
    dramatiq.set_broker(stub_broker)
    return stub_broker


@pytest.fixture()
def worker(broker):
    worker = Worker(broker, worker_timeout=100)
    yield worker
    worker.stop()


@pytest.fixture()
def frozen_time():
    with freeze_time("2019-01-14T12:45:23"):
        yield


def test_task_logs(broker, worker, backend, frozen_time):
    @dramatiq.actor(queue_name="test")
    def simple_task(a, b):
        return "hello"

    message = simple_task.send("a", b="b")

    assert backend.enqueued() == [
        {
            "type": "enqueued",
            "timestamp": datetime.now(),
            "task_id": message.message_id,
            "task": {
                "queue": "test",
                "task_id": message.message_id,
                "task_name": "simple_task",
                "task_path": None,
                "execute_at": None,
                "args": ["a"],
                "kwargs": {"b": "b"},
                "options": {},
            },
        }
    ]

    assert backend.dequeued() == []
    assert backend.completed() == []

    worker.start()
    broker.join(simple_task.queue_name)
    worker.join()

    assert backend.dequeued() == [
        {
            "task_id": message.message_id,
            "timestamp": datetime.now(),
            "type": "dequeued",
        }
    ]
    assert backend.completed() == [
        {
            "task_id": message.message_id,
            "timestamp": datetime.now(),
            "result": "hello",
            "type": "completed",
        }
    ]
