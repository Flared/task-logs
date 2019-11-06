import tempfile
from datetime import datetime

import pytest
import dramatiq
from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker
from freezegun import freeze_time

from task_logs.engines.file import FileWriteEngine, FileReadEngine
from task_logs.dramatiq import TaskLogsMiddleware


@pytest.fixture()
def engine():
    with tempfile.NamedTemporaryFile(mode="w+") as fd:
        yield FileReadEngine(logfile=fd), FileWriteEngine(logfile=fd)


@pytest.fixture()
def broker(engine):
    rengine, wengine = engine
    stub_broker = StubBroker(middleware=[TaskLogsMiddleware(engine=wengine)])
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


def test_task_logs(broker, worker, engine, frozen_time):
    rengine, wengine = engine

    @dramatiq.actor(queue_name="test")
    def simple_task(a, b):
        return "hello"

    message = simple_task.send("a", b="b")

    rengine._reload()
    assert rengine.enqueued() == [
        {
            "queue": "test",
            "task_id": message.message_id,
            "task_name": "simple_task",
            "task_path": None,
            "enqueued_at": datetime.now(),
            "execute_at": None,
            "args": ["a"],
            "kwargs": {"b": "b"},
            "options": {},
        }
    ]

    worker.start()
    broker.join(simple_task.queue_name)
    worker.join()
