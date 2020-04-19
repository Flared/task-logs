from datetime import datetime

import dramatiq
import pytest
from dramatiq import Middleware, Worker
from dramatiq.brokers.stub import StubBroker
from freezegun import freeze_time

from task_logs.backends.backend import (
    CompletedLog,
    DequeuedLog,
    EnqueuedLog,
    ExceptionLog,
    JobDetails,
    LogType,
)
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


def test_dramatiq_completion(broker, worker, backend, frozen_time):
    @dramatiq.actor(queue_name="test")
    def simple_task(a, b):
        return "hello"

    message = simple_task.send("a", b="b")

    assert backend.enqueued() == [
        EnqueuedLog(
            type=LogType.ENQUEUED,
            timestamp=datetime.now(),
            job_id=message.message_id,
            task_id="simple_task",
            job=JobDetails(
                queue="test",
                task_path=(
                    "tests.test_dramatiq.test_dramatiq_completion.<locals>"
                    ".simple_task"
                ),
                execute_at=None,
                args=["a"],
                kwargs={"b": "b"},
                options={},
            ),
        )
    ]

    assert backend.dequeued() == []
    assert backend.completed() == []

    worker.start()
    broker.join(simple_task.queue_name)
    worker.join()

    assert backend.dequeued() == [
        DequeuedLog(
            job_id=message.message_id,
            task_id="simple_task",
            timestamp=datetime.now(),
            type=LogType.DEQUEUED,
        )
    ]
    assert backend.completed() == [
        CompletedLog(
            job_id=message.message_id,
            task_id="simple_task",
            timestamp=datetime.now(),
            result="hello",
            type=LogType.COMPLETED,
        )
    ]


def test_dramatiq_failed(broker, worker, backend, frozen_time):
    class FailMessage(Middleware):
        def after_process_message(
            self, broker, message, *, result=None, exception=None
        ):
            message.fail()

    @dramatiq.actor(queue_name="test")
    def simple_task_failed():
        return

    broker.add_middleware(FailMessage())

    message = simple_task_failed.send()
    simple_task_failed.send_with_options(log=False)

    assert backend.enqueued() == [
        EnqueuedLog(
            type=LogType.ENQUEUED,
            timestamp=datetime.now(),
            job_id=message.message_id,
            task_id="simple_task_failed",
            job=JobDetails(
                queue="test",
                task_path="tests.test_dramatiq.test_dramatiq_failed.<locals>"
                ".simple_task_failed",
                execute_at=None,
                args=[],
                kwargs={},
                options={},
            ),
        )
    ]

    assert backend.dequeued() == []
    assert backend.completed() == []

    worker.start()
    broker.join(simple_task_failed.queue_name)
    worker.join()

    assert backend.dequeued() == [
        DequeuedLog(
            job_id=message.message_id,
            task_id="simple_task_failed",
            timestamp=datetime.now(),
            type=LogType.DEQUEUED,
        )
    ]
    exceptions = backend.exception()
    for exception in exceptions:
        assert "Failed" in exception.exception
        exception.exception = ""
    assert exceptions == [
        ExceptionLog(
            job_id=message.message_id,
            task_id="simple_task_failed",
            timestamp=datetime.now(),
            type=LogType.EXCEPTION,
            exception="",
        )
    ]


def test_dramatiq_error(broker, worker, backend, frozen_time):
    @dramatiq.actor(queue_name="test")
    def simple_task_error():
        raise ValueError("Expected")

    message = simple_task_error.send_with_options(time_limit=10000)

    assert backend.enqueued() == [
        EnqueuedLog(
            type=LogType.ENQUEUED,
            timestamp=datetime.now(),
            job_id=message.message_id,
            task_id="simple_task_error",
            job=JobDetails(
                queue="test",
                task_path="tests.test_dramatiq.test_dramatiq_error.<locals>"
                ".simple_task_error",
                execute_at=None,
                args=[],
                kwargs={},
                options={"time_limit": 10000},
            ),
        )
    ]

    assert backend.dequeued() == []
    assert backend.completed() == []

    worker.start()
    broker.join(simple_task_error.queue_name)
    worker.join()

    assert backend.dequeued() == [
        DequeuedLog(
            job_id=message.message_id,
            task_id="simple_task_error",
            timestamp=datetime.now(),
            type=LogType.DEQUEUED,
        )
    ]
    exceptions = backend.exception()
    for exception in exceptions:
        assert 'ValueError("Expected")' in exception.exception
        exception.exception = ""
    assert exceptions == [
        ExceptionLog(
            job_id=message.message_id,
            task_id="simple_task_error",
            timestamp=datetime.now(),
            type=LogType.EXCEPTION,
            exception="",
        )
    ]


@pytest.mark.parametrize(
    "actor_log,task_log,log_expected",
    [
        (None, None, True),
        (False, None, False),
        (True, None, True),
        (None, False, False),
        (False, False, False),
        (True, False, False),
        (None, True, True),
        (False, True, True),
        (True, True, True),
    ],
)
def test_disabled_log(broker, worker, backend, actor_log, task_log, log_expected):
    @dramatiq.actor(queue_name="test", log=actor_log)
    def simple_task_with_log_option():
        pass

    simple_task_with_log_option.send_with_options(log=task_log)

    worker.start()
    broker.join(simple_task_with_log_option.queue_name)
    worker.join()

    expected = 1 if log_expected else 0
    assert len(backend.dequeued()) == expected
    assert len(backend.completed()) == expected
