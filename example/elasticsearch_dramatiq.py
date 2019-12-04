import dramatiq
from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker

from task_logs.backends.elastic import ElasticsearchWriteEngine
from task_logs.dramatiq import TaskLogsMiddleware

backend = ElasticsearchWriteBackend(["http://localhost:9200"])
stub_broker = StubBroker(middleware=[TaskLogsMiddleware(backend=backend)])
stub_broker.emit_after("process_boot")
dramatiq.set_broker(stub_broker)

@dramatiq.task
def some_task(a, *, b):
    return {"r": b}

some_task.send(1, b="hi")

worker = Worker(broker, worker_timeout=100)
worker.start()
broker.join("default")
worker.join()
worker.stop()
