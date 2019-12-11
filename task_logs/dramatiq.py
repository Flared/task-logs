from typing import Optional, Set, Any
from datetime import datetime

from dramatiq import Middleware, Broker, Message

from .backends.backend import WriterBackend


class TaskLogsMiddleware(Middleware):
    def __init__(self, backend: WriterBackend):
        self.backend = backend

    @property
    def actor_options(self) -> Set[str]:
        return {"log"}

    def after_enqueue(self, broker: Broker, message: Message, delay: float) -> None:
        if not self.should_log(broker, message):
            return

        self.backend.write_enqueued(
            {
                "queue": message.queue_name,
                "task_id": message.message_id,
                "task_name": message.actor_name,
                "task_path": None,
                "execute_at": None,
                "args": message.args,
                "kwargs": message.kwargs,
                "options": message.options,
            }
        )

    def before_process_message(self, broker: Broker, message: Message) -> None:
        if not self.should_log(broker, message):
            return

        self.backend.write_dequeued(message.message_id)

    def after_process_message(
        self,
        broker: Broker,
        message: Message,
        *,
        result: Any = None,
        exception: Optional[BaseException] = None
    ) -> None:
        if not self.should_log(broker, message):
            return

        if exception is None:
            self.backend.write_completed(message.message_id, result=result)
        else:
            self.backend.write_exception(message.message_id, exception=exception)

    def after_nack(self, broker: Broker, message: Message) -> None:
        if not self.should_log(broker, message):
            return

        self.backend.write_exception(message.message_id, exception="Failed")

    def should_log(self, broker: Broker, message: Message) -> bool:
        actor = broker.get_actor(message.actor_name)
        should_log = message.options.get("log")
        if should_log is not None:
            return should_log
        return actor.options.get("log") != False
