from typing import Optional, Set, Any
from datetime import datetime

from dramatiq import Middleware, Broker, Message

from .engines.engine import WriteEngine


class TaskLogsMiddleware(Middleware):
    def __init__(self, engine: WriteEngine):
        self.engine = engine

    @property
    def actor_options(self) -> Set[str]:
        return {"log"}

    def after_enqueue(self, broker: Broker, message: Message, delay: float) -> None:
        self.engine.log_enqueued(
            {
                "queue": message.queue_name,
                "task_id": message.message_id,
                "task_name": message.actor_name,
                "task_path": None,
                "enqueued_at": datetime.now(),
                "execute_at": None,
                "args": message.args,
                "kwargs": message.kwargs,
                "options": message.options,
            }
        )
        print("after_enqueue")

    def before_process_message(self, broker: Broker, message: Message) -> None:
        print("before_process_message")

    def after_process_message(
        self,
        broker: Broker,
        message: Message,
        *,
        result: Any = None,
        exception: Optional[BaseException] = None
    ) -> None:
        print("after_process_message")

    def after_nack(self, broker: Broker, message: Message) -> None:
        print("after_nack")
