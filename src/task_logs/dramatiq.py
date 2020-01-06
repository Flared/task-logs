from typing import Any, Optional, Set

from dramatiq import Broker, Message, Middleware

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

        actor = broker.get_actor(message.actor_name)
        task_path = actor.fn.__module__ + "." + actor.fn.__qualname__

        self.backend.write_enqueued(
            task_id=message.message_id,
            task_name=message.actor_name,
            task={
                "queue": message.queue_name,
                "task_path": task_path,
                "execute_at": None,
                "args": message.args,
                "kwargs": message.kwargs,
                "options": message.options,
            },
        )

    def before_process_message(self, broker: Broker, message: Message) -> None:
        if not self.should_log(broker, message):
            return

        self.backend.write_dequeued(
            task_id=message.message_id, task_name=message.actor_name
        )

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
            self.backend.write_completed(
                task_id=message.message_id, task_name=message.actor_name, result=result
            )
        else:
            self.backend.write_exception(
                task_id=message.message_id,
                task_name=message.actor_name,
                exception=exception,
            )

    def after_nack(self, broker: Broker, message: Message) -> None:
        if not self.should_log(broker, message):
            return

        self.backend.write_exception(
            task_id=message.message_id, task_name=message.actor_name, exception="Failed"
        )

    def should_log(self, broker: Broker, message: Message) -> bool:
        actor = broker.get_actor(message.actor_name)
        should_log: Optional[bool] = message.options.get("log")
        if should_log is not None:
            return should_log
        return actor.options.get("log") is not False
