from freezegun import freeze_time


@freeze_time("2000-01-01", auto_tick_seconds=10)
def fake_factory(backend):
    backend.write_enqueued(
        task_id="2fffe3e4-144d-40e1-9014-34a298c65bfc",
        task_name="simple_task",
        task={
            "queue": "test_queue",
            "task_path": "task_logs.tests.simple_task",
            "execute_at": None,
            "args": ["a"],
            "kwargs": {"b": 1},
            "options": {"time_limit": 10000},
        },
    )

    backend.write_dequeued(
        task_id="2fffe3e4-144d-40e1-9014-34a298c65bfc", task_name="simple_task"
    )
    backend.write_completed(
        task_id="2fffe3e4-144d-40e1-9014-34a298c65bfc",
        task_name="simple_task",
        result="done!",
    )

    backend.write_enqueued(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7",
        task_name="other_task",
        task={
            "queue": "test_queue",
            "task_path": "task_logs.tests.other_task",
            "execute_at": "2019-01-01T01:01:01",
            "args": ["a"],
            "kwargs": {"b": 2},
            "options": {},
        },
    )

    backend.write_enqueued(
        task_id="e308282a-5f6a-4553-a2c0-8612368ab917",
        task_name="simple_task",
        task={
            "queue": "test_queue",
            "task_path": "task_logs.tests.simple_task",
            "execute_at": None,
            "args": ["a"],
            "kwargs": {"b": 2},
            "options": {},
        },
    )

    backend.write_dequeued(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7", task_name="other_task"
    )
    backend.write_exception(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7",
        task_name="other_task",
        exception="ValueError",
    )

    backend.write_dequeued(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7", task_name="other_task"
    )
    backend.write_exception(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7",
        task_name="other_task",
        exception="ValueError",
    )

    backend.write_dequeued(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7", task_name="other_task"
    )
    backend.write_completed(
        task_id="bbed01b8-226c-411e-9d0f-5e4fa4445bf7",
        task_name="other_task",
        result="done!",
    )