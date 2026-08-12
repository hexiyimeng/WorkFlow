import asyncio
from types import SimpleNamespace

import main
from core.state_manager import ExecutionStatus


class _RuntimeState:
    def __init__(self, task):
        self.active_execution_id = "execution-1"
        self.current_task = task
        self.session = SimpleNamespace(status=ExecutionStatus.RUNNING)
        self.cancelled = []
        self.cleared = []

    def get_execution(self, execution_id):
        assert execution_id == self.active_execution_id
        return self.session

    def cancel_execution(self, execution_id):
        self.cancelled.append(execution_id)
        self.session.status = ExecutionStatus.CANCELLING
        self.current_task.cancel()
        return True

    def clear_active_execution(self, execution_id):
        self.cleared.append(execution_id)
        self.active_execution_id = None
        self.current_task = None


def test_slurm_shutdown_detaches_monitor_without_user_cancel_or_local_dask(
    monkeypatch,
):
    async def scenario():
        monitor_started = asyncio.Event()

        async def monitor():
            monitor_started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(monitor())
        await monitor_started.wait()
        runtime_state = _RuntimeState(task)
        stop_calls = []

        monkeypatch.setattr(main, "state_manager", runtime_state)
        monkeypatch.setattr(main, "uses_slurm_execution_backend", lambda: True)
        detach_calls = []
        monkeypatch.setattr(
            main,
            "detach_execution_backend",
            lambda execution_id: detach_calls.append(execution_id),
        )
        monkeypatch.setattr(
            main.dask_service,
            "stop_cluster",
            lambda: stop_calls.append(True),
        )

        await main._shutdown_execution_runtime()

        assert task.cancelled()
        assert runtime_state.cancelled == []
        assert detach_calls == ["execution-1"]
        assert runtime_state.cleared == ["execution-1"]
        assert runtime_state.session.status == ExecutionStatus.RUNNING
        assert stop_calls == []

    asyncio.run(scenario())


def test_local_shutdown_cancels_execution_before_stopping_dask(monkeypatch):
    async def scenario():
        execution_started = asyncio.Event()

        async def execution():
            execution_started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(execution())
        await execution_started.wait()
        runtime_state = _RuntimeState(task)
        stop_calls = []

        monkeypatch.setattr(main, "state_manager", runtime_state)
        monkeypatch.setattr(main, "uses_slurm_execution_backend", lambda: False)
        monkeypatch.setattr(
            main.dask_service,
            "stop_cluster",
            lambda: stop_calls.append(True),
        )

        await main._shutdown_execution_runtime()

        assert task.cancelled()
        assert runtime_state.cancelled == ["execution-1"]
        assert runtime_state.cleared == []
        assert runtime_state.session.status == ExecutionStatus.CANCELLING
        assert stop_calls == [True]

    asyncio.run(scenario())
