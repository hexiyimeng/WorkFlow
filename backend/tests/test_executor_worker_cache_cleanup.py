from __future__ import annotations

import asyncio
import time

import pytest

from services.executor import _clear_worker_caches_with_timeout


class _FakeFuture:
    def __init__(self, value, *, done: bool = True):
        self.value = value
        self.is_done = done
        self.released = False

    def done(self) -> bool:
        return self.is_done

    def result(self, *, timeout):
        assert timeout == 0
        return self.value

    def release(self) -> None:
        self.released = True


class _FakeClient:
    def __init__(self, *, block_gather: bool = False, sync_delay: float = 0):
        self.block_gather = block_gather
        self.sync_delay = sync_delay
        self.submissions: list[dict] = []
        self.futures: list[_FakeFuture] = []
        self.cancelled: list[_FakeFuture] = []

    class _Scheduler:
        def identity(self, *, n_workers):
            assert n_workers == -1
            return {
                "workers": {
                    "tcp://127.0.0.1:2": {},
                    "tcp://127.0.0.1:1": {},
                }
            }

    scheduler = _Scheduler()

    def sync(self, function, **kwargs):
        callback_timeout = kwargs.pop("callback_timeout", None)
        assert callback_timeout is not None and callback_timeout > 0
        if self.sync_delay:
            time.sleep(self.sync_delay)
        return function(**kwargs)

    def submit(self, function, **kwargs):
        assert callable(function)
        self.submissions.append(kwargs)
        future = _FakeFuture(
            {"cleared": kwargs["workers"][0]},
            done=not self.block_gather,
        )
        self.futures.append(future)
        return future

def test_worker_cache_cleanup_uses_one_cancellable_task_per_worker() -> None:
    client = _FakeClient()

    result = asyncio.run(
        _clear_worker_caches_with_timeout(client, timeout_seconds=1.0)
    )

    assert tuple(result) == (
        "tcp://127.0.0.1:1",
        "tcp://127.0.0.1:2",
    )
    assert [submission["workers"] for submission in client.submissions] == [
        ["tcp://127.0.0.1:1"],
        ["tcp://127.0.0.1:2"],
    ]
    assert all(submission["allow_other_workers"] is False for submission in client.submissions)
    assert all(future.released for future in client.futures)
    assert client.cancelled == []


def test_timed_out_worker_cache_tasks_are_released_without_a_blocking_cancel() -> None:
    client = _FakeClient(block_gather=True)

    with pytest.raises(TimeoutError):
        asyncio.run(
            _clear_worker_caches_with_timeout(client, timeout_seconds=0.01)
        )

    assert client.cancelled == []
    assert all(future.released for future in client.futures)


def test_scheduler_query_and_cache_tasks_share_one_timeout_budget() -> None:
    client = _FakeClient(block_gather=True, sync_delay=0.04)
    started = time.monotonic()

    with pytest.raises(TimeoutError):
        asyncio.run(
            _clear_worker_caches_with_timeout(client, timeout_seconds=0.05)
        )

    assert time.monotonic() - started < 0.09
    assert all(future.released for future in client.futures)
