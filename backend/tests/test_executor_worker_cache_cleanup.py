from __future__ import annotations

import asyncio

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
    def __init__(self, *, block_gather: bool = False):
        self.block_gather = block_gather
        self.submissions: list[dict] = []
        self.futures: list[_FakeFuture] = []
        self.cancelled: list[_FakeFuture] = []

    def scheduler_info(self, *, n_workers):
        assert n_workers == -1
        return {"workers": {"tcp://127.0.0.1:2": {}, "tcp://127.0.0.1:1": {}}}

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
