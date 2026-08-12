import asyncio
import logging

from core.state_manager import state_manager


def test_sync_and_async_observers_receive_events_without_websocket_subscribers():
    sync_events = []
    async_events = []

    def sync_observer(execution_id, payload):
        sync_events.append((execution_id, payload))

    async def async_observer(execution_id, payload):
        await asyncio.sleep(0)
        async_events.append((execution_id, payload))

    sync_id = state_manager.register_broadcast_observer(sync_observer)
    async_id = state_manager.register_broadcast_observer(
        async_observer,
        execution_id="runner-1",
    )
    try:
        asyncio.run(state_manager.broadcast("runner-1", {"type": "progress"}))
    finally:
        state_manager.remove_broadcast_observer(sync_id)
        state_manager.remove_broadcast_observer(async_id)

    assert sync_events == [(
        "runner-1",
        {"type": "progress", "executionId": "runner-1"},
    )]
    assert async_events == sync_events


def test_observer_failure_is_logged_and_does_not_block_other_observers(caplog):
    received = []

    def broken_observer(_execution_id, _payload):
        raise RuntimeError("spool is unavailable")

    def healthy_observer(execution_id, payload):
        received.append((execution_id, payload["type"]))

    broken_id = state_manager.register_broadcast_observer(broken_observer)
    healthy_id = state_manager.register_broadcast_observer(healthy_observer)
    try:
        with caplog.at_level(logging.ERROR, logger="WorkFlow.State"):
            asyncio.run(state_manager.broadcast("runner-2", {"type": "log"}))
    finally:
        state_manager.remove_broadcast_observer(broken_id)
        state_manager.remove_broadcast_observer(healthy_id)

    assert received == [("runner-2", "log")]
    assert "Broadcast observer" in caplog.text


def test_removed_observer_receives_no_more_events():
    received = []
    observer_id = state_manager.register_broadcast_observer(
        lambda execution_id, payload: received.append((execution_id, payload))
    )
    assert state_manager.remove_broadcast_observer(observer_id) is True
    assert state_manager.remove_broadcast_observer(observer_id) is False

    asyncio.run(state_manager.broadcast("runner-3", {"type": "done"}))
    assert received == []
