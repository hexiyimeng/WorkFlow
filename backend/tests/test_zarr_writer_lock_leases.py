from __future__ import annotations

import dask.config
import numpy as np

from nodes import zarr_writer_node


def _write_real_zarr_region(
    path: str,
    start: int,
    values: np.ndarray,
    lock_name: str,
) -> bool:
    import zarr

    target = zarr.open_array(path, mode="r+")
    zarr_writer_node._write_with_storage_chunk_locks(
        target=target,
        region=(slice(start, start + len(values)),),
        array=values,
        lock_names=(lock_name,),
    )
    return True


def _scheduler_lock_lease_timeout(dask_scheduler, lock_name: str):
    extension = dask_scheduler.extensions["semaphores"]
    return extension.lease_timeouts.get(lock_name, "missing")


class _Target:
    def __init__(self, events: list[object]):
        self.events = events
        self.value = None

    def __setitem__(self, region, array) -> None:
        self.events.append(("write", region))
        self.value = np.asarray(array).copy()


def test_zarr_correctness_lock_is_registered_without_a_finite_lease(
    monkeypatch,
) -> None:
    events: list[object] = []

    class _Lock:
        def __init__(self, name: str):
            self.name = name
            events.append(
                (
                    "construct",
                    name,
                    dask.config.get("distributed.scheduler.locks.lease-timeout"),
                )
            )

        def acquire(self, *, timeout):
            events.append(
                (
                    "acquire",
                    self.name,
                    dask.config.get("distributed.scheduler.locks.lease-timeout"),
                    timeout,
                )
            )
            return True

        def release(self):
            events.append(("release", self.name))
            return True

    monkeypatch.setattr(zarr_writer_node, "_make_distributed_lock", _Lock)
    target = _Target(events)
    array = np.arange(4, dtype=np.uint16)

    zarr_writer_node._write_with_storage_chunk_locks(
        target=target,
        region=(slice(3, 7),),
        array=array,
        lock_names=("chunk-3",),
    )

    assert events == [
        ("construct", "chunk-3", "inf"),
        (
            "acquire",
            "chunk-3",
            "inf",
            zarr_writer_node.DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECONDS,
        ),
        ("write", (slice(3, 7),)),
        ("release", "chunk-3"),
    ]
    np.testing.assert_array_equal(target.value, array)


def test_two_irregular_regions_share_the_same_partial_storage_chunk() -> None:
    shape = (1077,)
    storage_chunks = (256,)

    region_a = zarr_writer_node._partial_storage_chunk_coordinates(
        (768,),
        (1013,),
        shape,
        storage_chunks,
    )
    region_b = zarr_writer_node._partial_storage_chunk_coordinates(
        (1013,),
        (1077,),
        shape,
        storage_chunks,
    )

    assert region_a == ((3,),)
    assert region_b == ((3,),)
    assert zarr_writer_node._is_storage_chunk_aligned(
        (768,),
        (1024,),
        shape,
        storage_chunks,
    )


def test_partial_lock_acquisition_failure_releases_prior_locks_without_writing(
    monkeypatch,
) -> None:
    events: list[object] = []

    class _Lock:
        def __init__(self, name: str):
            self.name = name

        def acquire(self, *, timeout):
            events.append(("acquire", self.name, timeout))
            return self.name != "chunk-2"

        def release(self):
            events.append(("release", self.name))
            return True

    monkeypatch.setattr(zarr_writer_node, "_make_distributed_lock", _Lock)
    target = _Target(events)

    try:
        zarr_writer_node._write_with_storage_chunk_locks(
            target=target,
            region=(slice(1, 3),),
            array=np.ones(2, dtype=np.uint8),
            lock_names=("chunk-1", "chunk-2"),
        )
    except RuntimeError as exc:
        assert "without performing an unsafe concurrent write" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("The timed-out lock acquisition should fail the write")

    assert events == [
        (
            "acquire",
            "chunk-1",
            zarr_writer_node.DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECONDS,
        ),
        (
            "acquire",
            "chunk-2",
            zarr_writer_node.DEFAULT_LOCK_ACQUIRE_TIMEOUT_SECONDS,
        ),
        ("release", "chunk-1"),
    ]
    assert target.value is None


def test_real_concurrent_partial_compressed_chunk_writes_preserve_both_regions(
    tmp_path,
) -> None:
    import numcodecs
    import zarr
    from dask.distributed import Client, LocalCluster

    path = tmp_path / "concurrent.zarr"
    target = zarr.open_array(
        str(path),
        mode="w",
        shape=(1077,),
        chunks=(256,),
        dtype="uint16",
        compressor=numcodecs.Zstd(level=1),
    )
    target[:] = 0
    namespace = zarr_writer_node._zarr_lock_namespace(
        str(path),
        "array",
        "0",
    )
    shared_lock_name = zarr_writer_node._zarr_chunk_lock_name(namespace, (3,))

    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=1,
        processes=False,
        protocol="tcp",
        host="127.0.0.1",
        dashboard_address=None,
    )
    client = Client(cluster)
    try:
        values_a = np.full(245, 11, dtype=np.uint16)
        values_b = np.full(64, 22, dtype=np.uint16)
        futures = [
            client.submit(
                _write_real_zarr_region,
                str(path),
                768,
                values_a,
                shared_lock_name,
                pure=False,
            ),
            client.submit(
                _write_real_zarr_region,
                str(path),
                1013,
                values_b,
                shared_lock_name,
                pure=False,
            ),
        ]
        assert client.gather(futures) == [True, True]
        lease_timeout = client.run_on_scheduler(
            _scheduler_lock_lease_timeout,
            lock_name=shared_lock_name,
        )
    finally:
        client.close()
        cluster.close()

    result = np.asarray(zarr.open_array(str(path), mode="r")[:])
    np.testing.assert_array_equal(result[768:1013], values_a)
    np.testing.assert_array_equal(result[1013:1077], values_b)
    assert lease_timeout is None
