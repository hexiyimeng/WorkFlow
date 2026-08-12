from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import uuid
import zipfile
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import zarr
from PIL import Image


IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".tif", ".tiff"}


def _select_demo_member(archive: zipfile.ZipFile, requested: str | None) -> str:
    members = [
        name
        for name in archive.namelist()
        if Path(name).suffix.lower() in IMAGE_SUFFIXES
        and not any(
            marker in Path(name).stem.lower()
            for marker in ("mask", "flow", "seg", "label", "outline")
        )
    ]
    if requested:
        if requested not in members:
            raise ValueError(f"Requested archive member is unavailable: {requested}")
        return requested
    if not members:
        raise ValueError("The Cellpose demo archive contains no usable source image.")
    return sorted(members)[0]


def _prepare_input(
    archive_path: Path,
    input_path: Path,
    *,
    member: str | None,
    max_side: int,
) -> dict[str, object]:
    archive_sha256 = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    with zipfile.ZipFile(archive_path) as archive:
        selected = _select_demo_member(archive, member)
        with archive.open(selected) as handle:
            image = np.asarray(Image.open(io.BytesIO(handle.read())))

    if image.ndim == 3:
        # Preserve the source intensity range while reducing RGB/multi-channel
        # demo images to the channel-less YX input expected by this smoke DAG.
        image = image.max(axis=-1)
    image = np.squeeze(image)
    if image.ndim != 2:
        raise ValueError(f"Expected a 2D source image, got shape {image.shape}.")
    if min(image.shape) < 128:
        raise ValueError(f"Source image is too small for the overlap test: {image.shape}.")

    target_y = min(int(image.shape[0]), max_side)
    target_x = min(int(image.shape[1]), max_side)
    start_y = (int(image.shape[0]) - target_y) // 2
    start_x = (int(image.shape[1]) - target_x) // 2
    image = np.ascontiguousarray(
        image[start_y : start_y + target_y, start_x : start_x + target_x]
    )
    chunks = (min(256, target_y), min(256, target_x))
    target = zarr.open_array(
        str(input_path),
        mode="w",
        shape=image.shape,
        chunks=chunks,
        dtype=image.dtype,
    )
    target[:] = image
    return {
        "sourceArchive": str(archive_path),
        "sourceArchiveSha256": archive_sha256,
        "sourceMember": selected,
        "sourceShape": [int(value) for value in image.shape],
        "sourceDtype": str(image.dtype),
        "chunks": list(chunks),
        "cropOrigin": [start_y, start_x],
    }


def _build_graph(
    *,
    input_path: Path,
    mask_path: Path,
    cell_table_path: Path,
) -> dict[str, object]:
    return {
        "reader": {
            "type": "OMEZarrReader",
            "inputs": {
                "file_path": str(input_path),
                "array_path": "",
                "axes": "Y,X",
                "chunk_mode": "by_axes",
                "chunk_y": 256,
                "chunk_x": 256,
                "chunk_z": 1,
                "keep_first_dim": False,
                "multiscale_index": 0,
                "scale_level": 0,
            },
        },
        "cellpose": {
            "type": "Cellpose",
            "inputs": {
                "image": ["reader", 0],
                "primary_channel": 0,
                "secondary_channel": -1,
                "model_name": "cpsam",
                "diameter": 0,
                "flow_threshold": 0.4,
                "cellprob_threshold": 0,
                "gpu_batch_size": 4,
                "do_3d": "false",
                "normalize": True,
            },
        },
        "zarr_writer": {
            "type": "ZarrWriter",
            "inputs": {
                "array": ["cellpose", 0],
                "output_path": str(mask_path),
                "store_kind": "array",
                "dataset_path": "0",
                "axes": "Y,X",
                "voxel_size": "1,1",
                "compressor_name": "default",
                "overwrite": True,
                "write_metadata": True,
            },
        },
        "parquet_writer": {
            "type": "WriteParquetCellTable",
            "inputs": {
                "mask": ["cellpose", 0],
                "output_dir": str(cell_table_path),
                "axes": "Y,X",
                "tile_z": 1,
                "tile_y": 256,
                "tile_x": 256,
                "compression": "zstd",
                "row_group_size": 100000,
                "overwrite": True,
                "write_block_metadata": True,
                "sort_by_spatial_key": True,
            },
        },
    }


async def _run(graph: dict[str, object], recovery_path: Path, execution_id: str) -> str:
    from core.state_manager import state_manager
    from services.dask_service import dask_service
    from services.executor import execute_graph
    from services.plugin_loader import load_all_plugins

    success, _loaded, failed = load_all_plugins()
    if not success:
        raise RuntimeError(f"Plugin loading failed: {failed}")
    config = {
        "mode": "window",
        # Terminal Writers expose one token element per source Dask block.
        # A 1x1 token Window therefore executes one source block and produces
        # four recoverable Windows for the default 512x512 / 256x256 fixture.
        "windowShape": [1, 1],
        "maxInFlightWindows": 1,
        "resumeAction": "new",
        "recoveryLocation": {
            "mode": "custom",
            "directory": str(recovery_path),
        },
    }
    try:
        await execute_graph(graph, execution_id, config)
        session = state_manager.get_execution(execution_id)
        return session.status if session is not None else "missing"
    finally:
        await asyncio.to_thread(dask_service.stop_cluster)


def _validate_run(run_dir: Path, expected_shape: tuple[int, int]) -> dict[str, object]:
    mask_path = run_dir / "output" / "masks.zarr"
    cell_table_path = run_dir / "output" / "cells"
    recovery_path = run_dir / "recovery"

    mask = zarr.open_array(str(mask_path), mode="r")
    if tuple(mask.shape) != expected_shape:
        raise AssertionError(f"Mask shape {mask.shape} != input shape {expected_shape}.")
    if np.dtype(mask.dtype) != np.dtype("uint32"):
        raise AssertionError(f"Unexpected mask dtype: {mask.dtype}")
    mask_values = np.asarray(mask[:])
    labeled_pixels = int(np.count_nonzero(mask_values))
    labels = int(np.unique(mask_values[mask_values > 0]).size)
    if labeled_pixels == 0 or labels == 0:
        raise AssertionError("Cellpose completed but produced no labeled pixels.")

    parquet_files = sorted(cell_table_path.rglob("*.parquet"))
    if not parquet_files:
        raise AssertionError("No Parquet fragments were written.")
    parquet_rows = sum(
        int(pq.ParquetFile(path).metadata.num_rows) for path in parquet_files
    )
    if parquet_rows <= 0:
        raise AssertionError("Parquet output contains no cells.")

    manifest = json.loads((recovery_path / "manifest.json").read_text(encoding="utf-8"))
    total_windows = int(manifest["windowPlan"]["totalWindows"])
    checkpoint = (recovery_path / "completed_windows.bin").read_bytes()
    if len(checkpoint) != total_windows or checkpoint != b"\x01" * total_windows:
        raise AssertionError("Window completion bitmap is incomplete or incorrectly sized.")
    if manifest.get("status") != "succeeded":
        raise AssertionError(f"Recovery manifest status is {manifest.get('status')!r}.")

    return {
        "maskShape": list(mask.shape),
        "maskDtype": str(mask.dtype),
        "labeledPixels": labeled_pixels,
        "labelCount": labels,
        "parquetFiles": len(parquet_files),
        "parquetRows": parquet_rows,
        "completedWindows": total_windows,
        "checkpointBytes": len(checkpoint),
        "manifestStatus": manifest["status"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--member")
    parser.add_argument("--max-side", type=int, default=512)
    args = parser.parse_args()

    execution_id = f"cluster-smoke-{uuid.uuid4().hex}"
    run_dir = args.runtime_dir.expanduser().resolve() / "test-runs" / execution_id
    input_path = run_dir / "input" / "actual-image.zarr"
    mask_path = run_dir / "output" / "masks.zarr"
    cell_table_path = run_dir / "output" / "cells"
    recovery_path = run_dir / "recovery"
    input_path.parent.mkdir(parents=True, exist_ok=False)
    mask_path.parent.mkdir(parents=True, exist_ok=False)

    provenance = _prepare_input(
        args.archive.expanduser().resolve(),
        input_path,
        member=args.member,
        max_side=args.max_side,
    )
    (run_dir / "input" / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    graph = _build_graph(
        input_path=input_path,
        mask_path=mask_path,
        cell_table_path=cell_table_path,
    )
    (run_dir / "graph.json").write_text(
        json.dumps(graph, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    status = asyncio.run(_run(graph, recovery_path, execution_id))
    if status != "succeeded":
        raise RuntimeError(f"WorkFlow integration execution ended with status={status}.")
    validation = _validate_run(run_dir, tuple(provenance["sourceShape"]))
    summary = {
        "status": "passed",
        "executionId": execution_id,
        "runDirectory": str(run_dir),
        "slurmJobId": os.environ.get("SLURM_JOB_ID"),
        "cudaVisibleDevices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "source": provenance,
        "validation": validation,
    }
    (run_dir / "result.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print("__WORKFLOW_INTEGRATION_RESULT__")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
