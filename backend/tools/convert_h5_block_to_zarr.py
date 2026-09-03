"""Stream one fused multiview HDF5 block and its reference mask to Zarr.

The converter is restartable.  It records completed target chunks in each
``.partial`` store and atomically renames that store only after every source
dataset has been copied and verified structurally.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Iterator

import h5py
from numcodecs import Blosc
import numpy as np
import zarr


LOGGER = logging.getLogger("WorkFlow.H5ToZarr")
CHANNEL_NAMES = ("640nm_10X", "405nm_10X")
SPATIAL_CHUNKS = (64, 256, 256)
CELLS_ROW_CHUNK = 65_536
STATE_FILE = ".workflow-conversion.json"
STATE_FLUSH_INTERVAL = 256


def _absolute_input_file(value: str | Path, *, name: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise ValueError(f"{name} must be an absolute regular non-symlink file: {path}")
    return path.resolve(strict=True)


def _absolute_output(value: str | Path, *, name: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise ValueError(f"{name} must be an absolute Zarr directory path.")
    if path.suffix.lower() != ".zarr":
        raise ValueError(f"{name} must end with .zarr: {path}")
    parent = path.parent.resolve(strict=True)
    if not parent.is_dir() or parent.is_symlink():
        raise ValueError(f"{name} parent must be a non-symlink directory: {parent}")
    return parent / path.name


def _source_signature(path: Path, datasets: dict[str, h5py.Dataset]) -> dict[str, object]:
    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtimeNs": stat.st_mtime_ns,
        "datasets": {
            name: {"shape": list(dataset.shape), "dtype": str(dataset.dtype)}
            for name, dataset in sorted(datasets.items())
        },
    }


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_or_create_state(
    partial: Path,
    *,
    kind: str,
    block_id: int,
    signature: dict[str, object],
) -> tuple[dict[str, object], set[str]]:
    partial.mkdir(mode=0o700, parents=False, exist_ok=True)
    state_path = partial / STATE_FILE
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        expected = {
            "schemaVersion": 1,
            "kind": kind,
            "blockId": block_id,
            "source": signature,
        }
        for key, value in expected.items():
            if state.get(key) != value:
                raise RuntimeError(
                    f"Partial conversion state does not match this source ({key}). "
                    f"Inspect or intentionally remove {partial}."
                )
    else:
        state = {
            "schemaVersion": 1,
            "kind": kind,
            "blockId": block_id,
            "source": signature,
            "status": "converting",
            "completedChunks": [],
        }
        _atomic_json(state_path, state)
    completed = {str(value) for value in state.get("completedChunks", [])}
    return state, completed


def _flush_state(partial: Path, state: dict[str, object], completed: set[str]) -> None:
    state["completedChunks"] = sorted(completed)
    _atomic_json(partial / STATE_FILE, state)


def _chunk_slices(
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
) -> Iterator[tuple[tuple[slice, ...], tuple[int, ...]]]:
    if len(shape) != len(chunks):
        raise ValueError("shape and chunks must have equal rank.")

    def recurse(axis: int, selections: list[slice], indexes: list[int]):
        if axis == len(shape):
            yield tuple(selections), tuple(indexes)
            return
        for index, start in enumerate(range(0, shape[axis], chunks[axis])):
            stop = min(start + chunks[axis], shape[axis])
            selections.append(slice(start, stop))
            indexes.append(index)
            yield from recurse(axis + 1, selections, indexes)
            indexes.pop()
            selections.pop()

    yield from recurse(0, [], [])


def _compressor() -> Blosc:
    return Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)


def _copy_chunks(
    source: h5py.Dataset,
    target,
    *,
    state_key: str,
    partial: Path,
    state: dict[str, object],
    completed: set[str],
    source_prefix: tuple[object, ...] = (),
) -> None:
    copied_since_flush = 0
    total = int(np.prod([
        (size + chunk - 1) // chunk
        for size, chunk in zip(target.shape, target.chunks)
    ]))
    done_for_dataset = sum(key.startswith(f"{state_key}:") for key in completed)
    LOGGER.info(
        "Copying %s: shape=%s chunks=%s completed=%s/%s",
        state_key,
        target.shape,
        target.chunks,
        done_for_dataset,
        total,
    )
    for selection, indexes in _chunk_slices(tuple(target.shape), tuple(target.chunks)):
        key = f"{state_key}:" + ".".join(str(index) for index in indexes)
        if key in completed:
            continue
        target[selection] = source[source_prefix + selection]
        completed.add(key)
        copied_since_flush += 1
        done_for_dataset += 1
        if copied_since_flush >= STATE_FLUSH_INTERVAL:
            _flush_state(partial, state, completed)
            LOGGER.info("%s progress: %s/%s chunks", state_key, done_for_dataset, total)
            copied_since_flush = 0
    _flush_state(partial, state, completed)
    LOGGER.info("%s complete: %s/%s chunks", state_key, done_for_dataset, total)


def _existing_complete_store(path: Path, *, kind: str, block_id: int) -> bool:
    if not path.exists():
        return False
    if not path.is_dir() or path.is_symlink():
        raise RuntimeError(f"Output path exists but is not a safe Zarr directory: {path}")
    root = zarr.open_group(str(path), mode="r")
    if root.attrs.get("workflowConversionStatus") != "complete":
        raise RuntimeError(f"Existing output is not marked complete: {path}")
    if root.attrs.get("workflowConversionKind") != kind:
        raise RuntimeError(f"Existing output has the wrong conversion kind: {path}")
    if int(root.attrs.get("blockId", -1)) != block_id:
        raise RuntimeError(f"Existing output has the wrong block id: {path}")
    LOGGER.info("Validated existing completed output; skipping: %s", path)
    return True


def _finalize(partial: Path, output: Path, root, state: dict[str, object]) -> None:
    state["status"] = "complete"
    _flush_state(partial, state, set(str(x) for x in state["completedChunks"]))
    root.attrs["workflowConversionStatus"] = "complete"
    if output.exists():
        raise RuntimeError(f"Refusing to replace existing output: {output}")
    os.replace(partial, output)
    LOGGER.info("Finalized %s", output)


def convert_image(
    source_path: Path,
    output: Path,
    *,
    block_id: int,
) -> None:
    if _existing_complete_store(output, kind="image", block_id=block_id):
        return
    partial = output.with_name(f"{output.name}.partial")
    if partial.is_symlink():
        raise RuntimeError(f"Partial output must not be a symlink: {partial}")
    with h5py.File(source_path, "r") as source_file:
        if "data" not in source_file:
            raise ValueError(f"HDF5 image source has no /data dataset: {source_path}")
        source = source_file["data"]
        if source.ndim != 4 or source.shape[0] != len(CHANNEL_NAMES):
            raise ValueError(
                "Expected /data shape (2,Z,Y,X) for 640nm and 405nm, got "
                f"{source.shape}."
            )
        signature = _source_signature(source_path, {"/data": source})
        state, completed = _load_or_create_state(
            partial,
            kind="image",
            block_id=block_id,
            signature=signature,
        )
        root = zarr.open_group(str(partial), mode="a")
        root.attrs.update({
            "workflowConversionKind": "image",
            "workflowConversionStatus": "converting",
            "blockId": block_id,
            "sourceHdf5": str(source_path),
            "channels": list(CHANNEL_NAMES),
        })
        spatial_shape = tuple(int(value) for value in source.shape[1:])
        spatial_chunks = tuple(
            min(size, chunk) for size, chunk in zip(spatial_shape, SPATIAL_CHUNKS)
        )
        for channel_index, channel_name in enumerate(CHANNEL_NAMES):
            channel_group = root.require_group(f"channels/{channel_name}")
            target = channel_group.require_dataset(
                "0",
                shape=spatial_shape,
                chunks=spatial_chunks,
                dtype=source.dtype,
                compressor=_compressor(),
                fill_value=0,
                overwrite=False,
            )
            channel_group.attrs["multiscales"] = [{
                "version": "0.4",
                "name": f"block-{block_id:03d}-{channel_name}",
                "axes": [
                    {"name": "z", "type": "space"},
                    {"name": "y", "type": "space"},
                    {"name": "x", "type": "space"},
                ],
                "datasets": [{"path": "0"}],
            }]
            _copy_chunks(
                source,
                target,
                state_key=f"channel-{channel_index}",
                partial=partial,
                state=state,
                completed=completed,
                source_prefix=(channel_index,),
            )
        state["completedChunks"] = sorted(completed)
        _finalize(partial, output, root, state)


def convert_reference(
    source_path: Path,
    output: Path,
    *,
    block_id: int,
) -> None:
    if _existing_complete_store(output, kind="reference", block_id=block_id):
        return
    partial = output.with_name(f"{output.name}.partial")
    if partial.is_symlink():
        raise RuntimeError(f"Partial output must not be a symlink: {partial}")
    with h5py.File(source_path, "r") as source_file:
        missing = [name for name in ("masks", "cells") if name not in source_file]
        if missing:
            raise ValueError(
                f"Reference HDF5 is missing dataset(s) {missing}: {source_path}"
            )
        masks = source_file["masks"]
        cells = source_file["cells"]
        if masks.ndim != 3:
            raise ValueError(f"Expected /masks rank 3, got {masks.shape}.")
        if cells.ndim not in (1, 2):
            raise ValueError(f"Expected /cells rank 1 or 2, got {cells.shape}.")
        signature = _source_signature(
            source_path,
            {"/cells": cells, "/masks": masks},
        )
        state, completed = _load_or_create_state(
            partial,
            kind="reference",
            block_id=block_id,
            signature=signature,
        )
        root = zarr.open_group(str(partial), mode="a")
        root.attrs.update({
            "workflowConversionKind": "reference",
            "workflowConversionStatus": "converting",
            "blockId": block_id,
            "sourceHdf5": str(source_path),
            "multiscales": [{
                "version": "0.4",
                "name": f"block-{block_id:03d}-reference-mask",
                "axes": [
                    {"name": "z", "type": "space"},
                    {"name": "y", "type": "space"},
                    {"name": "x", "type": "space"},
                ],
                "datasets": [{"path": "0"}],
            }],
        })
        mask_shape = tuple(int(value) for value in masks.shape)
        mask_chunks = tuple(
            min(size, chunk) for size, chunk in zip(mask_shape, SPATIAL_CHUNKS)
        )
        target_masks = root.require_dataset(
            "0",
            shape=mask_shape,
            chunks=mask_chunks,
            dtype=masks.dtype,
            compressor=_compressor(),
            fill_value=0,
            overwrite=False,
        )
        _copy_chunks(
            masks,
            target_masks,
            state_key="masks",
            partial=partial,
            state=state,
            completed=completed,
        )

        cells_shape = tuple(int(value) for value in cells.shape)
        row_chunk = max(1, min(CELLS_ROW_CHUNK, cells_shape[0]))
        cells_chunks = (
            (row_chunk,)
            if cells.ndim == 1
            else (row_chunk, max(1, cells_shape[1]))
        )
        target_cells = root.require_dataset(
            "cells",
            shape=cells_shape,
            chunks=cells_chunks,
            dtype=cells.dtype,
            compressor=_compressor(),
            fill_value=0,
            overwrite=False,
        )
        _copy_chunks(
            cells,
            target_cells,
            state_key="cells",
            partial=partial,
            state=state,
            completed=completed,
        )
        state["completedChunks"] = sorted(completed)
        _finalize(partial, output, root, state)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert one fused HDF5 image block and reference mask to Zarr."
    )
    parser.add_argument("--block-id", required=True, type=int)
    parser.add_argument("--image-h5", required=True)
    parser.add_argument("--reference-h5", required=True)
    parser.add_argument("--image-zarr", required=True)
    parser.add_argument("--reference-zarr", required=True)
    args = parser.parse_args(argv)
    if args.block_id < 1:
        parser.error("--block-id must be positive.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    image_h5 = _absolute_input_file(args.image_h5, name="image_h5")
    reference_h5 = _absolute_input_file(args.reference_h5, name="reference_h5")
    image_zarr = _absolute_output(args.image_zarr, name="image_zarr")
    reference_zarr = _absolute_output(args.reference_zarr, name="reference_zarr")
    if image_zarr == reference_zarr:
        parser.error("Image and reference Zarr outputs must be different.")

    convert_image(image_h5, image_zarr, block_id=args.block_id)
    convert_reference(reference_h5, reference_zarr, block_id=args.block_id)
    LOGGER.info("HDF5 block %s conversion completed successfully.", args.block_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
