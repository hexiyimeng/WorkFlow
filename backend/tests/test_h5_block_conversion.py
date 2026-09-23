from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import zarr

from tools.convert_h5_block_to_zarr import convert_image, convert_reference


def test_converts_image_channels_and_reference_datasets(tmp_path: Path) -> None:
    image_h5 = tmp_path / "29.h5"
    reference_h5 = tmp_path / "29_seg_.h5"
    image_values = np.arange(2 * 5 * 7 * 9, dtype=np.uint16).reshape(2, 5, 7, 9)
    mask_values = np.arange(5 * 7 * 9, dtype=np.int32).reshape(5, 7, 9)
    cells_values = np.arange(21, dtype=np.int32).reshape(3, 7)
    with h5py.File(image_h5, "w") as handle:
        handle.create_dataset(
            "data",
            data=image_values,
            chunks=(1, 2, 3, 3),
            compression="gzip",
            compression_opts=2,
        )
    with h5py.File(reference_h5, "w") as handle:
        handle.create_dataset("masks", data=mask_values, chunks=(2, 3, 3))
        handle.create_dataset("cells", data=cells_values)

    image_zarr = tmp_path / "block029.zarr"
    reference_zarr = tmp_path / "block029-reference.zarr"
    convert_image(image_h5, image_zarr, block_id=29)
    convert_reference(reference_h5, reference_zarr, block_id=29)

    image = zarr.open_group(str(image_zarr), mode="r")
    assert image.attrs["workflowConversionStatus"] == "complete"
    assert image.attrs["channels"] == ["640nm_10X", "405nm_10X"]
    np.testing.assert_array_equal(image["channels/640nm_10X/0"][:], image_values[0])
    np.testing.assert_array_equal(image["channels/405nm_10X/0"][:], image_values[1])

    reference = zarr.open_group(str(reference_zarr), mode="r")
    assert reference.attrs["workflowConversionStatus"] == "complete"
    np.testing.assert_array_equal(reference["0"][:], mask_values)
    np.testing.assert_array_equal(reference["cells"][:], cells_values)

    # A completed conversion is idempotent and must not rewrite its outputs.
    convert_image(image_h5, image_zarr, block_id=29)
    convert_reference(reference_h5, reference_zarr, block_id=29)


def test_converts_empty_cells_dataset(tmp_path: Path) -> None:
    reference_h5 = tmp_path / "empty_seg_.h5"
    with h5py.File(reference_h5, "w") as handle:
        handle.create_dataset("masks", data=np.zeros((2, 3, 4), dtype=np.int32))
        handle.create_dataset("cells", shape=(0, 7), dtype=np.int32)

    reference_zarr = tmp_path / "empty-reference.zarr"
    convert_reference(reference_h5, reference_zarr, block_id=45)

    reference = zarr.open_group(str(reference_zarr), mode="r")
    assert reference["cells"].shape == (0, 7)
    assert reference["cells"].chunks == (1, 7)
