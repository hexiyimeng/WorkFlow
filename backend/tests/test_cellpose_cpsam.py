from __future__ import annotations

import numpy as np

from nodes.cellpose_node import Cellpose, cellpose_block


class _FakeModel:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def eval(self, image, **kwargs):
        self.kwargs = kwargs
        return np.zeros(np.asarray(image).shape, dtype=np.uint32), None, None


class _FakeContext:
    device = "cuda:0"
    resources = {"axes": ("Y", "X")}
    primary_input_name = "image"

    def __init__(self) -> None:
        self.requested_model: str | None = None
        self.fake_model = _FakeModel()

    def model(self, *, name: str, **_kwargs):
        self.requested_model = name
        return self.fake_model


def test_cellpose_defaults_to_cpsam() -> None:
    model_input = Cellpose.INPUT_TYPES()["required"]["model_name"]

    assert model_input[0][0] == "cpsam"
    assert model_input[1]["default"] == "cpsam"


def test_legacy_cyto3_workflow_uses_cpsam_and_native_patch_size() -> None:
    context = _FakeContext()

    result = cellpose_block(
        np.zeros((16, 16), dtype=np.float32),
        model_name="cyto3",
        ctx=context,
    )

    assert result.shape == (16, 16)
    assert context.requested_model == "cpsam"
    assert context.fake_model.kwargs is not None
    assert context.fake_model.kwargs["bsize"] == 256
