from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from nodes import cellpose_node


def test_missing_cellpose_model_reports_configured_shared_directory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    configured_directory = tmp_path / "shared-models" / "cellpose"
    monkeypatch.setattr(
        cellpose_node,
        "get_provider_model_dir",
        lambda provider: str(configured_directory),
    )

    with pytest.raises(FileNotFoundError) as error:
        cellpose_node.validate_cellpose_model(
            str(configured_directory / "missing-model"),
            "missing-model",
        )

    message = str(error.value)
    assert str(configured_directory) in message
    assert "WorkFlow_MODELS_DIR" in message
    assert "backend/models/cellpose" not in message


def test_empty_cellpose_model_selection_uses_configured_directory_wording() -> None:
    context = SimpleNamespace(device="cuda:0")

    with pytest.raises(ValueError, match="configured shared Cellpose model directory"):
        cellpose_node.cellpose_block(
            np.zeros((2, 2), dtype=np.float32),
            model_name="",
            ctx=context,
        )
