from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
VALIDATOR_PATH = PROJECT_ROOT / "deploy" / "hpc" / "validate_frontend_dist.py"


def _load_validator():
    spec = importlib.util.spec_from_file_location(
        "workflow_frontend_dist_validator",
        VALIDATOR_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_repository_frontend_build_has_all_referenced_assets() -> None:
    validator = _load_validator()

    references = validator.validate_frontend_dist(PROJECT_ROOT / "backend" / "dist")

    assert references
    assert any(path.suffix == ".js" for path in references)


def test_frontend_validation_rejects_a_missing_referenced_asset(tmp_path: Path) -> None:
    validator = _load_validator()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "index.html").write_text(
        '<script type="module" src="/assets/app.js"></script>',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing or empty frontend asset"):
        validator.validate_frontend_dist(dist_dir)


def test_frontend_validation_rejects_an_entry_without_a_local_bundle(
    tmp_path: Path,
) -> None:
    validator = _load_validator()
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "index.html").write_text(
        '<script type="module" src="https://example.invalid/app.js"></script>',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="local JavaScript bundle"):
        validator.validate_frontend_dist(dist_dir)
