from __future__ import annotations

import pytest

from nodes.ome_zarr_reader import _resolve_group_array


class _FakeArray:
    shape = (1,)
    dtype = "uint8"


class _FakeGroup(dict):
    attrs: dict[str, object] = {}


def test_ambiguous_group_requires_explicit_array_path_with_candidates() -> None:
    group = _FakeGroup(s0=_FakeArray(), s1=_FakeArray(), s2=_FakeArray())

    with pytest.raises(ValueError) as error:
        _resolve_group_array(group, None, 0, 0)

    message = str(error.value)
    assert "multiple candidate arrays" in message
    assert "'s0'" in message
    assert "'s1'" in message
    assert "array_path" in message


def test_explicit_array_path_selects_requested_resolution() -> None:
    selected = _FakeArray()
    group = _FakeGroup(s0=selected, s1=_FakeArray())

    array, path = _resolve_group_array(group, "s0", 0, 0)

    assert array is selected
    assert path == "s0"
