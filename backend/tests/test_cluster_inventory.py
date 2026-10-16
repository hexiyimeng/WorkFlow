from __future__ import annotations

import pytest

from core.cluster_inventory import parse_scontrol_show_node


def test_parse_scontrol_show_node_keeps_partitioned_node():
    inventory = parse_scontrol_show_node(
        "NodeName=cn001 CPUTot=16 CPUAlloc=0 RealMemory=65536 AllocMem=0 "
        "Gres=(null) State=IDLE Partitions=compute\n"
    )
    assert [node.name for node in inventory.nodes] == ["cn001"]
    assert inventory.nodes[0].partitions == ("compute",)


def test_parse_scontrol_show_node_skips_nodes_without_partitions():
    inventory = parse_scontrol_show_node(
        "NodeName=vn005 CPUTot=64 CPUAlloc=0 RealMemory=131072 AllocMem=0 "
        "Gres=gpu:nvidia:2 State=DOWN+NOT_RESPONDING\n"
        "NodeName=vn006 CPUTot=64 CPUAlloc=0 RealMemory=131072 AllocMem=0 "
        "Gres=(null) State=DOWN+NOT_RESPONDING Partitions=(null)\n"
        "NodeName=cn001 CPUTot=16 CPUAlloc=0 RealMemory=65536 AllocMem=0 "
        "Gres=(null) State=IDLE Partitions=compute\n"
    )
    assert [node.name for node in inventory.nodes] == ["cn001"]


def test_parse_scontrol_show_node_still_rejects_empty_output():
    with pytest.raises(ValueError, match="no NodeName records"):
        parse_scontrol_show_node("")
