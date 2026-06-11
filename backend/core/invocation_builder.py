from __future__ import annotations

import logging
from typing import Any, Mapping

from core.node_invocation import NodeInvocation, NodeRuntime
from core.type_system import is_dask_array_type, parse_port_type


logger = logging.getLogger("WorkFlow.InvocationBuilder")


def get_node_input_defs(node_cls) -> dict:
    if not hasattr(node_cls, "INPUT_TYPES"):
        return {"required": {}, "optional": {}}
    try:
        return node_cls.INPUT_TYPES()
    except Exception as exc:
        logger.warning("Failed to get INPUT_TYPES from %s: %s", node_cls, exc)
        return {"required": {}, "optional": {}}


def declared_type_and_meta(config: Any) -> tuple[Any, dict]:
    declared = config[0] if isinstance(config, (tuple, list)) and len(config) > 0 else config
    meta = config[1] if isinstance(config, (tuple, list)) and len(config) > 1 and isinstance(config[1], dict) else {}
    return declared, meta


def coerce_input_value(name: str, declared_type: Any, value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(declared_type, str):
        return value
    try:
        if declared_type == "INT":
            return int(float(value))
        if declared_type == "FLOAT":
            return float(value)
        if declared_type == "BOOLEAN":
            if isinstance(value, str):
                return value.lower() == "true"
            return bool(value)
        if declared_type == "STRING":
            return str(value)
    except Exception as exc:
        logger.warning("Failed to convert input %s=%r: %s", name, value, exc)
    return value


def validate_dask_array_input(name: str, value: Any) -> None:
    import dask.array as da

    if not isinstance(value, da.Array):
        raise TypeError(
            f"Input '{name}' must be dask.array.Array, got {type(value).__name__}."
        )


TYPE_VALIDATORS = {
    "DASK_ARRAY": validate_dask_array_input,
}


def validate_input_value(name: str, declared_type: Any, value: Any) -> None:
    if value is None or not isinstance(declared_type, str):
        return
    parsed = parse_port_type(declared_type)
    validator = TYPE_VALIDATORS.get(parsed.container)
    if validator is not None:
        validator(name, value)


def prepare_node_inputs(node_cls, raw_inputs: Mapping[str, Any], node_id: str = "Unknown") -> dict[str, Any]:
    final_inputs: dict[str, Any] = {}
    input_defs = get_node_input_defs(node_cls)

    for name, config in input_defs.get("required", {}).items():
        declared, meta = declared_type_and_meta(config)
        value = raw_inputs.get(name)
        is_enum_or_list = isinstance(declared, list) and len(declared) > 0

        if value is None or (isinstance(value, str) and value == ""):
            if "default" in meta:
                value = meta["default"]
            elif is_enum_or_list:
                value = declared[0]

        if value is None or (isinstance(value, str) and value == ""):
            if declared == "STRING":
                raise ValueError(f"Required input '{name}' is missing for Node {node_id}.")
            raise ValueError(
                f"Required input '{name}' is missing for Node {node_id} "
                f"(type={declared}, received={value!r})."
            )
        validate_input_value(name, declared, value)
        final_inputs[name] = value

    for name, config in input_defs.get("optional", {}).items():
        declared, meta = declared_type_and_meta(config)
        value = raw_inputs.get(name)
        if value is None and "default" in meta:
            value = meta["default"]
        validate_input_value(name, declared, value)
        final_inputs[name] = value

    for name, value in list(final_inputs.items()):
        config = input_defs.get("required", {}).get(name) or input_defs.get("optional", {}).get(name)
        if config is None:
            continue
        declared, _ = declared_type_and_meta(config)
        if isinstance(declared, str) and not is_dask_array_type(declared):
            final_inputs[name] = coerce_input_value(name, declared, value)

    return final_inputs


def build_node_invocation(
    node_cls,
    prepared_inputs: Mapping[str, Any],
    node_id: str,
    execution_id: str,
) -> NodeInvocation:
    input_defs = get_node_input_defs(node_cls)
    for section in ("required", "optional"):
        for name, config in input_defs.get(section, {}).items():
            if name not in prepared_inputs:
                continue
            declared, _ = declared_type_and_meta(config)
            validate_input_value(name, declared, prepared_inputs.get(name))

    return NodeInvocation(
        runtime=NodeRuntime(node_id=node_id, execution_id=execution_id),
        input_defs=input_defs,
        inputs=dict(prepared_inputs),
    )
