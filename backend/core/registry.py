# registry.py
import logging
from typing import Dict, Type

from core.logger import logger
from core.plugin_diagnostics import clear_node_info_errors, record_node_info_error
from core.type_system import validate_port_type


diagnostics_logger = logging.getLogger("WorkFlow.PluginDiagnostics")

NODE_CLASS_MAPPINGS: Dict[str, Type] = {}


def clear_node_registry() -> None:
    NODE_CLASS_MAPPINGS.clear()


def validate_node_port_types(input_config: dict, return_types) -> None:
    for section in ("required", "optional"):
        for input_name, config in input_config.get(section, {}).items():
            declared = config[0] if isinstance(config, (tuple, list)) and config else config
            if isinstance(declared, list):
                continue
            try:
                validate_port_type(declared)
            except ValueError as exc:
                raise ValueError(f"Input '{input_name}' declares {exc}") from exc
    for index, declared in enumerate(return_types or ()):
        try:
            validate_port_type(declared)
        except ValueError as exc:
            raise ValueError(f"Output {index} declares {exc}") from exc


def register_node(name: str):
    """
    装饰器：注册节点类
    """

    def decorator(cls):
        if name in NODE_CLASS_MAPPINGS:
            existing = NODE_CLASS_MAPPINGS[name]
            logger.warning(
                f"[Registry] Duplicate node name '{name}': "
                f"{existing.__name__} will be overridden by {cls.__name__}"
        )
        NODE_CLASS_MAPPINGS[name] = cls
        cls.NODE_TYPE_NAME = name
        return cls

    return decorator


def get_node_info():
    """
    生成符合标准的前端协议 JSON
    """
    clear_node_info_errors()
    info = {}
    for name, cls in NODE_CLASS_MAPPINGS.items():
        # 1. 获取输入定义
        # ComfyUI 标准: INPUT_TYPES 必须是类方法
        if hasattr(cls, "INPUT_TYPES"):
            try:
                input_config = cls.INPUT_TYPES()
            except Exception as e:
                record_node_info_error(name, cls, e)
                diagnostics_logger.exception(
                    "[PluginLoader] Failed to generate object_info for node: %s\n"
                    "Class: %s\n"
                    "Error: %s: %s",
                    name,
                    getattr(cls, "__name__", str(cls)),
                    type(e).__name__,
                    e,
                )
                input_config = {"required": {}, "optional": {}}
        else:
            input_config = {"required": {}, "optional": {}}

        # 2. 处理输出定义
        # RETURN_TYPES: 输出类型的列表 (e.g. ["IMAGE", "MASK"])
        # RETURN_NAMES: 输出插槽的名称 (e.g. ["Image", "Alpha"]) - 可选
        return_types = getattr(cls, "RETURN_TYPES", [])
        return_names = getattr(cls, "RETURN_NAMES", [])
        try:
            validate_node_port_types(input_config, return_types)
        except Exception as e:
            record_node_info_error(name, cls, e)
            diagnostics_logger.exception(
                "[PluginLoader] Invalid port declaration for node: %s\n"
                "Class: %s\n"
                "Error: %s: %s",
                name,
                getattr(cls, "__name__", str(cls)),
                type(e).__name__,
                e,
            )
            continue

        # 如果没有定义输出名称，默认生成 output_0, output_1... 或者直接用类型名
        if not return_names and return_types:
            return_names = return_types

        # 3. 获取其他元数据
        category = getattr(cls, "CATEGORY", "User/Custom")
        display_name = getattr(cls, "DISPLAY_NAME", name)
        description = getattr(cls, "DESCRIPTION", None) or (cls.__doc__.strip() if cls.__doc__ else "No description.")

        # 4. 获取进度类型（兼容字符串和枚举）
        info[name] = {
            "name": name,
            "type": name,  # 前端期望 type 字段，与 name 相同以保持兼容性
            "display_name": display_name,
            "category": category,
            "description": description,
            "input": input_config,
            "output": return_types,
            "output_name": return_names,
            # 告诉前端这个节点实际执行哪个函数（虽然前端不一定用，但这是协议的一部分）
            "output_node": getattr(cls, "OUTPUT_NODE", False),
        }
    return info
