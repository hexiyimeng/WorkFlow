"""
内存监控工具：用于追踪 execution 生命周期中的内存变化。

支持的内存类型：
- Python 进程内存 (RSS)
- Dask worker 内存（如果可用）
- GPU 显存（如果可用）

使用方式：
    from utils.memory_monitor import MemoryMonitor

    monitor = MemoryMonitor()
    monitor.log_snapshot("execution_start")
    # ... do work ...
    monitor.log_snapshot("execution_end")
    monitor.log_delta("execution_start", "execution_end")
"""

import os
import logging
import time
from typing import Optional, Dict, Any

logger = logging.getLogger("WorkFlow.MemoryMonitor")


class MemoryMonitor:
    """
    内存监控器：记录和对比内存快照。
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.snapshots: Dict[str, Dict[str, Any]] = {}

        # 检测可用的监控后端
        self._has_psutil = self._check_psutil()
        self._has_torch = self._check_torch()

    def reset_for_execution(self, execution_id: str):
        """
        Clear process-global snapshots before a new execution starts.

        This singleton depends on the single-active-execution gate in
        state_manager. If multiple active executions are restored later,
        snapshots must become per-execution instead of process-global.
        """
        logger.debug(f"[MemoryMonitor] Reset snapshots for execution {execution_id}")
        self.snapshots.clear()

    def _check_psutil(self) -> bool:
        """检查 psutil 是否可用"""
        try:
            import psutil
            return True
        except ImportError:
            logger.debug("[MemoryMonitor] psutil not available, process memory tracking disabled")
            return False

    def _check_torch(self) -> bool:
        """检查 PyTorch GPU 是否可用"""
        try:
            import torch
            return torch.cuda.is_available()
        except Exception:
            return False

    def _get_process_memory_mb(self) -> Optional[float]:
        """获取当前进程的 RSS 内存（MB）"""
        if not self._has_psutil:
            return None
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / (1024 * 1024)
        except Exception as e:
            logger.debug(f"[MemoryMonitor] Failed to get process memory: {e}")
            return None

    def _get_gpu_memory_mb(self) -> Optional[Dict[str, float]]:
        """获取 GPU 显存使用情况（MB）"""
        if not self._has_torch:
            return None
        try:
            import torch
            result = {}
            for i in range(torch.cuda.device_count()):
                allocated = torch.cuda.memory_allocated(i) / (1024 * 1024)
                reserved = torch.cuda.memory_reserved(i) / (1024 * 1024)
                result[f"gpu_{i}"] = {
                    "allocated_mb": round(allocated, 1),
                    "reserved_mb": round(reserved, 1)
                }
            return result if result else None
        except Exception as e:
            logger.debug(f"[MemoryMonitor] Failed to get GPU memory: {e}")
            return None

    def _get_dask_memory_mb(self, client=None) -> Optional[Dict[str, float]]:
        """获取 Dask worker 内存使用情况（MB）"""
        if client is None:
            return None
        try:
            # 尝试获取 worker 内存信息
            def get_worker_memory():
                import psutil
                process = psutil.Process()
                return process.memory_info().rss / (1024 * 1024)

            # 使用 client.run 在所有 worker 上执行
            results = client.run(get_worker_memory)
            if results:
                return {k: round(v, 1) for k, v in results.items()}
            return None
        except Exception as e:
            logger.debug(f"[MemoryMonitor] Failed to get Dask memory: {e}")
            return None

    def take_snapshot(self, name: str, client=None) -> Dict[str, Any]:
        """
        记录当前内存快照。

        Args:
            name: 快照名称（如 "execution_start"）
            client: Dask client（可选，用于获取 worker 内存）

        Returns:
            内存快照字典
        """
        snapshot = {
            "timestamp": time.time(),
            "process_mb": self._get_process_memory_mb(),
            "gpu": self._get_gpu_memory_mb(),
            "dask_workers": self._get_dask_memory_mb(client) if client else None
        }

        self.snapshots[name] = snapshot
        return snapshot

    def log_snapshot(self, name: str, client=None, level: str = "info") -> Dict[str, Any]:
        """
        记录快照并输出日志。

        Args:
            name: 快照名称
            client: Dask client
            level: 日志级别

        Returns:
            内存快照字典
        """
        if not self.enabled:
            return {}

        snapshot = self.take_snapshot(name, client)

        # 格式化日志
        parts = []
        if snapshot["process_mb"]:
            parts.append(f"Process={snapshot['process_mb']:.0f}MB")
        if snapshot["gpu"]:
            for gpu_id, info in snapshot["gpu"].items():
                parts.append(f"{gpu_id}=alloc:{info['allocated_mb']:.0f}MB/res:{info['reserved_mb']:.0f}MB")
        if snapshot["dask_workers"]:
            worker_mems = [f"{k}:{v:.0f}MB" for k, v in snapshot["dask_workers"].items()]
            parts.append(f"Dask=[{', '.join(worker_mems)}]")

        msg = f"[Memory] {name}: {' | '.join(parts) if parts else 'N/A'}"

        if level == "debug":
            logger.debug(msg)
        else:
            logger.info(msg)

        return snapshot

    def log_delta(self, name1: str, name2: str) -> Dict[str, Any]:
        """
        计算并记录两个快照之间的内存变化。

        Args:
            name1: 起始快照名称
            name2: 结束快照名称

        Returns:
            内存变化字典
        """
        if not self.enabled:
            return {}

        s1 = self.snapshots.get(name1)
        s2 = self.snapshots.get(name2)

        if not s1 or not s2:
            logger.warning(f"[Memory] Cannot compute delta: missing snapshots {name1} or {name2}")
            return {}

        delta = {
            "time_elapsed_s": round(s2["timestamp"] - s1["timestamp"], 1)
        }

        # Process memory delta
        if s1["process_mb"] and s2["process_mb"]:
            delta["process_delta_mb"] = round(s2["process_mb"] - s1["process_mb"], 1)
            delta["process_delta_percent"] = round(
                (s2["process_mb"] - s1["process_mb"]) / s1["process_mb"] * 100, 1
            ) if s1["process_mb"] > 0 else 0

        # GPU memory delta
        if s1["gpu"] and s2["gpu"]:
            gpu_deltas = {}
            for gpu_id in s2["gpu"]:
                if gpu_id in s1["gpu"]:
                    alloc_delta = s2["gpu"][gpu_id]["allocated_mb"] - s1["gpu"][gpu_id]["allocated_mb"]
                    res_delta = s2["gpu"][gpu_id]["reserved_mb"] - s1["gpu"][gpu_id]["reserved_mb"]
                    gpu_deltas[gpu_id] = {
                        "allocated_delta_mb": round(alloc_delta, 1),
                        "reserved_delta_mb": round(res_delta, 1)
                    }
            if gpu_deltas:
                delta["gpu_delta"] = gpu_deltas

        # Log the delta
        parts = [f"elapsed={delta['time_elapsed_s']}s"]
        if "process_delta_mb" in delta:
            sign = "+" if delta["process_delta_mb"] >= 0 else ""
            parts.append(f"Process={sign}{delta['process_delta_mb']:.0f}MB ({sign}{delta['process_delta_percent']:.1f}%)")
        if "gpu_delta" in delta:
            for gpu_id, info in delta["gpu_delta"].items():
                sign = "+" if info["allocated_delta_mb"] >= 0 else ""
                parts.append(f"{gpu_id}={sign}{info['allocated_delta_mb']:.0f}MB")

        logger.info(f"[Memory] Delta {name1} -> {name2}: {' | '.join(parts)}")

        return delta

    def log_summary(self, execution_id: str = None) -> str:
        """
        生成内存变化摘要字符串。

        Returns:
            摘要字符串，适合记录到日志或返回给前端
        """
        if not self.snapshots:
            return "No memory snapshots recorded"

        # 获取所有快照名称（按时间排序）
        sorted_names = sorted(self.snapshots.keys(), key=lambda n: self.snapshots[n]["timestamp"])

        summary_parts = []
        for name in sorted_names:
            s = self.snapshots[name]
            if s["process_mb"]:
                summary_parts.append(f"{name}:{s['process_mb']:.0f}MB")

        return " -> ".join(summary_parts) if summary_parts else "No process memory data"


# Process-global singleton. This is valid only while state_manager enforces one
# active execution at a time. Multi-active execution would need per-execution
# snapshot storage to avoid mixed diagnostics.
_memory_monitor: Optional[MemoryMonitor] = None


def get_memory_monitor(enabled: bool = True) -> MemoryMonitor:
    """获取全局内存监控器实例"""
    global _memory_monitor
    if _memory_monitor is None:
        _memory_monitor = MemoryMonitor(enabled=enabled)
    return _memory_monitor
