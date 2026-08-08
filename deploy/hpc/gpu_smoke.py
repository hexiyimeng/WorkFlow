from __future__ import annotations

import json
import os
import platform
import time

import torch


def main() -> None:
    started = time.monotonic()
    visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    if not torch.cuda.is_available():
        raise RuntimeError("torch.cuda.is_available() is false")
    if torch.cuda.device_count() != 1:
        raise RuntimeError(
            "Slurm GPU isolation failed: expected exactly one visible GPU, "
            f"found {torch.cuda.device_count()} (CUDA_VISIBLE_DEVICES={visible!r})"
        )

    torch.cuda.set_device(0)
    left = torch.randn((2048, 2048), device="cuda:0")
    right = torch.randn((2048, 2048), device="cuda:0")
    result = left @ right
    checksum = float(result[0, 0].item())
    torch.cuda.synchronize()

    print(
        json.dumps(
            {
                "status": "passed",
                "host": platform.node(),
                "cudaVisibleDevices": visible,
                "torchVersion": torch.__version__,
                "torchCudaVersion": torch.version.cuda,
                "deviceCount": torch.cuda.device_count(),
                "deviceName": torch.cuda.get_device_name(0),
                "deviceMemoryBytes": torch.cuda.get_device_properties(0).total_memory,
                "checksum": checksum,
                "elapsedSeconds": round(time.monotonic() - started, 3),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
