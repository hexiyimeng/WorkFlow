"""Compatibility entry point for the Slurm compute-side Worker launcher.

The workflow Driver now remains in the long-lived service process. Compute
nodes only launch Resource-Planner-defined Dask Workers, so the historical
compute-side graph runner delegates to :mod:`services.slurm_worker_launcher`.
"""

from __future__ import annotations

from services.slurm_worker_launcher import main


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
