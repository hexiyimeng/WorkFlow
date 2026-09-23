import logging
import multiprocessing
import os
import socket

from core.platform import dask_spill_dir

logger = logging.getLogger("WorkFlow.Config")


def _parse_nonnegative_int(value: str, *, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be a non-negative integer, got {value!r}.")
    return parsed


def _parse_positive_int(value: str, *, name: str) -> int:
    parsed = _parse_nonnegative_int(value, name=name)
    if parsed == 0:
        raise ValueError(f"{name} must be greater than zero, got {value!r}.")
    return parsed


def _parse_gpu_ids(value: str, *, name: str = "WorkFlow_GPU_IDS") -> tuple[str, ...]:
    parts = tuple(part.strip() for part in value.split(","))
    if not parts or any(not part for part in parts):
        raise ValueError(f"{name} must not contain empty GPU identifiers, got {value!r}.")
    if len(set(parts)) != len(parts):
        raise ValueError(f"{name} must not contain duplicate GPU identifiers, got {value!r}.")
    return parts


def _is_main_process():
    return multiprocessing.current_process().name == "MainProcess"


def _log_override(message):
    if _is_main_process():
        logger.warning(message)
    else:
        logger.debug(message)


class AppConfig:
    """
    Global runtime configuration.

    This module intentionally does not configure provider-specific model cache
    paths such as Cellpose, StarDist, SAM, etc. Model storage is handled by
    core.model_registry using a generic provider layout beneath the configured
    model root (``WorkFlow_MODELS_DIR``), which defaults to:

        backend/models/{provider}/

    Provider-specific nodes may translate that generic directory into whatever
    a third-party library requires at import/load time.
    """

    _instance = None

    GPU_IDS = None
    DASHBOARD_ADDRESS = ":8787"
    DASHBOARD_HOST = None

    MAX_IN_FLIGHT_WINDOWS = None
    DASK_LOCAL_DIR = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
            cls._instance._detect_environment()
        return cls._instance

    def _detect_environment(self):
        hostname = socket.gethostname().lower()
        cpu_count = multiprocessing.cpu_count()

        if _is_main_process():
            logger.info(f"[Config] Host={hostname} | CPU={cpu_count}")

        logger.debug("[Config] Worker resources are supplied by Worker Profiles and Pools.")
        self.GPU_IDS = None
        self.MAX_IN_FLIGHT_WINDOWS = None

        gpu_ids = os.getenv("WorkFlow_GPU_IDS")
        if gpu_ids is not None:
            self.GPU_IDS = _parse_gpu_ids(gpu_ids)
            _log_override(f"   -> [Override] WorkFlow_GPU_IDS={','.join(self.GPU_IDS)}")

        max_in_flight = os.getenv("WorkFlow_MAX_IN_FLIGHT_WINDOWS")
        if max_in_flight is not None:
            self.MAX_IN_FLIGHT_WINDOWS = _parse_positive_int(
                max_in_flight,
                name="WorkFlow_MAX_IN_FLIGHT_WINDOWS",
            )
            _log_override(
                f"   -> [Override] WorkFlow_MAX_IN_FLIGHT_WINDOWS={self.MAX_IN_FLIGHT_WINDOWS}"
            )

        if os.getenv("WorkFlow_DASK_LOCAL_DIR"):
            self.DASK_LOCAL_DIR = str(dask_spill_dir(os.getenv("WorkFlow_DASK_LOCAL_DIR")))
            _log_override(f"   -> [Override] WorkFlow_DASK_LOCAL_DIR={self.DASK_LOCAL_DIR}")

        if os.getenv("WorkFlow_DASHBOARD_HOST"):
            self.DASHBOARD_HOST = os.getenv("WorkFlow_DASHBOARD_HOST")
            _log_override(f"   -> [Override] WorkFlow_DASHBOARD_HOST={self.DASHBOARD_HOST}")

        dashboard_address = os.getenv("WorkFlow_DASHBOARD_ADDRESS")
        if dashboard_address is not None:
            dashboard_address = dashboard_address.strip()
            if not dashboard_address:
                raise ValueError(
                    "WorkFlow_DASHBOARD_ADDRESS must not be empty when provided."
                )
            self.DASHBOARD_ADDRESS = dashboard_address
            _log_override(
                "   -> [Override] WorkFlow_DASHBOARD_ADDRESS="
                f"{self.DASHBOARD_ADDRESS}"
            )

        spill_str = self.DASK_LOCAL_DIR or "auto"
        logger.debug(
            f"[Config] Final base config: SpillDir={spill_str}; "
            "Worker counts and memory are supplied by Worker Profiles and Pools."
        )


config = AppConfig()
