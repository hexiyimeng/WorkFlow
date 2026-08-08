"""Framework services for persistent Window recovery directories."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import logging
import os
from pathlib import Path
import stat
from typing import Any, Iterator, Mapping, Sequence
import uuid

import numpy as np

from core.registry import NODE_CLASS_MAPPINGS
from core.window_execution import (
    ActiveExecutionLock,
    CheckpointSummary,
    ExecutionConfig,
    ExecutionLayout,
    RecoveryManifest,
    RecoveryOutput,
    WindowCheckpointStore,
    cleanup_stale_active_lock,
    compute_plan_fingerprint,
    compute_workflow_fingerprint,
    execution_config_to_dict,
    load_execution_config_snapshot,
    load_graph_snapshot,
    load_recovery_manifest,
    require_window_recovery_location,
    validate_recovery_output_separation,
)


logger = logging.getLogger(__name__)


def _absolute_path(value: Any, *, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty absolute path.")
    if "\x00" in value:
        raise ValueError(f"{name} contains a null byte.")
    path = Path(value.strip()).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path.")
    return path.resolve()


def _resolve_literal_output_path(
    node_id: str,
    node_cls: type,
    path_input: str,
    value: Any,
) -> str:
    if not isinstance(value, str):
        raise ValueError(
            f"Terminal OUTPUT node {node_id!r} input {path_input!r} must be a "
            "literal string, not a graph connection or another value type."
        )
    resolved = _absolute_path(
        value,
        name=f"Terminal OUTPUT node {node_id!r} input {path_input!r}",
    )

    # Writers may keep format-specific validation locally. The framework still
    # enforces that a validator cannot infer or append part of the final path.
    validator = getattr(node_cls, "validate_output_path", None)
    if callable(validator):
        validated = _absolute_path(
            validator(value),
            name=f"Terminal OUTPUT node {node_id!r} input {path_input!r}",
        )
        if validated != resolved:
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} modified its configured output "
                "path. The graph must contain the complete final path."
            )
    return str(resolved)


def discover_terminal_outputs(
    graph: Mapping[str, Mapping[str, Any]],
    execution_roots: Sequence[str] | None = None,
) -> tuple[RecoveryOutput, ...]:
    """Discover persistent terminal outputs through ``OUTPUT_PATH_INPUT`` only."""

    if not isinstance(graph, Mapping):
        raise ValueError("graph must be an object.")
    if execution_roots is None:
        # Import lazily so the executor may also consume this service without an
        # import cycle.
        from services.executor import find_execution_roots

        execution_roots = find_execution_roots(dict(graph))

    outputs: list[RecoveryOutput] = []
    for node_id in execution_roots:
        node_data = graph.get(node_id)
        if not isinstance(node_data, Mapping):
            raise ValueError(f"Terminal OUTPUT node {node_id!r} is missing from the graph.")
        node_type = node_data.get("type")
        node_cls = NODE_CLASS_MAPPINGS.get(node_type)
        if node_cls is None:
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} has unregistered type {node_type!r}."
            )

        path_input = getattr(node_cls, "OUTPUT_PATH_INPUT", None)
        if not isinstance(path_input, str) or not path_input.strip():
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} ({node_type}) must declare "
                "OUTPUT_PATH_INPUT for Window recovery."
            )
        path_input = path_input.strip()
        inputs = node_data.get("inputs")
        if not isinstance(inputs, Mapping) or path_input not in inputs:
            raise ValueError(
                f"Terminal OUTPUT node {node_id!r} is missing declared output path "
                f"input {path_input!r}."
            )
        path = _resolve_literal_output_path(
            node_id,
            node_cls,
            path_input,
            inputs[path_input],
        )
        display_name = getattr(node_cls, "DISPLAY_NAME", None) or str(node_type)
        outputs.append(
            RecoveryOutput(
                node_id=str(node_id),
                node_type=str(node_type),
                display_name=str(display_name),
                path_input=path_input,
                path=path,
            )
        )
    return tuple(outputs)


@dataclass(frozen=True)
class RecoveryInspection:
    layout: ExecutionLayout
    manifest: RecoveryManifest
    graph: dict[str, Any]
    execution_config: ExecutionConfig
    checkpoint_summary: CheckpointSummary
    completed_windows_bitmap: np.ndarray = field(repr=False)

    @property
    def completed_windows(self) -> int:
        return self.checkpoint_summary.completed_windows

    @property
    def total_windows(self) -> int:
        return self.checkpoint_summary.total_windows

    @property
    def remaining_windows(self) -> int:
        return self.total_windows - self.completed_windows

    def to_summary(self) -> dict[str, Any]:
        plan = self.manifest.window_plan
        return {
            "found": True,
            "valid": True,
            "compatible": True,
            "executionId": self.manifest.execution_id,
            "status": self.manifest.status,
            "recoveryDirectory": str(self.layout.control_directory),
            "completedWindows": self.completed_windows,
            "totalWindows": self.total_windows,
            "remainingWindows": self.remaining_windows,
            "outputShape": list(plan.output_shape),
            "windowShape": list(plan.window_shape),
            "windowGridShape": list(plan.window_grid_shape),
            "outputs": [output.to_dict() for output in self.manifest.outputs],
        }


def _validate_recovery_directory_entries(layout: ExecutionLayout) -> None:
    allowed_names = {
        layout.manifest_path.name,
        layout.graph_path.name,
        layout.execution_config_path.name,
        layout.checkpoint_path.name,
        layout.lock_path.name,
        layout.lock_mutation_guard_path.name,
    }
    try:
        unexpected_names = sorted(
            entry.name
            for entry in layout.control_directory.iterdir()
            if entry.name not in allowed_names
        )
    except OSError as exc:
        raise PermissionError(
            f"Cannot inspect recovery directory {layout.control_directory}: {exc}"
        ) from exc
    if unexpected_names:
        raise ValueError(
            "Recovery directory contains unsupported entries: "
            + ", ".join(unexpected_names)
        )


def _validate_saved_graph(
    graph: dict[str, Any],
) -> tuple[list[str], tuple[RecoveryOutput, ...]]:
    from services.executor import (
        find_execution_roots,
        validate_graph_acyclic,
        validate_graph_structure,
        validate_graph_types,
    )

    validate_graph_structure(graph)
    validate_graph_acyclic(graph)
    validate_graph_types(graph)
    execution_roots = find_execution_roots(graph)
    if not execution_roots:
        raise ValueError("Recovery graph has no terminal OUTPUT nodes.")
    return execution_roots, discover_terminal_outputs(graph, execution_roots)


def inspect_recovery_directory(
    directory: str | os.PathLike[str],
    *,
    require_unlocked: bool = True,
) -> RecoveryInspection:
    """Validate a complete recovery directory without starting execution."""

    control_directory = _absolute_path(
        str(directory),
        name="recoveryDirectory",
    )
    if not control_directory.exists():
        raise FileNotFoundError(
            f"Recovery directory does not exist: {control_directory}"
        )
    if not control_directory.is_dir():
        raise NotADirectoryError(
            f"Recovery path is not a directory: {control_directory}"
        )

    layout = ExecutionLayout(control_directory)
    if require_unlocked and layout.lock_path.exists():
        if not cleanup_stale_active_lock(layout):
            raise RuntimeError(
                f"Recovery directory is currently active: {control_directory}"
            )
    _validate_recovery_directory_entries(layout)

    manifest = load_recovery_manifest(layout)
    if (
        manifest.status in {"prepared", "running"}
        and not layout.lock_path.exists()
    ):
        # A live Window execution owns active.lock. Without that owner these
        # statuses are last-known state from an interrupted backend process.
        manifest = manifest.with_status("interrupted")
    graph = load_graph_snapshot(layout)
    execution_config = load_execution_config_snapshot(layout)
    if execution_config.mode != "window":
        raise ValueError("Recovery execution_config.json must select Window execution.")
    require_window_recovery_location(execution_config)

    execution_roots, discovered_outputs = _validate_saved_graph(graph)
    saved_layout = ExecutionLayout.resolve(
        execution_config,
        discovered_outputs,
    )
    if saved_layout.control_directory != layout.control_directory:
        raise ValueError(
            "Recovery execution_config.json resolves to a different recovery "
            "directory."
        )
    workflow_fingerprint = compute_workflow_fingerprint(graph, execution_roots)
    if workflow_fingerprint != manifest.workflow_fingerprint:
        raise ValueError(
            "Recovery graph workflow fingerprint does not match manifest.json."
        )

    plan = manifest.window_plan
    if execution_config.window_shape != plan.window_shape:
        raise ValueError(
            "Recovery execution configuration Window shape does not match manifest.json."
        )
    plan_fingerprint = compute_plan_fingerprint(
        plan.output_shape,
        plan.window_shape,
    )
    if plan_fingerprint != manifest.plan_fingerprint:
        raise ValueError(
            "Recovery Window plan fingerprint does not match manifest.json."
        )

    recorded_outputs = {
        output.node_id: output
        for output in manifest.outputs
    }
    if set(recorded_outputs) != {output.node_id for output in discovered_outputs}:
        raise ValueError(
            "Recovery manifest outputs do not match the saved graph terminal outputs."
        )
    for discovered in discovered_outputs:
        recorded = recorded_outputs[discovered.node_id]
        if (
            recorded.node_type != discovered.node_type
            or recorded.path != discovered.path
        ):
            raise ValueError(
                f"Recovery output {discovered.node_id!r} does not match graph.json."
            )

    checkpoint_store = WindowCheckpointStore(layout)
    completed_windows_bitmap = checkpoint_store.load_writable(
        expected_shape=plan.window_grid_shape,
    )
    if completed_windows_bitmap is None:
        raise ValueError(
            "Recovery checkpoint is missing or uses an unsupported format: "
            f"{layout.checkpoint_path}"
        )
    checkpoint = CheckpointSummary(
        shape=tuple(int(size) for size in completed_windows_bitmap.shape),
        dtype=completed_windows_bitmap.dtype,
        completed_windows=int(np.count_nonzero(completed_windows_bitmap)),
        total_windows=int(completed_windows_bitmap.size),
    )
    if checkpoint.total_windows != plan.total_windows:
        raise ValueError(
            "Recovery checkpoint size does not match manifest totalWindows."
        )

    return RecoveryInspection(
        layout=layout,
        manifest=manifest,
        graph=graph,
        execution_config=execution_config,
        checkpoint_summary=checkpoint,
        completed_windows_bitmap=completed_windows_bitmap,
    )


def open_recovery_directory(
    directory: str | os.PathLike[str],
) -> dict[str, Any]:
    inspection = inspect_recovery_directory(
        directory,
        require_unlocked=True,
    )
    return {
        "graph": inspection.graph,
        "readOnly": True,
        "executionConfig": execution_config_to_dict(
            inspection.execution_config
        ),
        "recoverySummary": inspection.to_summary(),
    }


class RecoveryRecordChangedError(RuntimeError):
    """The inspected record was replaced before deletion was confirmed."""


_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x10
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x400


def _is_link_or_reparse_point(path: Path) -> bool:
    if path.is_symlink() or (
        hasattr(path, "is_junction") and path.is_junction()
    ):
        return True
    try:
        details = os.lstat(path)
    except FileNotFoundError:
        return False
    return bool(
        getattr(details, "st_file_attributes", 0)
        & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
    )


def _directory_lstat_identity(path: Path) -> tuple[int, int]:
    if _is_link_or_reparse_point(path):
        raise ValueError(f"Recovery directory is an unsafe link: {path}")
    details = os.lstat(path)
    if not stat.S_ISDIR(details.st_mode):
        raise ValueError(f"Recovery deletion target is not a directory: {path}")
    return (int(details.st_dev), int(details.st_ino))


def _lstat_identity_matches_handle(
    path_identity: tuple[int, int],
    handle_identity: tuple[int, int],
) -> bool:
    if os.name == "nt":
        # BY_HANDLE_FILE_INFORMATION exposes the volume serial separately from
        # Python's st_dev encoding, but both APIs expose the same NTFS file ID
        # as fileIndex/st_ino.  Source and quarantine are siblings, so the
        # same-volume condition is inherent in the atomic rename.
        return path_identity[1] == handle_identity[1]
    return path_identity == handle_identity


@dataclass
class _StableDirectoryHandle:
    path: Path
    identity: tuple[int, int]
    descriptor: int | None = None
    windows_handle: Any = None

    def unlink_regular_file(self, name: str) -> None:
        """Unlink one whitelisted file without following the final entry."""

        if self.descriptor is not None:
            try:
                details = os.stat(
                    name,
                    dir_fd=self.descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return
            if not stat.S_ISREG(details.st_mode):
                raise ValueError(
                    f"Detached recovery entry is not a regular file: {name}"
                )
            os.unlink(name, dir_fd=self.descriptor)
            return

        child = self.path / name
        try:
            details = os.lstat(child)
        except FileNotFoundError:
            return
        if _is_link_or_reparse_point(child) or not stat.S_ISREG(details.st_mode):
            raise ValueError(
                f"Detached recovery entry is not a regular file: {name}"
            )
        # The Windows directory handle held by this object denies parent
        # rename/delete.  A final-entry replacement can make this unlink fail
        # or remove only the replacement itself; unlink never follows it.
        child.unlink()


@contextmanager
def _open_stable_directory(
    path: Path,
    *,
    allow_delete_share: bool,
) -> Iterator[_StableDirectoryHandle]:
    """Open a no-follow directory handle and expose its filesystem identity."""

    if os.name != "nt":
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        descriptor = os.open(path, flags)
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISDIR(details.st_mode):
                raise ValueError(
                    f"Recovery deletion target is not a directory: {path}"
                )
            yield _StableDirectoryHandle(
                path=path,
                identity=(int(details.st_dev), int(details.st_ino)),
                descriptor=descriptor,
            )
        finally:
            os.close(descriptor)
        return

    # os.open() cannot open directories on Windows.  Keep a Win32 directory
    # handle instead; OPEN_REPARSE_POINT makes the link itself visible, and
    # omitting FILE_SHARE_DELETE for the detached directory pins its pathname
    # throughout child cleanup.
    import ctypes
    from ctypes import wintypes

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("dwFileAttributes", wintypes.DWORD),
            ("ftCreationTime", wintypes.FILETIME),
            ("ftLastAccessTime", wintypes.FILETIME),
            ("ftLastWriteTime", wintypes.FILETIME),
            ("dwVolumeSerialNumber", wintypes.DWORD),
            ("nFileSizeHigh", wintypes.DWORD),
            ("nFileSizeLow", wintypes.DWORD),
            ("nNumberOfLinks", wintypes.DWORD),
            ("nFileIndexHigh", wintypes.DWORD),
            ("nFileIndexLow", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    get_information.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    file_read_attributes = 0x80
    file_share_read = 0x1
    file_share_write = 0x2
    file_share_delete = 0x4
    open_existing = 3
    file_flag_backup_semantics = 0x02000000
    file_flag_open_reparse_point = 0x00200000
    share_mode = file_share_read | file_share_write
    if allow_delete_share:
        share_mode |= file_share_delete
    handle = create_file(
        str(path),
        file_read_attributes,
        share_mode,
        None,
        open_existing,
        file_flag_backup_semantics | file_flag_open_reparse_point,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        details = _ByHandleFileInformation()
        if not get_information(handle, ctypes.byref(details)):
            raise ctypes.WinError(ctypes.get_last_error())
        if not (details.dwFileAttributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY):
            raise ValueError(
                f"Recovery deletion target is not a directory: {path}"
            )
        if details.dwFileAttributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError(
                f"Refusing to traverse a recovery symlink or junction: {path}"
            )
        file_index = (
            (int(details.nFileIndexHigh) << 32)
            | int(details.nFileIndexLow)
        )
        yield _StableDirectoryHandle(
            path=path,
            identity=(int(details.dwVolumeSerialNumber), file_index),
            windows_handle=handle,
        )
    finally:
        close_handle(handle)


def _recovery_deletion_target(directory: str | os.PathLike[str]) -> Path:
    raw_value = str(directory)
    if not raw_value.strip() or "\x00" in raw_value:
        raise ValueError("recoveryDirectory must be a non-empty absolute path.")
    target = Path(raw_value.strip()).expanduser()
    if not target.is_absolute():
        raise ValueError("recoveryDirectory must be an absolute path.")
    if _is_link_or_reparse_point(target):
        raise ValueError("Refusing to delete a symlink or junction recovery path.")
    control_directory = target.resolve()
    if control_directory == Path(control_directory.anchor):
        raise ValueError("Refusing to delete a filesystem root recovery path.")
    return control_directory


def _validate_deletable_entries(layout: ExecutionLayout) -> None:
    _validate_recovery_directory_entries(layout)
    for entry in layout.control_directory.iterdir():
        if _is_link_or_reparse_point(entry):
            raise ValueError(
                f"Recovery directory contains an unsafe link: {entry.name}"
            )
        if not entry.is_file():
            raise ValueError(
                f"Recovery directory entry is not a regular file: {entry.name}"
            )


def delete_recovery_record(
    directory: str | os.PathLike[str],
    *,
    expected_execution_id: str,
) -> dict[str, Any]:
    """Atomically detach one inactive record without deleting output data."""

    if (
        not isinstance(expected_execution_id, str)
        or not expected_execution_id.strip()
    ):
        raise ValueError("expectedExecutionId must be a non-empty string.")
    expected_execution_id = expected_execution_id.strip()
    control_directory = _recovery_deletion_target(directory)
    if not control_directory.exists():
        raise FileNotFoundError(
            f"Recovery directory does not exist: {control_directory}"
        )
    if not control_directory.is_dir():
        raise NotADirectoryError(
            f"Recovery path is not a directory: {control_directory}"
        )

    layout = ExecutionLayout(control_directory)
    # Reject an unsafe guard/lock entry before ActiveExecutionLock opens it,
    # then repeat the check while owned to close ordinary mutation races.
    _validate_deletable_entries(layout)
    ownership = ActiveExecutionLock(
        layout,
        f"recovery-delete-{uuid.uuid4().hex}",
    ).acquire()
    renamed = False
    try:
        quarantine = control_directory.with_name(
            f".{control_directory.name}.{uuid.uuid4().hex}.deleting"
        )
        cleanup_errors: list[str] = []
        canonical_names = (
            layout.checkpoint_path.name,
            layout.graph_path.name,
            layout.execution_config_path.name,
            layout.manifest_path.name,
            layout.lock_path.name,
            layout.lock_mutation_guard_path.name,
        )
        # Keep the source directory identity open across rename.  If another
        # process replaces the pathname between validation and rename, the
        # detached object's filesystem identity will differ and is never
        # traversed.
        with _open_stable_directory(
            control_directory,
            allow_delete_share=True,
        ) as source_handle:
            # Validate the complete current-format record only after exclusive
            # ownership and a stable root identity are held.  A second handle
            # immediately before rename closes normal pathname-swap races;
            # the post-rename comparison below is authoritative.
            inspection = inspect_recovery_directory(
                control_directory,
                require_unlocked=False,
            )
            if inspection.manifest.execution_id != expected_execution_id:
                raise RecoveryRecordChangedError(
                    "The recovery record changed after it was inspected. "
                    "Inspect it again before deleting."
                )
            _validate_deletable_entries(layout)
            validate_recovery_output_separation(
                control_directory,
                inspection.manifest.outputs,
            )
            output_paths = [
                output.path for output in inspection.manifest.outputs
            ]
            with _open_stable_directory(
                control_directory,
                allow_delete_share=True,
            ) as current_handle:
                if current_handle.identity != source_handle.identity:
                    raise RecoveryRecordChangedError(
                        "The recovery directory changed during validation. "
                        "Inspect it again before deleting."
                    )
            source_path_identity = _directory_lstat_identity(
                control_directory
            )
            if not _lstat_identity_matches_handle(
                source_path_identity,
                source_handle.identity,
            ):
                raise RecoveryRecordChangedError(
                    "The recovery directory changed immediately before "
                    "deletion. Inspect it again before deleting."
                )
            os.rename(control_directory, quarantine)
            renamed = True
            detached_opened = False
            try:
                with _open_stable_directory(
                    quarantine,
                    allow_delete_share=False,
                ) as detached_handle:
                    if detached_handle.identity != source_handle.identity:
                        raise RecoveryRecordChangedError(
                            "The recovery directory changed during deletion. "
                            "No detached directory contents were removed."
                        )
                    detached_opened = True
                    for name in canonical_names:
                        try:
                            detached_handle.unlink_regular_file(name)
                        except (OSError, ValueError) as exc:
                            cleanup_errors.append(f"{name}: {exc}")
            except RecoveryRecordChangedError:
                raise
            except (OSError, ValueError) as exc:
                # Rename is the logical commit point.  A transient Windows
                # sharing/antivirus failure must not turn a committed delete
                # into an ambiguous API error.  Only classify it as cleanup
                # pending when lstat still proves that the detached pathname
                # names the exact directory that was renamed.
                try:
                    detached_path_identity = _directory_lstat_identity(
                        quarantine
                    )
                except (OSError, ValueError) as identity_error:
                    raise RecoveryRecordChangedError(
                        "The detached recovery directory changed or could not "
                        "be safely identified after deletion. No contents were "
                        "traversed."
                    ) from identity_error
                if detached_path_identity != source_path_identity:
                    raise RecoveryRecordChangedError(
                        "The recovery directory changed during deletion. "
                        "No detached directory contents were removed."
                    ) from exc
                cleanup_errors.append(f"directory access: {exc}")

            if detached_opened:
                try:
                    quarantine.rmdir()
                except OSError as exc:
                    cleanup_errors.append(f"directory: {exc}")

        cleanup_pending = bool(cleanup_errors)
        if cleanup_pending:
            logger.warning(
                "Recovery record %s was detached, but cleanup remains at %s: %s",
                expected_execution_id,
                quarantine,
                "; ".join(cleanup_errors),
            )

        response = {
            "deleted": True,
            "recoveryDirectory": str(control_directory),
            "deletedExecutionId": expected_execution_id,
            "outputsPreserved": output_paths,
            "cleanupPending": cleanup_pending,
        }
        if cleanup_pending:
            response["cleanupDirectory"] = str(quarantine)
        return response
    finally:
        # After the atomic rename, releasing through the old layout would
        # recreate the just-detached source directory and its guard file.
        if not renamed:
            ownership.release()


def _is_recognized_recovery_directory(path: Path) -> bool:
    if not path.is_dir():
        return False
    try:
        load_recovery_manifest(ExecutionLayout(path))
    except (OSError, ValueError):
        return False
    return True


def _available_filesystem_roots() -> tuple[Path, ...]:
    if os.name != "nt":
        return (Path("/"),)

    roots: list[Path] = []
    for codepoint in range(ord("A"), ord("Z") + 1):
        candidate = Path(f"{chr(codepoint)}:\\")
        try:
            if candidate.is_dir():
                roots.append(candidate.resolve())
        except OSError:
            continue
    return tuple(roots)


def list_directories(
    path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    """List any absolute directory accessible to the backend process."""

    if path is None or not str(path).strip():
        roots = _available_filesystem_roots()
        if os.name != "nt":
            return list_directories(roots[0])
        return {
            "path": None,
            "parent": None,
            "directories": [
                {
                    "name": root.drive or str(root),
                    "path": str(root),
                    "isRecoveryDirectory": _is_recognized_recovery_directory(root),
                }
                for root in roots
            ],
        }

    current = _absolute_path(str(path), name="path")
    if not current.exists():
        raise FileNotFoundError(f"Directory does not exist: {current}")
    if not current.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {current}")

    directories: list[dict[str, Any]] = []
    try:
        children = sorted(current.iterdir(), key=lambda item: item.name.casefold())
    except OSError as exc:
        raise PermissionError(f"Cannot list directory {current}: {exc}") from exc
    for child in children:
        try:
            resolved_child = child.resolve()
            if not resolved_child.is_dir():
                continue
        except (OSError, RuntimeError):
            continue
        directories.append({
            "name": child.name,
            "path": str(resolved_child),
            "isRecoveryDirectory": _is_recognized_recovery_directory(
                resolved_child
            ),
        })

    parent = None if current.parent == current else str(current.parent)
    return {
        "path": str(current),
        "parent": parent,
        "directories": directories,
    }
