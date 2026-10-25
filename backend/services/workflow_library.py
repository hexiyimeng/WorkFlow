"""Project-local JSON library, independent of algorithms and execution services."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import time
from uuid import UUID, uuid4


class LibraryConflict(ValueError):
    pass


class WorkflowLibrary:
    def __init__(self, root: Path | None = None):
        self.root = (root or Path(os.environ.get(
            "WorkFlow_USER_DIR", str(Path(__file__).resolve().parents[2] / "user")
        ))).expanduser().absolute()

    def directory(self, kind: str) -> Path:
        if kind not in {"workflows", "subgraphs"}:
            raise ValueError("Unknown library kind.")
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("workflows", "subgraphs", "custom_nodes"):
            path = self.root / name
            if path.is_symlink():
                raise ValueError("Library directories must not be symlinks.")
            path.mkdir(exist_ok=True)
        return self.root / kind

    def path(self, kind: str, record_id: str) -> Path:
        try:
            if str(UUID(record_id)) != record_id:
                raise ValueError()
        except (ValueError, AttributeError):
            raise ValueError("Invalid library record ID.") from None
        path = self.directory(kind) / f"{record_id}.json"
        if path.is_symlink():
            raise ValueError("Library records must not be symlinks.")
        return path

    @contextmanager
    def locked(self, kind: str):
        # Cross-process lock: revisions remain valid with multiple API workers.
        path = self.directory(kind) / ".library.lock"
        if path.is_symlink():
            raise ValueError("Invalid library lock.")
        with path.open("a+b") as stream:
            if os.name == "nt":
                import msvcrt
                if stream.tell() == 0:
                    stream.write(b"0")
                    stream.flush()
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    stream.seek(0)
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(stream, fcntl.LOCK_UN)

    @staticmethod
    def summary(record: dict) -> dict:
        return {key: record[key] for key in ("id", "name", "folder", "revision", "updatedAt")}

    def read(self, kind: str, record_id: str) -> dict:
        path = self.path(kind, record_id)
        if path.stat().st_size > 8 * 1024 * 1024:
            raise ValueError("Library document exceeds 8 MiB.")
        with path.open(encoding="utf-8") as stream:
            record = json.load(stream)
        if (not isinstance(record, dict) or record.get("id") != record_id
                or not isinstance(record.get("name"), str)
                or not isinstance(record.get("folder"), str)
                or not isinstance(record.get("revision"), int)
                or not isinstance(record.get("updatedAt"), int)
                or not isinstance(record.get("document"), dict)):
            raise ValueError("Invalid library record.")
        return record

    def list(self, kind: str) -> dict:
        records, warnings = [], []
        for path in self.directory(kind).glob("*.json"):
            try:
                records.append(self.summary(self.read(kind, path.stem)))
            except (ValueError, OSError, KeyError, TypeError):
                warnings.append(f"Could not read library file: {path.name}")
        return {"items": sorted(records, key=lambda r: r["name"].casefold()), "warnings": warnings}

    def save(self, kind: str, name: str, folder: str, document: dict,
             record_id: str | None = None, revision: int | None = None) -> dict:
        name, folder = name.strip(), folder.strip().strip("/")
        if not name or len(name) > 160 or len(folder) > 320:
            raise ValueError("Provide a name (1–160 characters) and a folder up to 320 characters.")
        if not isinstance(document.get("nodes"), list) or not isinstance(document.get("edges"), list):
            raise ValueError("A workflow document must contain nodes and edges arrays.")
        if not isinstance(document.get("workflowId"), str) or not document["workflowId"].strip():
            raise ValueError("A workflow document requires workflowId.")
        with self.locked(kind):
            current = self.read(kind, record_id) if record_id else None
            if current and revision != current["revision"]:
                raise LibraryConflict("This workflow changed elsewhere. Reload it or save a copy.")
            # Names are labels, never filesystem paths. Avoid ambiguous sidebar entries.
            for entry in self.list(kind)["items"]:
                if entry["id"] != record_id and entry["name"] == name and entry["folder"] == folder:
                    raise LibraryConflict("That name already exists in this folder. Choose another name.")
            record_id = record_id or str(uuid4())
            record = {"id": record_id, "name": name, "folder": folder,
                      "revision": current["revision"] + 1 if current else 1,
                      "updatedAt": int(time.time() * 1000), "document": document}
            encoded = json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8")
            if len(encoded) > 8 * 1024 * 1024:
                raise ValueError("Library document exceeds 8 MiB.")
            path = self.path(kind, record_id)
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
                    temporary = Path(stream.name)
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            return record

    def delete(self, kind: str, record_id: str, revision: int) -> None:
        with self.locked(kind):
            if self.read(kind, record_id)["revision"] != revision:
                raise LibraryConflict("This workflow changed elsewhere. Refresh before deleting.")
            self.path(kind, record_id).unlink()
