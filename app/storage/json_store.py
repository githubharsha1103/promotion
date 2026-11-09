"""Atomic JSON record storage backed by one file per record category."""

import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Callable

from app.storage.locking import locked


class JsonStoreError(RuntimeError):
    """Base error for JSON storage operations."""


class RecordNotFoundError(JsonStoreError):
    """Raised when an update or delete targets a missing record."""


class JsonStore:
    """Store uniquely identified records in a category JSON file."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _read_unlocked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise JsonStoreError(f"Could not read JSON store {self.path}: {exc}") from exc
        if not isinstance(payload, dict) or not all(
            isinstance(key, str) and isinstance(value, dict)
            for key, value in payload.items()
        ):
            raise JsonStoreError(f"JSON store has invalid record structure: {self.path}")
        return payload

    def _write_unlocked(self, records: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(records, handle, ensure_ascii=False, indent=2, allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
        except (OSError, TypeError, ValueError) as exc:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
            raise JsonStoreError(f"Could not write JSON store {self.path}: {exc}") from exc

    def list(self) -> list[dict[str, Any]]:
        with locked(self.path):
            return list(self._read_unlocked().values())

    def get(self, record_id: str) -> dict[str, Any] | None:
        with locked(self.path):
            return self._read_unlocked().get(record_id)

    def create(self, record: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")
        saved = dict(record)
        record_id = str(saved.get("id") or uuid.uuid4().hex)
        saved["id"] = record_id
        with locked(self.path):
            records = self._read_unlocked()
            if record_id in records:
                raise JsonStoreError(f"Record already exists: {record_id}")
            records[record_id] = saved
            self._write_unlocked(records)
        return saved

    def update(
        self,
        record_id: str,
        changes: dict[str, Any] | Callable[[dict[str, Any]], dict[str, Any]],
    ) -> dict[str, Any]:
        with locked(self.path):
            records = self._read_unlocked()
            if record_id not in records:
                raise RecordNotFoundError(f"Record not found: {record_id}")
            current = records[record_id]
            updated = changes(dict(current)) if callable(changes) else {**current, **changes}
            if not isinstance(updated, dict):
                raise TypeError("update must produce a dictionary")
            updated["id"] = record_id
            records[record_id] = updated
            self._write_unlocked(records)
        return updated

    def delete(self, record_id: str) -> bool:
        with locked(self.path):
            records = self._read_unlocked()
            if record_id not in records:
                return False
            del records[record_id]
            self._write_unlocked(records)
        return True
