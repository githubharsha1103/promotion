"""Persistent account-target restriction records."""

import hashlib
from pathlib import Path
from typing import Any

from app.storage.locking import locked
from app.storage.repositories._base import JsonRepository


class RestrictionRepository(JsonRepository):
    filename = "restrictions.json"

    def __init__(self, records_dir: str | Path):
        super().__init__(records_dir)

    @staticmethod
    def pair_id(account_id: str, target_bot_id: int | str | None, username: str) -> str:
        target_key = f"bot:{target_bot_id}" if target_bot_id is not None else f"username:{username.casefold().lstrip('@')}"
        return hashlib.sha256(f"{account_id}\0{target_key}".encode("utf-8")).hexdigest()[:32]

    def record_detection(self, record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Create an active pair restriction or refresh a manually cleared pair."""

        saved = dict(record)
        record_id = self.pair_id(
            str(saved["account_id"]), saved.get("target_bot_id"), str(saved.get("target_username", ""))
        )
        saved["id"] = record_id
        with locked(self.path):
            records = self._read_unlocked()
            existing = records.get(record_id)
            if existing is not None and existing.get("status", "active") == "active":
                return existing, False
            if existing is not None:
                saved["created_at"] = existing.get("created_at", saved.get("detected_at"))
                records[record_id] = saved
            else:
                records[record_id] = saved
            self._write_unlocked(records)
        return saved, True

    def clear(self, record_id: str, *, cleared_at: str, cleared_by: int | None) -> dict[str, Any] | None:
        """Mark a restriction cleared while retaining its audit record."""

        with locked(self.path):
            records = self._read_unlocked()
            current = records.get(str(record_id))
            if current is None:
                return None
            updated = {**current, "status": "cleared", "cleared_at": cleared_at, "cleared_by": cleared_by}
            records[str(record_id)] = updated
            self._write_unlocked(records)
            return updated
