"""Persistent target-wide daily chat limit records."""

from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any

from app.storage.locking import locked
from app.storage.repositories._base import JsonRepository


class DailyLimitRepository(JsonRepository):
    filename = "daily_chat_limits.json"

    def __init__(self, records_dir: str | Path):
        super().__init__(records_dir)

    @staticmethod
    def record_id(target_bot_id: int | str | None, username: str, next_eligibility_at: str) -> str:
        target_key = (
            f"bot:{target_bot_id}" if target_bot_id is not None
            else f"username:{str(username).casefold().lstrip('@')}"
        )
        identity = f"{target_key}\0{next_eligibility_at}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]

    def record_detection(self, record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        saved = dict(record)
        saved["id"] = self.record_id(
            saved.get("target_bot_id"),
            str(saved.get("target_username", "")),
            str(saved["next_eligibility_at"]),
        )
        return self.create_if_absent(saved)

    def expire_due(self, now: datetime) -> list[dict[str, Any]]:
        """Mark elapsed daily limits expired in one locked JSON update."""
        now_iso = now.isoformat()
        expired = []
        with locked(self.path):
            records = self._read_unlocked()
            for record_id, record in records.items():
                if record.get("status") != "Daily Limit Reached":
                    continue
                try:
                    eligibility = datetime.fromisoformat(str(record["next_eligibility_at"]))
                except (KeyError, ValueError):
                    continue
                if eligibility > now:
                    continue
                updated = {**record, "status": "Expired", "expired_at": now_iso}
                records[record_id] = updated
                expired.append(updated)
            if expired:
                self._write_unlocked(records)
        return expired
