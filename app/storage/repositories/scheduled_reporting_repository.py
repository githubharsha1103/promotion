"""Atomic JSON storage for scheduled reporting events and report snapshots."""

from pathlib import Path

from app.storage.repositories._base import JsonRepository


class ScheduledReportingRepository(JsonRepository):
    filename = "scheduled_reporting.json"

    def __init__(self, records_dir: str | Path):
        super().__init__(records_dir)

    def put_once(self, record: dict) -> tuple[dict, bool]:
        """Create a record once using its stable report/event key."""
        record_id = str(record["id"])
        existing = self.get(record_id)
        if existing is not None:
            return existing, False
        try:
            return self.create(record), True
        except Exception:
            # Another coordinator may have written the same stable key.
            existing = self.get(record_id)
            if existing is not None:
                return existing, False
            raise
