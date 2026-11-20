"""Atomic JSON storage for scheduled reporting events and report snapshots."""

from pathlib import Path

from app.storage.repositories._base import JsonRepository


class ScheduledReportingRepository(JsonRepository):
    filename = "scheduled_reporting.json"

    def __init__(self, records_dir: str | Path):
        super().__init__(records_dir)

    def put_once(self, record: dict) -> tuple[dict, bool]:
        """Create a record once using its stable report/event key."""
        return self.create_if_absent(record)
