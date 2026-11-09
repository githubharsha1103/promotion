"""Persistent scheduler configuration and state."""

from pathlib import Path

from app.storage.repositories._base import JsonRepository


class SchedulerRepository(JsonRepository):
    filename = "scheduler.json"

    def __init__(self, records_dir: str | Path):
        super().__init__(records_dir)

    def get_config(self) -> dict:
        return self.get("config") or {}

    def save_config(self, values: dict) -> dict:
        current = self.get("config")
        if current is None:
            return self.create({"id": "config", **values})
        return self.update("config", values)

    def get_state(self) -> dict:
        return self.get("state") or {}

    def save_state(self, values: dict) -> dict:
        current = self.get("state")
        if current is None:
            return self.create({"id": "state", **values})
        return self.update("state", values)
