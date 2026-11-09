"""Shared JSON repository base."""

from pathlib import Path

from app.storage.json_store import JsonStore


class JsonRepository(JsonStore):
    """Bind a repository to a named JSON file below the records directory."""

    filename = "records.json"

    def __init__(self, records_dir: str | Path):
        super().__init__(Path(records_dir) / self.filename)
