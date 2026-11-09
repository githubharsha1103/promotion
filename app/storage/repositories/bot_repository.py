"""Local bot session and configuration repositories."""

from pathlib import Path

from app.storage.repositories._base import JsonRepository
from app.storage.json_store import JsonStore


class BotRepository(JsonRepository):
    filename = "bot_sessions.json"

    def __init__(self, records_dir: str | Path):
        self.records_dir = Path(records_dir)
        super().__init__(records_dir)
        self.configurations = JsonStore(self.records_dir / "bot_configurations.json")
