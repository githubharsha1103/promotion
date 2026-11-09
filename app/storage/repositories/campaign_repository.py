"""Local campaign definitions and task repositories."""

from pathlib import Path

from app.storage.repositories._base import JsonRepository
from app.storage.json_store import JsonStore


class CampaignRepository(JsonRepository):
    filename = "campaigns.json"

    def __init__(self, records_dir: str | Path):
        self.records_dir = Path(records_dir)
        super().__init__(records_dir)
        self.tasks = JsonStore(self.records_dir / "campaign_tasks.json")
