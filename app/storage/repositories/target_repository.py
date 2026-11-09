"""Local target metadata repository."""

from app.storage.repositories._base import JsonRepository


class TargetRepository(JsonRepository):
    filename = "targets.json"
