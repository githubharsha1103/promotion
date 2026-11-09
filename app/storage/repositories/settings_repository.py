"""Local application settings repository."""

from app.storage.repositories._base import JsonRepository


class SettingsRepository(JsonRepository):
    filename = "settings.json"
