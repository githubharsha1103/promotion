"""Local message metadata repository."""

from app.storage.repositories._base import JsonRepository


class MessageRepository(JsonRepository):
    filename = "messages.json"
