"""Local chat metadata repository."""

from app.storage.repositories._base import JsonRepository


class ChatRepository(JsonRepository):
    filename = "chats.json"
