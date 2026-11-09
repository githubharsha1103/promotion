"""Local account metadata repository."""

from app.storage.repositories._base import JsonRepository


class AccountRepository(JsonRepository):
    filename = "accounts.json"
