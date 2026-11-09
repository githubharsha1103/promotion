"""Local sticker metadata repository."""

from app.storage.repositories._base import JsonRepository


class StickerRepository(JsonRepository):
    filename = "stickers.json"
