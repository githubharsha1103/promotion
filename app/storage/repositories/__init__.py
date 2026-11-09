"""JSON-backed repositories for local application records."""

from app.storage.repositories.account_repository import AccountRepository
from app.storage.repositories.bot_repository import BotRepository
from app.storage.repositories.campaign_repository import CampaignRepository
from app.storage.repositories.chat_repository import ChatRepository
from app.storage.repositories.message_repository import MessageRepository
from app.storage.repositories.settings_repository import SettingsRepository
from app.storage.repositories.sticker_repository import StickerRepository
from app.storage.repositories.target_repository import TargetRepository

__all__ = [
    "AccountRepository", "BotRepository", "CampaignRepository", "ChatRepository",
    "MessageRepository", "SettingsRepository", "StickerRepository", "TargetRepository",
]
