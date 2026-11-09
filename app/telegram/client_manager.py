"""Manage independent Telethon clients and filesystem sessions per account."""

import asyncio
import logging
import re
from pathlib import Path
from typing import Any

from telethon import TelegramClient

from app.config import Settings, get_settings


logger = logging.getLogger(__name__)
_SAFE_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class TelegramClientManager:
    """Own one Telethon client per account without handling login credentials."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        max_attempts: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        if max_attempts < 1 or retry_delay < 0:
            raise ValueError("Retry attempts must be positive and delay non-negative")
        self.settings = settings or get_settings()
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self._clients: dict[str, TelegramClient] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _credentials(self) -> tuple[int, str]:
        api_id = self.settings.telegram_api_id
        api_hash = self.settings.telegram_api_hash.get_secret_value()
        if not api_id or not api_hash:
            raise RuntimeError("Telegram API credentials are not configured")
        return api_id, api_hash

    def _session_path(self, account_id: str) -> Path:
        if not _SAFE_ACCOUNT_ID.fullmatch(str(account_id)):
            raise ValueError("Account ID must contain only letters, numbers, '_' or '-'")
        sessions_dir = self.settings.sessions_dir.resolve()
        sessions_dir.mkdir(parents=True, exist_ok=True)
        session_path = (sessions_dir / str(account_id)).resolve()
        if sessions_dir not in session_path.parents:
            raise ValueError("Session path must remain inside the sessions directory")
        return session_path

    def create_client(self, account_id: str) -> TelegramClient:
        """Create (or return) the unique client for an account ID."""

        account_id = str(account_id)
        existing = self._clients.get(account_id)
        if existing is not None:
            return existing
        api_id, api_hash = self._credentials()
        client = TelegramClient(str(self._session_path(account_id)), api_id, api_hash)
        self._clients[account_id] = client
        self._locks[account_id] = asyncio.Lock()
        return client

    def get_client(self, account_id: str) -> TelegramClient | None:
        return self._clients.get(str(account_id))

    async def discard_client(self, account_id: str) -> None:
        """Disconnect and remove an unauthenticated temporary client."""

        account_id = str(account_id)
        await self.disconnect(account_id)
        self._clients.pop(account_id, None)
        self._locks.pop(account_id, None)

    def session_file_exists(self, account_id: str) -> bool:
        return self.session_file_path(account_id).is_file()

    def session_file_path(self, account_id: str) -> Path:
        """Return the filesystem path for an account's Telethon session file."""

        return self._session_path(str(account_id)).with_suffix(".session")

    async def connect(self, account_id: str) -> bool:
        """Connect one account with bounded retries; failures stay account-local."""

        account_id = str(account_id)
        client = self.create_client(account_id)
        lock = self._locks[account_id]
        async with lock:
            if client.is_connected():
                return True
            for attempt in range(1, self.max_attempts + 1):
                try:
                    await client.connect()
                    if await client.is_user_authorized():
                        logger.info("Telegram account connected")
                        return True
                    await client.disconnect()
                    logger.info("Telegram account session is not authorized")
                    return False
                except Exception as exc:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    logger.warning(
                        "Telegram account connection attempt %d/%d failed (error=%s)",
                        attempt,
                        self.max_attempts,
                        type(exc).__name__,
                    )
                    if attempt < self.max_attempts:
                        await asyncio.sleep(self.retry_delay * attempt)
            return False

    async def disconnect(self, account_id: str) -> None:
        """Disconnect one managed account, leaving other clients untouched."""

        account_id = str(account_id)
        client = self._clients.get(account_id)
        if client is None:
            return
        async with self._locks[account_id]:
            try:
                if client.is_connected():
                    await client.disconnect()
            finally:
                logger.info("Telegram account disconnected")

    async def is_connected(self, account_id: str) -> bool:
        client = self._clients.get(str(account_id))
        return bool(client and client.is_connected())

    async def is_authorized(self, account_id: str) -> bool:
        client = self._clients.get(str(account_id))
        if client is None or not client.is_connected():
            return False
        try:
            return await client.is_user_authorized()
        except Exception as exc:
            logger.warning(
                "Could not check Telegram authorization (error=%s)",
                type(exc).__name__,
            )
            return False

    async def reconnect_saved_accounts(self, account_repository: Any) -> dict[str, bool]:
        """Reconnect records marked authenticated when their session file exists."""

        results: dict[str, bool] = {}
        try:
            accounts = account_repository.list()
        except Exception as exc:
            logger.error("Could not read saved Telegram accounts (error=%s)", type(exc).__name__)
            return results

        for account in accounts:
            account_id = str(account.get("id", ""))
            if not account_id or account.get("authenticated") is not True:
                continue
            try:
                if not self.session_file_exists(account_id):
                    logger.info("Skipping saved account without session file")
                    continue
                results[account_id] = await self.connect(account_id)
            except Exception as exc:
                # A malformed account or one account's failure must not prevent others starting.
                logger.warning(
                    "Could not restore Telegram account (error=%s)",
                    type(exc).__name__,
                )
                if account_id:
                    results[account_id] = False
        return results

    async def disconnect_all(self) -> None:
        """Disconnect all clients independently, even if one disconnect fails."""

        for account_id in list(self._clients):
            try:
                await self.disconnect(account_id)
            except Exception as exc:
                logger.warning(
                    "Telegram account shutdown disconnect failed (error=%s)",
                    type(exc).__name__,
                )
