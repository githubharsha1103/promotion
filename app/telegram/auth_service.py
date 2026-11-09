"""Short-lived OTP and 2FA authentication flows for Telegram accounts."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import re
import secrets
import uuid
from typing import Any

from telethon.errors import (
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)

from app.storage.repositories.account_repository import AccountRepository
from app.telegram.client_manager import TelegramClientManager


logger = logging.getLogger(__name__)
_PHONE_PATTERN = re.compile(r"^\+[1-9][0-9]{6,14}$")


class AuthenticationFlowError(Exception):
    """Safe, user-displayable authentication flow error."""


@dataclass
class PendingAuthentication:
    token: str
    account_id: str
    phone: str
    name: str
    phone_code_hash: str
    expires_at: datetime
    requires_password: bool = False


class TelegramAuthenticationService:
    """Handle in-memory OTP and optional 2FA steps for account enrollment."""

    def __init__(
        self,
        client_manager: TelegramClientManager,
        account_repository: AccountRepository,
        *,
        flow_ttl: timedelta = timedelta(minutes=10),
    ) -> None:
        self.client_manager = client_manager
        self.accounts = account_repository
        self.flow_ttl = flow_ttl
        self._pending: dict[str, PendingAuthentication] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _validate_phone(phone: str) -> str:
        normalized = phone.strip()
        if not _PHONE_PATTERN.fullmatch(normalized):
            raise AuthenticationFlowError("Enter a phone number in international format, such as +12345678900.")
        return normalized

    @staticmethod
    def _normalize_for_compare(phone: str) -> str:
        return re.sub(r"\D", "", phone)

    async def _expire_old_flows(self) -> None:
        now = datetime.now(timezone.utc)
        expired = [token for token, flow in self._pending.items() if flow.expires_at <= now]
        for token in expired:
            flow = self._pending.pop(token)
            await self._discard(flow.account_id)

    def _existing_phone(self, phone: str) -> bool:
        target = self._normalize_for_compare(phone)
        return any(
            self._normalize_for_compare(str(record.get("phone", ""))) == target
            for record in self.accounts.list()
            if record.get("phone")
        )

    async def _discard(self, account_id: str) -> None:
        await self.client_manager.discard_client(account_id)
        session_file = self.client_manager.session_file_path(account_id)
        try:
            session_file.unlink(missing_ok=True)
            session_file.with_name(session_file.name + "-journal").unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Could not remove temporary Telegram session file (error=%s)", type(exc).__name__)

    def _get_flow(self, token: str) -> PendingAuthentication:
        flow = self._pending.get(token)
        if flow is None:
            raise AuthenticationFlowError("This authentication step expired or was cancelled. Start again.")
        if flow.expires_at <= datetime.now(timezone.utc):
            raise AuthenticationFlowError("This authentication step expired. Start again.")
        return flow

    async def start(self, phone: str, name: str = "") -> str:
        phone = self._validate_phone(phone)
        name = name.strip()
        if not name or len(name) > 64:
            raise AuthenticationFlowError("Enter an account name between 1 and 64 characters.")
        async with self._lock:
            await self._expire_old_flows()
            if self._existing_phone(phone) or any(
                self._normalize_for_compare(flow.phone) == self._normalize_for_compare(phone)
                for flow in self._pending.values()
            ):
                raise AuthenticationFlowError("This phone number is already registered or has a pending login.")

            account_id = uuid.uuid4().hex
            existing_ids = {str(record.get("id", "")) for record in self.accounts.list()}
            if (
                account_id in existing_ids
                or self.client_manager.get_client(account_id) is not None
                or self.client_manager.session_file_exists(account_id)
            ):
                raise AuthenticationFlowError("Could not allocate a new account session. Try again.")
            try:
                client = self.client_manager.create_client(account_id)
                await client.connect()
                sent_code = await client.send_code_request(phone)
            except PhoneNumberInvalidError as exc:
                await self._discard(account_id)
                raise AuthenticationFlowError("Telegram rejected that phone number. Check it and try again.") from exc
            except Exception as exc:
                await self._discard(account_id)
                logger.warning("Telegram login code request failed (error=%s)", type(exc).__name__)
                raise AuthenticationFlowError("Could not request a login code. Please try again later.") from exc

            token = secrets.token_urlsafe(32)
            self._pending[token] = PendingAuthentication(
                token=token,
                account_id=account_id,
                phone=phone,
                name=name,
                phone_code_hash=sent_code.phone_code_hash,
                expires_at=datetime.now(timezone.utc) + self.flow_ttl,
            )
            logger.info("Telegram login code requested")
            return token

    async def verify_code(self, token: str, code: str) -> bool:
        """Verify a submitted code. Return True when a 2FA password is needed."""

        flow = self._get_flow(token)
        client = self.client_manager.get_client(flow.account_id)
        if client is None:
            raise AuthenticationFlowError("Authentication session is unavailable. Start again.")
        try:
            await client.sign_in(
                phone=flow.phone,
                code=code.strip(),
                phone_code_hash=flow.phone_code_hash,
            )
        except SessionPasswordNeededError:
            flow.requires_password = True
            flow.expires_at = datetime.now(timezone.utc) + self.flow_ttl
            return True
        except PhoneCodeInvalidError as exc:
            raise AuthenticationFlowError("That login code is incorrect. Try again.") from exc
        except PhoneCodeExpiredError as exc:
            await self.cancel(token)
            raise AuthenticationFlowError("That login code expired. Start authentication again.") from exc
        except Exception as exc:
            logger.warning("Telegram login code verification failed (error=%s)", type(exc).__name__)
            raise AuthenticationFlowError("Could not verify the login code. Please try again.") from exc
        await self._complete(token)
        return False

    async def verify_password(self, token: str, password: str) -> None:
        flow = self._get_flow(token)
        if not flow.requires_password:
            raise AuthenticationFlowError("A 2FA password is not expected at this step.")
        client = self.client_manager.get_client(flow.account_id)
        if client is None:
            raise AuthenticationFlowError("Authentication session is unavailable. Start again.")
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError as exc:
            raise AuthenticationFlowError("That 2FA password is incorrect. Try again.") from exc
        except Exception as exc:
            logger.warning("Telegram 2FA verification failed (error=%s)", type(exc).__name__)
            raise AuthenticationFlowError("Could not verify the 2FA password. Please try again.") from exc
        await self._complete(token)

    async def _complete(self, token: str) -> None:
        flow = self._get_flow(token)
        client = self.client_manager.get_client(flow.account_id)
        try:
            authorized = client is not None and await client.is_user_authorized()
        except Exception as exc:
            logger.warning("Could not confirm Telegram authorization (error=%s)", type(exc).__name__)
            authorized = False
        if not authorized:
            await self.cancel(token)
            raise AuthenticationFlowError("Telegram did not authorize this session. Start again.")
        try:
            user: Any = await client.get_me()
        except Exception as exc:
            logger.warning("Could not read authenticated Telegram account metadata (error=%s)", type(exc).__name__)
            raise AuthenticationFlowError("Could not load account details. Please try again.") from exc
        if self._existing_phone(flow.phone):
            await self.cancel(token)
            raise AuthenticationFlowError("This phone number is already registered.")

        record = {
            "id": flow.account_id,
            "phone": flow.phone,
            "name": flow.name,
            "session_file": f"{flow.account_id}.session",
            "telegram_user_id": getattr(user, "id", None),
            "username": getattr(user, "username", None),
            "first_name": getattr(user, "first_name", None),
            "authenticated": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.accounts.create(record)
        except Exception as exc:
            await self.cancel(token)
            logger.warning("Could not save authenticated Telegram account (error=%s)", type(exc).__name__)
            raise AuthenticationFlowError("Could not save this account. No existing session was changed.") from exc
        self._pending.pop(token, None)
        logger.info("Telegram account authentication completed")

    async def cancel(self, token: str) -> None:
        flow = self._pending.pop(token, None)
        if flow is not None:
            await self._discard(flow.account_id)

    async def close(self) -> None:
        """Discard pending, unauthenticated sessions during application shutdown."""

        tokens = list(self._pending)
        for token in tokens:
            await self.cancel(token)
