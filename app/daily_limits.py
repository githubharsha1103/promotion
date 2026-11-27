"""Target-wide daily chat limit state, persistence, expiry, and notification."""

import asyncio
from datetime import datetime, time, timedelta
import logging
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo

from app.storage.repositories.daily_limit_repository import DailyLimitRepository
from app.telegram.ban_detection import DailyChatLimitDetectionResult


logger = logging.getLogger(__name__)
_IST = ZoneInfo("Asia/Kolkata")
DailyLimitNotifier = Callable[[dict], Awaitable[None]]
DailyLimitListener = Callable[[dict], None]


class DailyChatLimitService:
    """Track active daily limits for each account and target bot."""

    def __init__(
        self,
        repository: DailyLimitRepository,
        reset_time: str = "00:00",
    ) -> None:
        self.repository = repository
        self.reset_time = self._parse_reset_time(reset_time)
        self._active_by_key: dict[str, dict] = {}
        self._listeners: list[DailyLimitListener] = []
        self._notifier: DailyLimitNotifier | None = None

    @staticmethod
    def _parse_reset_time(value: str) -> time:
        try:
            hour, minute = (int(part) for part in str(value).split(":"))
        except (ValueError, AttributeError) as exc:
            raise ValueError("DAILY_CHAT_LIMIT_RESET_TIME must use HH:MM") from exc
        if hour not in range(24) or minute not in range(60):
            raise ValueError("DAILY_CHAT_LIMIT_RESET_TIME must use HH:MM")
        return time(hour, minute)

    @staticmethod
    def target_keys(
        target_bot_id: int | str | None, username: str, account_id: str = "",
    ) -> tuple[str, ...]:
        keys = []
        account_key = f"account:{str(account_id)}:"
        if target_bot_id is not None:
            keys.append(f"{account_key}bot:{target_bot_id}")
        normalized_username = str(username or "").casefold().lstrip("@")
        if normalized_username:
            keys.append(f"{account_key}username:{normalized_username}")
        return tuple(keys)

    def add_detection_listener(self, listener: DailyLimitListener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def set_notifier(self, notifier: DailyLimitNotifier) -> None:
        self._notifier = notifier

    def load_active(self) -> None:
        now = datetime.now(_IST)
        self.repository.expire_due(now)
        for record in self.repository.list():
            if record.get("status") == "Daily Limit Reached":
                self._cache_active(record, notify=False)

    def active_for(
        self, account_id: str, target_bot_id: int | str | None, username: str,
    ) -> dict | None:
        now = datetime.now(_IST)
        for key in self.target_keys(target_bot_id, username, account_id):
            record = self._active_by_key.get(key)
            if record is None:
                continue
            try:
                next_eligibility = datetime.fromisoformat(str(record["next_eligibility_at"]))
            except (KeyError, ValueError):
                logger.error("Daily-limit record has invalid reset time (record_id=%s)", record.get("id"))
                return record
            if next_eligibility > now:
                return record
        return None

    def all_records(self) -> list[dict]:
        return self.repository.list()

    def active_records(self) -> list[dict]:
        unique = {str(record.get("id")): record for record in self._active_by_key.values()}
        return list(unique.values())

    def activate(self, result: DailyChatLimitDetectionResult) -> dict:
        detected_at = datetime.fromisoformat(result.detected_at).astimezone(_IST)
        next_eligibility = datetime.combine(
            detected_at.date(), self.reset_time, tzinfo=_IST,
        )
        if next_eligibility <= detected_at:
            next_eligibility += timedelta(days=1)
        record = {
            "record_type": "daily_chat_limit",
            "status": "Daily Limit Reached",
            "account_id": str(result.account_id),
            "account_name": str(result.account_name or "Unnamed account"),
            "target_id": str(result.target_id),
            "target_bot_id": int(result.target_bot_id),
            "target_username": str(result.target_username).lstrip("@"),
            "matched_phrase": str(result.matched_phrase),
            "original_message": str(result.original_message),
            "message_id": result.message_id,
            "detected_at": detected_at.isoformat(),
            "next_eligibility_at": next_eligibility.isoformat(),
            "campaign_id": result.campaign_id,
            "detection_id": str(result.detection_id),
        }
        record["id"] = self.repository.record_id(
            record["target_bot_id"], record["target_username"],
            record["next_eligibility_at"], record["account_id"],
        )
        self._cache_active(record)
        return record

    async def persist_detection(self, result: DailyChatLimitDetectionResult) -> None:
        record = self.activate(result)
        saved = None
        is_new = False
        for attempt in range(3):
            try:
                saved, is_new = await asyncio.to_thread(
                    self.repository.record_detection, record,
                )
                break
            except Exception as exc:
                if attempt == 2:
                    logger.exception(
                        "Could not persist daily chat limit (target=%s, error=%s)",
                        result.target_username, type(exc).__name__,
                    )
                    raise
                await asyncio.sleep(0.25 * (2 ** attempt))
        if saved is None:
            return
        self._cache_active(saved, notify=False)
        if not is_new or self._notifier is None:
            return
        try:
            await self._notifier(saved)
        except Exception as exc:
            logger.warning(
                "Could not notify owner about daily chat limit (target=%s, error=%s)",
                result.target_username, type(exc).__name__,
            )

    def expire_due(self) -> None:
        now = datetime.now(_IST)
        expired = self.repository.expire_due(now)
        for record in expired:
            for key in self.target_keys(
                record.get("target_bot_id"), str(record.get("target_username", "")),
                str(record.get("account_id", "")),
            ):
                if self._active_by_key.get(key, {}).get("id") == record.get("id"):
                    self._active_by_key.pop(key, None)
            self._notify_listeners(record)

    def _cache_active(self, record: dict, *, notify: bool = True) -> None:
        for key in self.target_keys(
            record.get("target_bot_id"), str(record.get("target_username", "")),
            str(record.get("account_id", "")),
        ):
            self._active_by_key[key] = record
        if notify:
            self._notify_listeners(record)

    def _notify_listeners(self, record: dict) -> None:
        for listener in tuple(self._listeners):
            try:
                listener(record)
            except Exception as exc:
                logger.warning("Daily-limit listener failed (error=%s)", type(exc).__name__)
