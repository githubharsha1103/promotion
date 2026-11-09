"""Persist detected account-target restrictions and notify the owner."""

import logging
from typing import Awaitable, Callable

from app.storage.repositories.restriction_repository import RestrictionRepository
from app.telegram.ban_detection import BanDetectionResult


logger = logging.getLogger(__name__)
RestrictionNotifier = Callable[[dict], Awaitable[None]]


class RestrictionService:
    def __init__(self, repository: RestrictionRepository) -> None:
        self.repository = repository
        self._notifier: RestrictionNotifier | None = None
        self._active_records: list[dict] = []
        self._loaded = False
        self._load_error: Exception | None = None
        self._listeners: list[Callable[[dict], None]] = []

    def load_active(self) -> bool:
        """Load persistent restrictions before campaign recovery or execution."""

        try:
            self._active_records = [
                record for record in self.repository.list()
                if record.get("status", "active") == "active"
            ]
        except Exception as exc:
            self._loaded = False
            self._load_error = exc
            logger.exception("Could not load active account restrictions (error=%s)", type(exc).__name__)
            return False
        self._loaded = True
        self._load_error = None
        return True

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    def active_for(
        self,
        account_id: str,
        target_id: str,
        *,
        target_bot_id: int | str | None = None,
        username: str | None = None,
    ) -> dict | None:
        if not self._loaded and not self.load_active():
            raise RuntimeError("Active restriction state is unavailable") from self._load_error
        account_id = str(account_id)
        target_id = str(target_id)
        bot_id = str(target_bot_id) if target_bot_id is not None else None
        normalized_username = str(username or "").casefold().lstrip("@")
        for record in self._active_records:
            if str(record.get("account_id")) != account_id:
                continue
            saved_bot_id = record.get("target_bot_id")
            if bot_id is not None:
                if saved_bot_id is not None and str(saved_bot_id) == bot_id:
                    return record
                # A known, different bot ID means this is not the restricted pair.
                if saved_bot_id is not None:
                    continue
            elif saved_bot_id is not None:
                # Until an edited/new target is resolved, use its saved username
                # only as a preflight hint. Resolve changed usernames to compare IDs.
                if normalized_username and str(record.get("target_username", "")).casefold().lstrip("@") == normalized_username:
                    return record
                continue
            if str(record.get("target_id", "")) == target_id:
                return record
            if normalized_username and str(record.get("target_username", "")).casefold().lstrip("@") == normalized_username:
                return record
        return None

    def clear_cached(self, account_id: str, target_id: str) -> None:
        self._active_records = [
            record for record in self._active_records
            if not (
                str(record.get("account_id")) == str(account_id)
                and str(record.get("target_id", "")) == str(target_id)
            )
        ]
        change = {"account_id": str(account_id), "target_id": str(target_id), "status": "cleared"}
        for listener in tuple(self._listeners):
            try:
                listener(change)
            except Exception as exc:
                logger.warning("Restriction listener failed (error=%s)", type(exc).__name__)

    def set_notifier(self, notifier: RestrictionNotifier) -> None:
        self._notifier = notifier

    def add_detection_listener(self, listener: Callable[[dict], None]) -> None:
        """Register an in-process listener for persisted restriction changes."""
        if listener not in self._listeners:
            self._listeners.append(listener)

    async def handle_detection(self, result: BanDetectionResult) -> None:
        excerpt = result.message_excerpt[:500]
        record = {
            "account_id": result.account_id,
            "target_id": result.target_id,
            "target_username": result.target_username,
            "target_bot_id": result.target_bot_id,
            "detection_reason": "Explicit target bot access denial",
            "matched_phrase": result.matched_phrase,
            "message_excerpt": excerpt,
            "detected_at": result.detected_at,
            "campaign_id": result.campaign_id,
            "detection_id": result.detection_id,
            "status": "active",
        }
        try:
            saved, is_new = self.repository.record_detection(record)
        except Exception as exc:
            logger.exception(
                "Could not persist target restriction (account_id=%s, target=%s, error=%s)",
                result.account_id,
                result.target_username,
                type(exc).__name__,
            )
            return
        if not any(existing.get("id") == saved.get("id") for existing in self._active_records):
            self._active_records.append(saved)
        for listener in tuple(self._listeners):
            try:
                listener(saved)
            except Exception as exc:
                logger.warning("Restriction listener failed (error=%s)", type(exc).__name__)
        if not is_new or self._notifier is None:
            return
        try:
            await self._notifier(saved)
        except Exception as exc:
            logger.warning(
                "Could not notify owner about target restriction (account_id=%s, target=%s, error=%s)",
                result.account_id,
                result.target_username,
                type(exc).__name__,
            )
