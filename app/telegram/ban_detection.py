"""Event-driven detection of explicit target bot access bans."""

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import re
import unicodedata
import uuid

from telethon import events


logger = logging.getLogger(__name__)
DEFAULT_BAN_PHRASES = (
    "you are banned",
    "you have been banned",
    "you are temporarily banned",
    "you cannot use this bot",
    "your account is blocked",
    "access to this bot has been denied",
)


def normalize_text(value: str) -> str:
    """Fold Unicode, case and punctuation into comparable words."""

    folded = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(re.findall(r"[\w]+", folded, flags=re.UNICODE))


@dataclass(frozen=True)
class BanDetectionResult:
    """Structured in-memory signal for a detected account-target restriction."""

    detection_id: str
    account_id: str
    target_id: str
    target_username: str
    target_bot_id: int
    matched_phrase: str
    message_excerpt: str
    message_id: int | None
    detected_at: str
    campaign_id: str | None = None


@dataclass
class _ClientWatch:
    client: object
    targets_by_bot_id: dict[int, tuple[str, str, str, object, str | None]]
    active_bot_ids: set[int]
    handler: object


class BanDetectionEngine:
    """Register one incoming-message handler per client and watch known bots."""

    def __init__(self, phrases: tuple[str, ...] = DEFAULT_BAN_PHRASES) -> None:
        normalized = tuple((phrase, normalize_text(phrase)) for phrase in phrases if normalize_text(phrase))
        self._phrases = normalized
        self._watches: dict[int, _ClientWatch] = {}
        self._detected: dict[tuple[str, str], BanDetectionResult] = {}

    def result_for(self, account_id: str, target_id: str) -> BanDetectionResult | None:
        return self._detected.get((str(account_id), str(target_id)))

    def clear_result(self, account_id: str, target_id: str) -> None:
        """Forget an in-memory result only after an explicit owner clear."""

        self._detected.pop((str(account_id), str(target_id)), None)

    def watch_target(
        self, account_id: str, target_id: str, username: str, client, entity,
        on_detected, campaign_id: str | None = None,
    ) -> None:
        """Associate a resolved bot and activate it for the current campaign cycle."""

        client_key = id(client)
        watch = self._watches.get(client_key)
        if watch is None:
            targets: dict[int, tuple[str, str, str, object, str | None]] = {}
            active: set[int] = set()

            async def handle(event) -> None:
                if not event.is_private or event.sender_id is None or event.chat_id != event.sender_id:
                    return
                target = targets.get(int(event.sender_id))
                if target is None or int(event.sender_id) not in active:
                    return
                text = event.raw_text or ""
                normalized = normalize_text(text)
                matched = next((phrase for phrase, candidate in self._phrases if candidate in normalized), None)
                if matched is None:
                    return
                account, target_key, target_username, callback, campaign_id = target
                key = (account, target_key)
                if key in self._detected:
                    return
                result = BanDetectionResult(
                    detection_id=uuid.uuid4().hex,
                    account_id=account,
                    target_id=target_key,
                    target_username=target_username,
                    target_bot_id=int(event.sender_id),
                    matched_phrase=matched,
                    message_excerpt=text[:500],
                    message_id=getattr(event.message, "id", None),
                    detected_at=datetime.now(timezone.utc).isoformat(),
                    campaign_id=campaign_id,
                )
                self._detected[key] = result
                logger.warning(
                    "Target bot access denial detected (account_id=%s, target=%s, phrase=%s)",
                    account,
                    target_username,
                    matched,
                )
                callback(result)

            client.add_event_handler(handle, events.NewMessage(incoming=True))
            watch = _ClientWatch(client, targets, active, handle)
            self._watches[client_key] = watch

        bot_id = int(getattr(entity, "id"))
        watch.targets_by_bot_id[bot_id] = (
            str(account_id), str(target_id), str(username).lstrip("@"), on_detected,
            str(campaign_id) if campaign_id is not None else None,
        )
        watch.active_bot_ids.add(bot_id)

    def stop_watching_target(self, client, entity) -> None:
        watch = self._watches.get(id(client))
        if watch is not None:
            watch.active_bot_ids.discard(int(getattr(entity, "id")))
