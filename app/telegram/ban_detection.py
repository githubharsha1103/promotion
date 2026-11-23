"""Event-driven detection of explicit target bot access bans."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import re
import time
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
    targets_by_bot_id: dict[int, tuple[str, str, str, object, str | None, object | None, object | None]]
    active_bot_ids: set[int]
    handler: object


ORDINARY_EMOJI_PATTERN = re.compile(
    r"\btap\s+(?P<emoji>[^\s]+)\s+below\b",
    flags=re.IGNORECASE | re.UNICODE,
)

TASK_COMPLETION_PATTERN = re.compile(
    r"^\s*🎉\ufe0f?\s*match\s+successful!?\s*$",
    flags=re.IGNORECASE | re.UNICODE,
)

OTHER_TASK_COMPLETION_PATTERN = re.compile(
    r"(?:\btask\s+completed\b|\bpoints?\s+(?:added|credited)\b)",
)


def is_task_completion_confirmation(value: str) -> bool:
    """Recognize documented completion text regardless of emoji or Unicode formatting."""

    normalized = unicodedata.normalize("NFKC", value).strip()
    if TASK_COMPLETION_PATTERN.fullmatch(normalized):
        return True
    return OTHER_TASK_COMPLETION_PATTERN.search(normalize_text(value)) is not None


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
        on_verification=None, on_confirmation=None,
    ) -> None:
        """Associate a resolved bot and activate it for the current campaign cycle."""

        client_key = id(client)
        watch = self._watches.get(client_key)
        if watch is None:
            targets: dict[int, tuple[str, str, str, object, str | None, object | None, object | None]] = {}
            active: set[int] = set()

            async def handle(event) -> None:
                received_monotonic_ns = time.monotonic_ns()
                text = event.raw_text or ""
                confirmation_match = is_task_completion_confirmation(text)
                if not event.is_private or event.sender_id is None or event.chat_id != event.sender_id:
                    if confirmation_match:
                        logger.info(
                            "Incoming confirmation reached Telethon handler "
                            "(monotonic_ns=%d, route=rejected_non_private)",
                            received_monotonic_ns,
                        )
                    return
                target = targets.get(int(event.sender_id))
                if target is None:
                    if confirmation_match:
                        logger.info(
                            "Incoming confirmation reached Telethon handler "
                            "(monotonic_ns=%d, route=rejected_unmapped_sender)",
                            received_monotonic_ns,
                        )
                    return
                if int(event.sender_id) not in active:
                    if confirmation_match:
                        logger.info(
                            "Incoming confirmation reached Telethon handler "
                            "(monotonic_ns=%d, route=rejected_inactive_target)",
                            received_monotonic_ns,
                        )
                    return
                account, target_key, target_username, callback, campaign_id, verification_callback, confirmation_callback = target

                normalized = normalize_text(text)
                matched = next((phrase for phrase, candidate in self._phrases if candidate in normalized), None)
                if matched is not None:
                    key = (account, target_key)
                    if key not in self._detected:
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
                    return

                emoji_match = ORDINARY_EMOJI_PATTERN.search(unicodedata.normalize("NFKC", text))
                if emoji_match is not None and verification_callback is not None:
                    emoji = emoji_match.group("emoji").strip()
                    asyncio.create_task(
                        verification_callback(
                            event, account, target_key, target_username, emoji,
                            campaign_id,
                        )
                    )
                    return

                if confirmation_match:
                    logger.info(
                        "Incoming confirmation reached Telethon handler "
                        "(monotonic_ns=%d, text_match=true, route=accepted, handler_available=%s)",
                        received_monotonic_ns,
                        confirmation_callback is not None,
                    )
                    if confirmation_callback is not None:
                        asyncio.create_task(
                            confirmation_callback(
                                event, account, target_key, text,
                                campaign_id, received_monotonic_ns,
                            )
                        )

            client.add_event_handler(handle, events.NewMessage(incoming=True))
            watch = _ClientWatch(client, targets, active, handle)
            self._watches[client_key] = watch

        bot_id = int(getattr(entity, "id"))
        watch.targets_by_bot_id[bot_id] = (
            str(account_id), str(target_id), str(username).lstrip("@"), on_detected,
            str(campaign_id) if campaign_id is not None else None,
            on_verification, on_confirmation,
        )
        watch.active_bot_ids.add(bot_id)

    def stop_watching_target(self, client, entity) -> None:
        watch = self._watches.get(id(client))
        if watch is not None:
            watch.active_bot_ids.discard(int(getattr(entity, "id")))
