"""Sequential, resumable MTProto campaign execution."""

import asyncio
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import time
import re
import unicodedata
from typing import Awaitable, Callable
import uuid

from telethon.errors import FloodWaitError
from telethon.tl import types

from app.daily_limits import DailyChatLimitService
from app.storage.repositories.account_repository import AccountRepository
from app.storage.repositories.campaign_repository import CampaignRepository
from app.storage.repositories.sticker_repository import StickerRepository
from app.storage.repositories.target_repository import TargetRepository
from app.storage.json_store import JsonStore
from app.telegram.client_manager import TelegramClientManager
from app.telegram.ban_detection import (
    BanDetectionEngine,
    BanDetectionResult,
    DEFAULT_BAN_PHRASES,
    DEFAULT_DAILY_CHAT_LIMIT_PHRASES,
    DailyChatLimitDetectionResult,
)
from app.restrictions import RestrictionService


logger = logging.getLogger(__name__)
SCHEDULED_PROMOTION_ID = "scheduled-promotion"
PROMOTION_MESSAGES = (
    "Join fast",
    "Join it",
    "This one is best",
    "Its completely free",
    "Highly recommended",
    "Try it now",
    "Best one here",
    "Don't miss this",
)


@dataclass(frozen=True)
class CampaignTiming:
    """Central internal timing defaults; values were not available in legacy config."""

    AFTER_SEARCH_SECONDS: float = 1.5
    AFTER_STICKER_SECONDS: float = 1.5
    AFTER_TEXT_SECONDS: float = 1.5
    AFTER_STOP_SECONDS: float = 1.5


@dataclass
class _Runtime:
    task: asyncio.Task | None = None
    pause: asyncio.Event = field(default_factory=asyncio.Event)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    notify_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    callback: Callable[[dict], Awaitable[None]] | None = None
    suspend_accounts: set[str] = field(default_factory=set)
    suspended_accounts: set[str] = field(default_factory=set)
    failed_suspensions: set[str] = field(default_factory=set)
    scheduled_campaign_id: str | None = None
    assignment_id: str | None = None
    scheduled_target_ids: list[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)


class _TargetAccessDenied(Exception):
    """Stop only the account-target task after an explicit denial response."""


class _TargetOptedOut(Exception):
    """Stop sending immediately when the destination withdraws consent."""


class _TargetDailyLimitReached(Exception):
    """Stop all account work for a target until its daily reset."""


class _RestrictionCheckUnavailable(Exception):
    """Fail closed when persistent restriction state cannot be read."""


class _VerificationPaused(Exception):
    """Pause only the affected account-target pair for an ordinary emoji task."""


@asynccontextmanager
async def _execution_lock(lock: asyncio.Lock, acquire: bool):
    if acquire:
        async with lock:
            yield
    else:
        yield


class CampaignExecutionService:
    """Run each account's target list sequentially with durable task checkpoints."""

    def __init__(
        self,
        campaign_repository: CampaignRepository,
        account_repository: AccountRepository,
        target_repository: TargetRepository,
        sticker_repository: StickerRepository,
        client_manager: TelegramClientManager,
        *,
        timing: CampaignTiming | None = None,
        restriction_service: RestrictionService | None = None,
        daily_limit_service: DailyChatLimitService | None = None,
    ) -> None:
        self.campaigns = campaign_repository
        self.accounts = account_repository
        self.targets = target_repository
        self.stickers = sticker_repository
        self.client_manager = client_manager
        self.timing = timing or CampaignTiming()
        configured_phrases = tuple(
            phrase.strip()
            for phrase in str(getattr(client_manager.settings, "ban_detection_phrases", "")).split("|")
            if phrase.strip()
        )
        # Keep built-in safety phrases enabled even when an environment override
        # supplies a custom list.
        phrases = tuple(dict.fromkeys((*DEFAULT_BAN_PHRASES, *configured_phrases)))
        configured_daily_phrases = tuple(
            phrase.strip()
            for phrase in str(getattr(client_manager.settings, "daily_chat_limit_phrases", "")).split("|")
            if phrase.strip()
        )
        daily_phrases = tuple(dict.fromkeys((
            *DEFAULT_DAILY_CHAT_LIMIT_PHRASES, *configured_daily_phrases,
        )))
        self.ban_detection = BanDetectionEngine(phrases, daily_phrases)
        self.restriction_service = restriction_service
        self.daily_limit_service = daily_limit_service
        self._ban_events: dict[tuple[str, str], asyncio.Event] = {}
        self._restriction_tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._daily_limit_events: dict[str, asyncio.Event] = {}
        self._daily_limit_tasks: dict[str, asyncio.Task] = {}
        self._scheduled_runtimes: dict[str, _Runtime] = {}
        self._pending_scheduled_stops: dict[str, _Runtime] = {}
        self._scheduled_stop_persist_task: asyncio.Task | None = None
        self._scheduled_claims: set[str] = set()
        self._normal_account_workers: dict[str, dict[asyncio.Task, tuple[str, _Runtime]]] = {}
        self._runtimes: dict[str, _Runtime] = {}
        self._account_locks: dict[str, asyncio.Lock] = {}
        self._rotation_locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()
        self._storage_io_slots = asyncio.Semaphore(4)
        self._campaign_state_lock = asyncio.Lock()
        self._scheduled_event_recorder = None
        self._scheduled_promotion_repository = None
        self._scheduled_task_store = JsonStore(campaign_repository.records_dir / "scheduled_promotion_tasks.json")
        self._verification_store = JsonStore(campaign_repository.records_dir / "emoji_verification_tasks.json")
        self._verification_events: dict[tuple[str, str, str], asyncio.Event] = {}
        self._verification_confirmed_events: dict[tuple[str, str, str], asyncio.Event] = {}
        self._verification_registered_monotonic_ns: dict[tuple[str, str, str], int] = {}
        if self.daily_limit_service is not None:
            self.daily_limit_service.add_detection_listener(self._daily_limit_changed)
            for record in self.daily_limit_service.active_records():
                self._daily_limit_changed(record)

    def bind_scheduled_promotion_repository(self, repository) -> None:
        """Bind standalone scheduled promotion metadata to the scheduler JSON store."""
        self._scheduled_promotion_repository = repository

    def scheduled_promotion_model(self) -> dict | None:
        if self._scheduled_promotion_repository is None:
            return None
        model = self._scheduled_promotion_repository.get_config().get("scheduled_execution")
        return dict(model) if isinstance(model, dict) else None

    def _is_standalone_scheduled(self, campaign_id: str) -> bool:
        return str(campaign_id) == SCHEDULED_PROMOTION_ID

    def _task_store(self, campaign_id: str):
        return self._scheduled_task_store if self._is_standalone_scheduled(campaign_id) else self.campaigns.tasks

    def scheduled_sticker_available(self, sticker_id: str | None) -> bool:
        try:
            self._sticker_path(str(sticker_id or ""))
            return True
        except (RuntimeError, OSError, ValueError):
            return False

    def set_scheduled_event_recorder(self, recorder) -> None:
        """Connect durable scheduled-only reporting instrumentation."""
        self._scheduled_event_recorder = recorder

    def _record_scheduled_event(self, event_type: str, *, identity: str | None = None, **values) -> None:
        if self._scheduled_event_recorder is None:
            return
        try:
            if self._is_standalone_scheduled(str(values.get("campaign_id", ""))):
                model = self.scheduled_promotion_model() or {}
                values.setdefault("campaign_name", model.get("name", "Scheduled Promotion"))
                values.setdefault("total_cycles", int(model.get("total_cycles", 1)))
            self._scheduled_event_recorder(event_type, identity=identity, **values)
        except Exception as exc:
            logger.warning("Scheduled reporting hook failed (type=%s, error=%s)",
                           event_type, type(exc).__name__)

    def _is_scheduled_pair(self, campaign_id: str, account_id: str) -> bool:
        runtime = self._scheduled_runtimes.get(str(account_id))
        return runtime is not None and runtime.scheduled_campaign_id == str(campaign_id)

    def _record_scheduled_operation(self, campaign_id: str, account_id: str, target_id: str,
                                    cycle: int, operation: str) -> None:
        if not self._is_scheduled_pair(campaign_id, account_id):
            return
        assignment = self._scheduled_runtimes[str(account_id)].assignment_id
        self._record_scheduled_event(
            "telegram_operation", identity=f"operation:{assignment}:{target_id}:{cycle}:{operation}",
            assignment_id=assignment, campaign_id=str(campaign_id), account_id=str(account_id),
            target_id=str(target_id), cycle=int(cycle), operation=operation,
        )

    def get(self, campaign_id: str) -> dict | None:
        if self._is_standalone_scheduled(campaign_id):
            return self.scheduled_promotion_model()
        return self.campaigns.get(str(campaign_id))

    def account_execution_lock(self, account_id: str) -> asyncio.Lock:
        """Shared lock for manual and future scheduled account workers."""
        return self._account_locks.setdefault(str(account_id), asyncio.Lock())

    def start_scheduled_assignment(self, campaign_id: str, account_id: str,
                                   target_ids: list[str] | None = None) -> asyncio.Task | None:
        """Run one scheduled campaign/account worker using the shared execution path."""
        key = str(account_id)
        runtime = self._scheduled_runtimes.get(key)
        if runtime is not None and runtime.task is not None and not runtime.task.done():
            return None
        if self._is_standalone_scheduled(campaign_id):
            campaign = self.scheduled_promotion_model()
            if (campaign is None or key not in {str(value) for value in campaign.get("account_ids", [])}
                    or not campaign.get("target_ids")):
                return None
            for target_id in campaign["target_ids"]:
                target = self.targets.get(str(target_id)) or {}
                if (target.get("opted_in") is not True or not target.get("username")
                        or self._target_content_config(campaign, str(target_id)) is None):
                    return None
        else:
            campaign = self._validated_campaign(str(campaign_id))
        if campaign is None:
            return None
        if target_ids is not None:
            selected = {str(value) for value in target_ids}
            campaign = {**campaign, "target_ids": [value for value in campaign["target_ids"]
                                                   if str(value) in selected]}
            if not campaign["target_ids"]:
                return None
        runtime = _Runtime()
        runtime.scheduled_campaign_id = str(campaign_id)
        runtime.scheduled_target_ids = [str(value) for value in campaign["target_ids"]]
        runtime.assignment_id = uuid.uuid4().hex
        self._scheduled_runtimes[key] = runtime
        runtime.task = asyncio.create_task(
            self._run_scheduled_assignment(campaign, key, runtime),
            name=f"scheduled-campaign-{campaign_id}-{key}",
        )
        return runtime.task

    async def _run_scheduled_assignment(self, campaign: dict, account_id: str, runtime: _Runtime) -> None:
        campaign_id = str(campaign["id"])
        assignment_id = str(runtime.assignment_id)
        try:
            await self._ensure_tasks_async(campaign)
            tasks = await self._storage_io(
                self._scheduled_task_snapshot, campaign_id, account_id,
                runtime.scheduled_target_ids,
            )
            self._record_scheduled_event(
                "assignment_started", identity=f"assignment-start:{assignment_id}",
                assignment_id=assignment_id, account_id=account_id, campaign_id=campaign_id,
                tasks=tasks,
            )
            await self._run_account(campaign, account_id, runtime)
        except Exception as exc:
            self._record_scheduled_event(
                "scheduled_execution_error", identity=f"scheduled-execution-error:{assignment_id}",
                assignment_id=assignment_id, campaign_id=campaign_id, account_id=account_id,
                reason=type(exc).__name__,
            )
            raise
        finally:
            try:
                snapshot = await self._storage_io(
                    self._scheduled_task_snapshot, campaign_id, account_id,
                    runtime.scheduled_target_ids,
                )
                unfinished = any(
                    task.get("status") in {"pending", "in_progress", "suspended"}
                    and int(task.get("next_cycle", 1)) <= int(campaign.get("total_cycles", 1))
                    for task in snapshot
                )
                self._record_scheduled_event(
                    "assignment_finished", identity=f"assignment-finish:{assignment_id}",
                    assignment_id=assignment_id, account_id=account_id, campaign_id=campaign_id,
                    interrupted=bool(runtime.stop.is_set() and unfinished), tasks=snapshot,
                )
            except Exception as exc:
                logger.warning(
                    "Could not snapshot scheduled assignment completion (error=%s)",
                    type(exc).__name__,
                )
            finally:
                if self._scheduled_runtimes.get(account_id) is runtime:
                    self._scheduled_runtimes.pop(account_id, None)

    def _scheduled_task_snapshot(self, campaign_id: str, account_id: str,
                                 target_ids: list[str] | None = None) -> list[dict]:
        return [dict(task) for task in self.task_records(str(campaign_id))
                if str(task.get("account_id")) == str(account_id)
                and (target_ids is None or str(task.get("target_id")) in {str(value) for value in target_ids})]

    def stop_scheduled_assignments(self) -> None:
        """Stop workers at their next existing cycle boundary."""
        for account_id, runtime in tuple(self._scheduled_runtimes.items()):
            runtime.stop.set()
            self._pending_scheduled_stops[account_id] = runtime
        if self._pending_scheduled_stops and (
            self._scheduled_stop_persist_task is None
            or self._scheduled_stop_persist_task.done()
        ):
            self._scheduled_stop_persist_task = asyncio.create_task(
                self._persist_scheduled_stop_requests(),
                name="scheduled-stop-persistence",
            )

    async def _persist_scheduled_stop_requests(self) -> None:
        while self._pending_scheduled_stops:
            pending = list(self._pending_scheduled_stops.items())
            self._pending_scheduled_stops.clear()
            for account_id, runtime in pending:
                campaign_id = str(runtime.scheduled_campaign_id or "")
                try:
                    campaign = await self._storage_io(self.get, campaign_id) or {}
                    snapshot = await self._storage_io(
                        self._scheduled_task_snapshot, campaign_id, account_id,
                        runtime.scheduled_target_ids,
                    )
                    unfinished = any(
                        int(task.get("next_cycle", 1)) <= int(campaign.get("total_cycles", 1))
                        and task.get("status") != "restricted"
                        for task in snapshot
                    )
                    await self._storage_io(
                        self._record_scheduled_event,
                        "assignment_stop_requested",
                        identity=f"assignment-stop:{runtime.assignment_id}",
                        assignment_id=runtime.assignment_id,
                        campaign_id=campaign_id,
                        account_id=str(account_id),
                        unfinished=unfinished,
                        tasks=snapshot,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Could not persist scheduled stop request (error=%s)",
                        type(exc).__name__,
                    )

    def scheduled_assignment_active(self, account_id: str) -> bool:
        runtime = self._scheduled_runtimes.get(str(account_id))
        return bool(runtime and runtime.task and not runtime.task.done())

    async def claim_scheduled_account(self, account_id: str) -> dict:
        """Reserve an account, suspend its normal workers, and wait for lock release."""
        account_id = str(account_id)
        if self.scheduled_assignment_active(account_id):
            return {"ok": False, "suspended": []}
        self._scheduled_claims.add(account_id)
        for campaign_id, runtime in self._runtimes.items():
            campaign = self.get(campaign_id) or {}
            account_ids = campaign.get("account_ids") or ([campaign.get("account_id")] if campaign.get("account_id") else [])
            if account_id in {str(item) for item in account_ids}:
                runtime.suspend_accounts.add(account_id)
        # Let a just-started campaign coordinator register its per-account task.
        await asyncio.sleep(0)
        workers = list(self._normal_account_workers.get(account_id, {}).items())
        for _task, (_campaign_id, runtime) in workers:
            runtime.suspend_accounts.add(account_id)
        if workers:
            await asyncio.gather(*(task for task, _ in workers), return_exceptions=True)
        lock = self.account_execution_lock(account_id)
        if (lock.locked() or self.scheduled_assignment_active(account_id)
                or any(account_id in runtime.failed_suspensions for _, (_, runtime) in workers)):
            self._scheduled_claims.discard(account_id)
            return {"ok": False, "suspended": []}
        suspended = []
        seen = set()
        for _task, (campaign_id, _runtime) in workers:
            if campaign_id in seen:
                continue
            seen.add(campaign_id)
            campaign = self.get(campaign_id) or {}
            if account_id in campaign.get("suspended_accounts", []):
                suspended.append({"campaign": campaign, "account_id": account_id,
                                  "progress": self._account_progress(campaign_id, account_id)})
        return {"ok": True, "suspended": suspended}

    def release_scheduled_account_claim(self, account_id: str) -> None:
        self._scheduled_claims.discard(str(account_id))

    def release_scheduled_account_claims(self, keep: set[str] | None = None) -> None:
        keep = {str(item) for item in (keep or set())}
        self._scheduled_claims.intersection_update(keep)

    def scheduled_account_claimed(self, account_id: str) -> bool:
        return str(account_id) in self._scheduled_claims

    def _account_progress(self, campaign_id: str, account_id: str) -> dict:
        tasks = [task for task in self.task_records(campaign_id)
                 if str(task.get("account_id")) == str(account_id)]
        return {
            "successful": sum(int(task.get("successful", 0)) for task in tasks),
            "failed": sum(int(task.get("failed", 0)) for task in tasks),
            "restricted": sum(int(task.get("restricted", 0)) for task in tasks),
            "next_cycles": {str(task.get("target_id")): int(task.get("next_cycle", 1))
                            for task in tasks},
        }

    async def resume_suspended_account(self, campaign_id: str, account_id: str, callback=None) -> bool:
        """Manually resume one owner-selected suspended account worker."""
        campaign_id, account_id = str(campaign_id), str(account_id)
        campaign = await self._storage_io(self._validated_campaign, campaign_id)
        if campaign is None or campaign.get("status") not in {"suspended", "paused", "failed"}:
            return False
        lock = self.account_execution_lock(account_id)
        if lock.locked() or self.scheduled_account_claimed(account_id) or self.scheduled_assignment_active(account_id):
            return False
        tasks = [task for task in self.task_records(campaign_id)
                 if str(task.get("account_id")) == account_id]
        checkpoints = dict(campaign.get("normal_suspension_checkpoints") or {})
        saved_tasks = checkpoints.get(account_id) or []
        if not saved_tasks and not any(task.get("status") == "suspended" for task in tasks):
            return False
        async with self._guard:
            if lock.locked() or self.scheduled_account_claimed(account_id) or self.scheduled_assignment_active(account_id):
                return False
            if self._runtimes.get(campaign_id) is not None and not self._runtimes[campaign_id].task.done():
                return False
            restorations = []
            newly_restricted = []
            for saved in saved_tasks:
                if saved.get("status") == "restricted":
                    continue
                target_id = str(saved.get("target_id", ""))
                try:
                    restriction = self._task_restriction({"account_id": account_id, "target_id": target_id})
                except Exception:
                    return False
                if restriction is not None:
                    newly_restricted.append((target_id, restriction))
                    continue
                restored = {key: value for key, value in saved.items() if key != "id"}
                if restored.get("status") == "suspended":
                    restored["status"] = "pending"
                restored.update({"phase": "idle", "search_started": False, "updated_at": self._now()})
                restorations.append((str(saved["id"]), restored))
            remaining_suspended = [value for value in campaign.get("suspended_accounts", [])
                                    if str(value) != account_id]
            reasons = dict(campaign.get("suspension_reasons") or {})
            reasons.pop(account_id, None)
            checkpoints.pop(account_id, None)
            self._update_campaign(campaign_id, {
                "suspended_accounts": remaining_suspended,
                "suspension_reasons": reasons,
                "normal_suspension_checkpoints": checkpoints,
            })
            for target_id, restriction in newly_restricted:
                self._mark_target_restricted(campaign, account_id, target_id, restriction)
            for task_id, restored in restorations:
                self._task_store(campaign_id).update(task_id, restored)
            if not saved_tasks:
                for task in tasks:
                    if task.get("status") == "suspended":
                        self._task_store(campaign_id).update(task["id"], {"status": "pending", "phase": "idle"})
            runtime = _Runtime(callback=callback)
            self._runtimes[campaign_id] = runtime
            await self._refresh_progress_async(
                campaign_id, status="running", operation="manually resumed",
            )
            runtime.task = asyncio.create_task(
                self._run(campaign_id, runtime, account_ids=[account_id]),
                name=f"campaign-{campaign_id}-resume-{account_id}",
            )
            return True

    def reset_scheduled_account_round(self, campaign_id: str, account_id: str,
                                      target_ids: list[str] | None = None) -> None:
        """Make completed scheduled cycles repeat while retaining audit counters."""
        campaign = self.get(str(campaign_id))
        if campaign is None:
            return
        selected_targets = ({str(value) for value in target_ids}
                            if target_ids is not None else None)
        for target_id in campaign.get("target_ids", []):
            if selected_targets is not None and str(target_id) not in selected_targets:
                continue
            task = self._task_for(str(campaign_id), str(account_id), str(target_id))
            if task is None or task.get("status") == "restricted":
                continue
            if int(task.get("next_cycle", 1)) > int(campaign.get("total_cycles", 1)):
                self._task_store(str(campaign_id)).update(task["id"], {
                    "next_cycle": 1, "status": "pending", "phase": "idle",
                    "search_started": False, "last_error": None,
                    "updated_at": self._now(),
                })

    def task_records(self, campaign_id: str) -> list[dict]:
        return [item for item in self._task_store(str(campaign_id)).list()
                if item.get("campaign_id") == str(campaign_id)]

    def clear_detected_restriction(self, account_id: str, target_id: str) -> None:
        """Clear the in-memory detector latch after the owner clears its record."""

        key = (str(account_id), str(target_id))
        self.ban_detection.clear_result(*key)
        self._ban_events[key] = asyncio.Event()
        if self.restriction_service is not None:
            self.restriction_service.clear_cached(*key)

    async def recover_interrupted(self) -> None:
        """Fail uncertain in-flight operations rather than replaying them after restart."""
        records = list(self.campaigns.list())
        standalone = self.scheduled_promotion_model()
        if standalone is not None:
            records.append(standalone)
        for record in records:
            campaign_id = str(record.get("id", ""))
            campaign_running = record.get("status") == "running"
            records = self.task_records(campaign_id)
            interrupted = [
                task for task in records
                if (
                    task.get("status") in {"verification_paused", "verification_failed"}
                    or (
                        task.get("phase") not in {None, "idle"}
                        and task.get("status") in {"pending", "in_progress", "suspended"}
                    )
                )
            ]
            # Scheduled workers do not own the campaign-level manual status. Recover
            # their persisted task checkpoints too, or an uncertain Telegram operation
            # could be replayed when the owner later confirms scheduler recovery.
            if not campaign_running and not interrupted:
                continue
            campaign = (record if self._is_standalone_scheduled(campaign_id)
                        else self._validated_campaign(campaign_id) or record)
            if campaign_running and campaign.get("account_ids") and campaign.get("target_ids"):
                self._ensure_tasks(campaign)
            work = self.task_records(campaign_id) if campaign_running else interrupted
            for task in work:
                try:
                    restriction = self._task_restriction(task)
                except Exception as exc:
                    logger.warning("Could not check restriction during campaign recovery (error=%s)", type(exc).__name__)
                    self._fail_target_remaining(
                        campaign, str(task.get("account_id", "")), str(task.get("target_id", "")),
                        "restriction_store_unavailable",
                    )
                    continue
                if restriction is not None:
                    self._mark_target_restricted(
                        campaign, str(task.get("account_id", "")), str(task.get("target_id", "")),
                        restriction,
                    )
                    continue
                phase = task.get("phase")
                if phase in {"preparing", "opening"}:
                    # No Telegram operation has started in these phases. Keep
                    # the cycle number so an explicit resume can safely retry it.
                    self._task_store(campaign_id).update(task["id"], {
                        "phase": "idle", "current_cycle": None,
                        "search_started": False,
                        "last_error": "interrupted_before_send",
                        "updated_at": self._now(),
                    })
                elif phase not in {None, "idle"}:
                    if task.get("search_started") is True:
                        try:
                            await self._recovery_stop(task)
                        except Exception as exc:
                            logger.warning("Campaign restart cleanup failed (error=%s)", type(exc).__name__)
                    self._finish_task(task, success=False, error="interrupted_operation_not_replayed")
            if campaign_running:
                await self._refresh_progress_async(
                    campaign_id, status="paused", operation="paused after restart",
                )
    async def start(self, campaign_id: str, callback=None) -> bool:
        async with self._guard:
            campaign = await self._storage_io(self._validated_campaign, campaign_id)
            if campaign is None or campaign.get("status") in {"running", "completed", "suspended"}:
                return False
            runtime = self._runtimes.get(campaign_id)
            if runtime is not None and runtime.task is not None and not runtime.task.done():
                return False
            runtime = _Runtime(callback=callback)
            self._runtimes[campaign_id] = runtime
            await self._storage_io(self._ensure_tasks, campaign)
            await self._refresh_progress_async(
                campaign_id, status="running", operation="starting",
            )
            runtime.task = asyncio.create_task(self._run(campaign_id, runtime), name=f"campaign-{campaign_id}")
            return True

    async def pause(self, campaign_id: str) -> bool:
        runtime = self._runtimes.get(campaign_id)
        campaign = self.get(campaign_id)
        if runtime is None or runtime.task is None or campaign is None or campaign.get("status") != "running":
            return False
        runtime.pause.set()
        await self._storage_io(self._update_campaign, campaign_id, {"pause_requested": True})
        return True

    async def resume(self, campaign_id: str, callback=None) -> bool:
        campaign = self.get(campaign_id)
        if campaign is None or campaign.get("status") not in {"paused", "failed", "stopped"}:
            return False
        return await self.start(campaign_id, callback)

    async def stop(self, campaign_id: str) -> bool:
        runtime = self._runtimes.get(campaign_id)
        campaign = self.get(campaign_id)
        if campaign is None or campaign.get("status") not in {"running", "paused"}:
            return False
        if runtime is None or runtime.task is None:
            await self._refresh_progress_async(
                campaign_id, status="stopped", operation="stopped",
            )
            return True
        runtime.stop.set()
        runtime.pause.clear()
        await self._storage_io(self._update_campaign, campaign_id, {"stop_requested": True})
        return True

    async def shutdown(self) -> None:
        for runtime in self._runtimes.values():
            if runtime.task and not runtime.task.done():
                runtime.stop.set()
        tasks = [runtime.task for runtime in self._runtimes.values() if runtime.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run(self, campaign_id: str, runtime: _Runtime, account_ids: list[str] | None = None) -> None:
        ticker: asyncio.Task | None = None
        try:
            campaign = await self._storage_io(self._validated_campaign, campaign_id)
            if campaign is None:
                raise RuntimeError("Campaign selections are no longer valid")
            ticker = asyncio.create_task(
                self._progress_ticker(campaign_id, runtime),
                name=f"campaign-progress-{campaign_id}",
            )
            account_ids = [str(item) for item in (account_ids or campaign["account_ids"])]
            if campaign.get("execution_mode", "sequential") == "sequential":
                for target_id in campaign["target_ids"]:
                    if runtime.stop.is_set() or runtime.pause.is_set():
                        break
                    results = await asyncio.gather(*(
                        self._run_normal_account(
                            campaign, account_id, runtime, [str(target_id)],
                        )
                        for account_id in account_ids
                    ), return_exceptions=True)
                    for account_id, result in zip(account_ids, results):
                        if isinstance(result, Exception):
                            logger.warning(
                                "Campaign account worker failed independently (error=%s)",
                                type(result).__name__,
                            )
                            self._fail_account_remaining(campaign, account_id, type(result).__name__)
            else:
                results = await asyncio.gather(*(
                    self._run_normal_account(campaign, account_id, runtime)
                    for account_id in account_ids
                ), return_exceptions=True)
                for account_id, result in zip(account_ids, results):
                    if isinstance(result, Exception):
                        logger.warning("Campaign account worker failed independently (error=%s)", type(result).__name__)
                        self._fail_account_remaining(campaign, account_id, type(result).__name__)
            latest = await self._get_async(campaign_id) or {}
            if runtime.stop.is_set():
                status, operation = "stopped", "stopped"
            elif runtime.pause.is_set():
                status, operation = "paused", "paused"
            elif runtime.failed_suspensions:
                status, operation = "paused", "suspension could not reach a safe checkpoint"
            elif runtime.suspended_accounts or (latest.get("suspended_accounts") or []):
                status, operation = "suspended", "suspended for scheduled priority"
            else:
                status, operation = "completed", "complete"
            await self._refresh_progress_async(campaign_id, status=status, operation=operation)
            await self._notify(runtime, await self._get_async(campaign_id))
        except asyncio.CancelledError:
            await self._refresh_progress_async(
                campaign_id, status="paused", operation="interrupted",
            )
            raise
        except Exception as exc:
            logger.warning("Campaign coordinator failed (campaign_id=%s, error=%s)", campaign_id, type(exc).__name__)
            await self._refresh_progress_async(
                campaign_id, status="failed", operation="failed",
                last_error=type(exc).__name__,
            )
            await self._notify(runtime, await self._get_async(campaign_id))
        finally:
            if ticker is not None:
                ticker.cancel()
                await asyncio.gather(ticker, return_exceptions=True)
            self._runtimes.pop(campaign_id, None)

    async def _run_normal_account(
        self, campaign: dict, account_id: str, runtime: _Runtime,
        target_ids: list[str] | None = None,
    ) -> None:
        task = asyncio.current_task()
        workers = self._normal_account_workers.setdefault(str(account_id), {})
        if task is not None:
            workers[task] = (str(campaign["id"]), runtime)
        if self.scheduled_account_claimed(account_id):
            runtime.suspend_accounts.add(str(account_id))
        try:
            await self._run_account(
                campaign, str(account_id), runtime, selected_target_ids=target_ids,
            )
        finally:
            if task is not None:
                workers.pop(task, None)
            if not workers:
                self._normal_account_workers.pop(str(account_id), None)

    def _normal_suspension_requested(self, runtime: _Runtime, account_id: str) -> bool:
        if self._scheduled_runtimes.get(str(account_id)) is runtime:
            return False
        return str(account_id) in runtime.suspend_accounts or self.scheduled_account_claimed(account_id)

    def _suspend_normal_account(self, campaign: dict, account_id: str, runtime: _Runtime) -> bool:
        account_id = str(account_id)
        current = self.get(str(campaign["id"])) or campaign
        active_tasks = [task for task in self.task_records(str(campaign["id"]))
                        if str(task.get("account_id")) == account_id
                        and task.get("status") != "restricted"
                        and int(task.get("next_cycle", 1)) <= int(campaign.get("total_cycles", 1))]
        if any(task.get("phase") not in {None, "idle"} for task in active_tasks):
            logger.error("Refusing unsafe campaign suspension at non-idle checkpoint (campaign_id=%s)", campaign["id"])
            runtime.failed_suspensions.add(account_id)
            return False
        if not active_tasks:
            return True
        runtime.suspended_accounts.add(account_id)
        suspended_accounts = {str(value) for value in current.get("suspended_accounts", [])}
        suspended_accounts.add(account_id)
        reasons = dict(current.get("suspension_reasons") or {})
        reasons[account_id] = "Scheduled campaign priority requires this account"
        checkpoints = dict(current.get("normal_suspension_checkpoints") or {})
        checkpoints[account_id] = [dict(task) for task in self.task_records(str(campaign["id"]))
                                   if str(task.get("account_id")) == account_id]
        for task in self.task_records(str(campaign["id"])):
            if str(task.get("account_id")) != account_id or task.get("status") == "restricted":
                continue
            if int(task.get("next_cycle", 1)) > int(campaign.get("total_cycles", 1)):
                continue
            # Suspension is only called at account entry or between whole cycles.
            self._task_store(str(campaign["id"])).update(task["id"], {
                "status": "suspended", "phase": "idle", "search_started": False,
                "suspension_reason": reasons[account_id], "updated_at": self._now(),
            })
        self._update_campaign(str(campaign["id"]), {
            "suspended_accounts": sorted(suspended_accounts),
            "suspension_reasons": reasons,
            "normal_suspension_checkpoints": checkpoints,
        })
        return True

    async def _run_account(
        self, campaign: dict, account_id: str, runtime: _Runtime,
        selected_target_ids: list[str] | None = None, lock_held: bool = False,
    ) -> None:
        account_lock = self.account_execution_lock(account_id)
        async with _execution_lock(account_lock, not lock_held):
            target_ids = (
                [str(value) for value in selected_target_ids]
                if selected_target_ids is not None
                else [str(value) for value in campaign["target_ids"]]
            )
            if (
                not lock_held
                and campaign.get("execution_mode") == "parallel"
                and len(target_ids) > 1
            ):
                try:
                    concurrency_limit = int(campaign.get("concurrency_limit", 3))
                except (TypeError, ValueError):
                    concurrency_limit = 1
                if self._scheduled_promotion_repository is not None:
                    configured = self._scheduled_promotion_repository.get_config().get(
                        "parallel_target_concurrency", 3,
                    )
                    try:
                        concurrency_limit = min(concurrency_limit, int(configured))
                    except (TypeError, ValueError):
                        concurrency_limit = 1
                concurrency_limit = max(1, min(5, concurrency_limit))
                semaphore = asyncio.Semaphore(concurrency_limit)

                async def run_target(target_id: str) -> None:
                    async with semaphore:
                        await self._run_account(
                            campaign, account_id, runtime, [target_id], lock_held=True,
                        )

                await asyncio.gather(*(run_target(target_id) for target_id in target_ids))
                return
            if self._normal_suspension_requested(runtime, account_id):
                self._suspend_normal_account(campaign, account_id, runtime)
                return
            eligible_targets: list[tuple[str, dict]] = []
            for target_id in target_ids:
                if self._normal_suspension_requested(runtime, account_id):
                    self._suspend_normal_account(campaign, account_id, runtime)
                    return
                task = await self._task_for_async(campaign["id"], account_id, target_id)
                if task is None:
                    task = await self._set_task_async(
                        campaign, account_id, target_id, {"phase": "idle"},
                    )
                if task.get("status") == "restricted":
                    continue
                target = self.targets.get(target_id)
                if target is None or target.get("opted_in") is not True:
                    self._fail_target_remaining(campaign, account_id, target_id, "target_not_opted_in")
                    continue
                try:
                    restriction = await self._restriction_for_async(
                        account_id, target_id, target,
                    )
                except Exception as exc:
                    logger.warning("Could not check account-target restriction (error=%s)", type(exc).__name__)
                    self._fail_target_remaining(campaign, account_id, target_id, "restriction_store_unavailable")
                    # An unavailable restriction store makes eligibility unknown.
                    for remaining_id in target_ids:
                        if remaining_id != target_id:
                            self._fail_target_remaining(campaign, account_id, remaining_id, "restriction_store_unavailable")
                    return
                if restriction is not None:
                    self._mark_target_restricted(campaign, account_id, target_id, restriction)
                    continue
                detected = self.ban_detection.result_for(account_id, target_id)
                if detected is not None:
                    self._mark_target_restricted(campaign, account_id, target_id, detected)
                    continue
                if int(task.get("next_cycle", 1)) > int(campaign.get("total_cycles", 1)):
                    continue
                daily_limit = (
                    self.daily_limit_service.active_for(
                        target.get("telegram_user_id"), str(target.get("username", "")),
                    )
                    if self.daily_limit_service is not None else None
                )
                if daily_limit is not None:
                    await self._mark_daily_limit_task_async(task, daily_limit)
                    continue
                eligible_targets.append((target_id, target))
            if not eligible_targets:
                return

            account = self.accounts.get(account_id)
            if account is None or account.get("authenticated") is not True:
                self._fail_account_remaining(campaign, account_id, "account_not_authenticated")
                return
            try:
                if not await self.client_manager.is_connected(account_id):
                    if not await self.client_manager.connect(account_id):
                        self._fail_account_remaining(campaign, account_id, "account_connection_failed")
                        return
                client = self.client_manager.get_client(account_id)
                if client is None or not await self.client_manager.is_authorized(account_id):
                    self._fail_account_remaining(campaign, account_id, "account_not_authorized")
                    return
            except Exception as exc:
                self._fail_account_remaining(campaign, account_id, type(exc).__name__)
                return

            for target_id, target in eligible_targets:
                if runtime.stop.is_set() or runtime.pause.is_set():
                    return
                task = await self._task_for_async(campaign["id"], account_id, target_id)
                if task is None or task.get("status") == "restricted":
                    continue
                try:
                    restriction = await self._restriction_for_async(
                        account_id, target_id, target,
                    )
                except Exception:
                    self._fail_target_remaining(campaign, account_id, target_id, "restriction_store_unavailable")
                    continue
                if restriction is not None:
                    self._mark_target_restricted(campaign, account_id, target_id, restriction)
                    continue
                detected = self.ban_detection.result_for(account_id, target_id)
                if detected is not None:
                    self._mark_target_restricted(campaign, account_id, target_id, detected)
                    continue
                try:
                    entity = await client.get_entity(target["username"])
                except Exception as exc:
                    self._fail_target_remaining(campaign, account_id, target_id, type(exc).__name__)
                    continue
                if getattr(entity, "bot", None) is not True:
                    self._fail_target_remaining(campaign, account_id, target_id, "target_is_not_a_bot")
                    continue
                bot_id = int(getattr(entity, "id"))
                if target.get("telegram_user_id") != bot_id:
                    target = self.targets.update(target_id, {"telegram_user_id": bot_id})
                restriction = await self._restriction_for_async(
                    account_id, target_id, target, target_bot_id=bot_id,
                )
                if restriction is not None:
                    self._mark_target_restricted(campaign, account_id, target_id, restriction)
                    continue

                recovery_task = await self._task_for_async(campaign["id"], account_id, target_id)
                if recovery_task is not None and recovery_task.get("status") == "verification_paused":
                    self.ban_detection.watch_target(
                        account_id, target_id, str(target["username"]), client, entity,
                        self._signal_ban, campaign_id=str(campaign["id"]),
                        on_verification=self._handle_ordinary_verification,
                        on_confirmation=self._handle_ordinary_verification_confirmation,
                        on_daily_limit=self._signal_daily_limit,
                        account_name=str(account.get("name") or account.get("phone") or "Unnamed account"),
                    )
                    try:
                        await self._wait_for_verification_resolution(
                            str(campaign["id"]), account_id, target_id,
                        )
                    finally:
                        self.ban_detection.stop_watching_target(client, entity)
                    recovery_task = (
                        await self._task_for_async(campaign["id"], account_id, target_id)
                    ) or recovery_task
                    if recovery_task.get("status") in {"verification_paused", "verification_failed"}:
                        continue

                for cycle in range(int(task.get("next_cycle", 1)), int(campaign["total_cycles"]) + 1):
                    if self._normal_suspension_requested(runtime, account_id):
                        self._suspend_normal_account(campaign, account_id, runtime)
                        return
                    if runtime.stop.is_set() or runtime.pause.is_set():
                        return
                    task = (await self._task_for_async(
                        campaign["id"], account_id, target_id,
                    )) or task
                    if task.get("status") in {"restricted", "verification_paused", "verification_failed"}:
                        break
                    detected = self.ban_detection.result_for(account_id, target_id)
                    if detected is not None:
                        self._mark_target_restricted(campaign, account_id, target_id, detected)
                        break
                    # Record which cycle is being prepared before reconnecting.
                    # If that reconnect fails, the failure is attributed to this
                    # cycle instead of the previous completed one.
                    await self._set_task_async(campaign, account_id, target_id, {
                        "phase": "preparing", "current_cycle": cycle,
                        "search_started": False,
                    })
                    if not await self.client_manager.is_connected(account_id):
                        try:
                            if not await self.client_manager.connect(account_id):
                                await self._finish_task_async(
                                    task, success=False, error="account_disconnected",
                                )
                                continue
                            client = self.client_manager.get_client(account_id)
                            entity = await client.get_entity(target["username"])
                            if getattr(entity, "bot", None) is not True:
                                self._fail_target_remaining(campaign, account_id, target_id, "target_is_not_a_bot")
                                break
                            bot_id = int(getattr(entity, "id"))
                            if target.get("telegram_user_id") != bot_id:
                                target = self.targets.update(target_id, {"telegram_user_id": bot_id})
                        except Exception as exc:
                            await self._finish_task_async(
                                task, success=False, error=type(exc).__name__,
                            )
                            continue
                    restriction = await self._restriction_for_async(
                        account_id, target_id, target, target_bot_id=bot_id,
                    )
                    if restriction is not None:
                        self._mark_target_restricted(campaign, account_id, target_id, restriction)
                        break
                    self.ban_detection.watch_target(
                        account_id, target_id, str(target["username"]), client, entity,
                        self._signal_ban, campaign_id=str(campaign["id"]),
                        on_verification=self._handle_ordinary_verification,
                        on_confirmation=self._handle_ordinary_verification_confirmation,
                        on_daily_limit=self._signal_daily_limit,
                        account_name=str(account.get("name") or account.get("phone") or "Unnamed account"),
                    )
                    try:
                        await self._execute_cycle(campaign, task, client, entity, cycle, runtime)
                    finally:
                        self.ban_detection.stop_watching_target(client, entity)
                if self._normal_suspension_requested(runtime, account_id):
                    self._suspend_normal_account(campaign, account_id, runtime)
                    return

    async def _execute_cycle(self, campaign: dict, task: dict, client, entity, cycle: int, runtime: _Runtime) -> bool:
        campaign_id = campaign["id"]
        account_id = task["account_id"]
        target_id = task["target_id"]
        content = await self._target_content_config_async(campaign, str(target_id))
        if content is None:
            self._fail_target_remaining(campaign, account_id, target_id, "campaign_content_unavailable")
            return False
        ban_event = self._ban_events.setdefault((str(account_id), str(target_id)), asyncio.Event())
        verification_event = self._verification_events.setdefault(
            (str(campaign_id), str(account_id), str(target_id)), asyncio.Event()
        )
        target = await self._storage_io(self.targets.get, str(target_id)) or {}
        username = str(target.get("username", "unknown"))
        target_bot_id = getattr(entity, "id", target.get("telegram_user_id"))
        daily_event = self._daily_limit_event(target_bot_id, username)
        await self._set_current_async(campaign_id, account_id, target_id, cycle, "searching")
        current_task = await self._task_for_async(campaign_id, account_id, target_id) or task
        resume_phase = str(current_task.get("verification_resume_phase") or "search")
        try:
            await self._raise_if_daily_limited_async(target_bot_id, username)
            await self._raise_if_target_opted_out_async(target_id)
            await self._raise_if_banned_async(account_id, target_id)
            self._raise_if_verification_paused(account_id, target_id, campaign_id)

            if resume_phase in {"search", "opening", "idle"}:
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "opening", "current_cycle": cycle, "search_started": False,
                    "verification_resume_phase": "search",
                })
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "search_pending", "search_started": True,
                })
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_target_opted_out_async(target_id)
                await client.send_message(entity, "/search")
                self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "search")
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_banned_async(account_id, target_id)
                self._raise_if_verification_paused(account_id, target_id, campaign_id)
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "after_search", "search_started": True,
                    "verification_resume_phase": (
                        "sticker" if content["sticker_enabled"] else
                        "message" if content["text_enabled"] else "stop"
                    ),
                })
                await self._set_current_async(campaign_id, account_id, target_id, cycle, "waiting")
                await self._wait_for_cycle(
                    self.timing.AFTER_SEARCH_SECONDS, ban_event, daily_event, verification_event,
                    account_id, target_id, campaign_id,
                )

            if content["sticker_enabled"] and resume_phase in {"search", "opening", "idle", "sticker"}:
                sticker_path = self._sticker_path(str(content["sticker_id"]))
                await self._set_current_async(
                    campaign_id, account_id, target_id, cycle, "sending sticker",
                )
                await self._set_task_async(
                    campaign, account_id, target_id,
                    {"phase": "sticker_pending", "search_started": True},
                )
                sticker = await self._storage_io(
                    self.stickers.get, str(content["sticker_id"]),
                ) or {}
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_target_opted_out_async(target_id)
                await client.send_file(
                    entity, str(sticker_path),
                    attributes=self._sticker_attributes(sticker, sticker_path),
                )
                self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "sticker")
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_banned_async(account_id, target_id)
                self._raise_if_verification_paused(account_id, target_id, campaign_id)
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "after_sticker", "search_started": True,
                    "verification_resume_phase": "message" if content["text_enabled"] else "stop",
                })
                await self._set_current_async(campaign_id, account_id, target_id, cycle, "waiting")
                await self._wait_for_cycle(
                    self.timing.AFTER_STICKER_SECONDS, ban_event, daily_event, verification_event,
                    account_id, target_id, campaign_id,
                )

            if content["text_enabled"] and resume_phase in {"search", "opening", "idle", "sticker", "message"}:
                if content.get("legacy_rotating_text") is True:
                    rotation_index, promotion_text = await self._reserve_rotation(campaign_id)
                else:
                    rotation_index = None
                    promotion_text = str(content["promotional_text"])
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "message_pending", "rotation_index": rotation_index,
                    "search_started": True,
                })
                await self._set_current_async(
                    campaign_id, account_id, target_id, cycle, "sending message",
                )
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_target_opted_out_async(target_id)
                await client.send_message(entity, promotion_text)
                self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "promotion_message")
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_banned_async(account_id, target_id)
                self._raise_if_verification_paused(account_id, target_id, campaign_id)
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "after_message", "search_started": True,
                    "verification_resume_phase": "stop",
                })
                await self._set_current_async(campaign_id, account_id, target_id, cycle, "waiting")
                await self._wait_for_cycle(
                    self.timing.AFTER_TEXT_SECONDS, ban_event, daily_event, verification_event,
                    account_id, target_id, campaign_id,
                )

            if resume_phase in {"search", "opening", "idle", "sticker", "message", "stop"}:
                await self._set_current_async(
                    campaign_id, account_id, target_id, cycle, "stopping",
                )
                await self._set_task_async(
                    campaign, account_id, target_id,
                    {"phase": "stop_pending", "search_started": True},
                )
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_target_opted_out_async(target_id)
                await client.send_message(entity, "/stop")
                self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "stop")
                await self._raise_if_daily_limited_async(target_bot_id, username)
                await self._raise_if_banned_async(account_id, target_id)
                self._raise_if_verification_paused(account_id, target_id, campaign_id)
                await self._set_task_async(campaign, account_id, target_id, {
                    "phase": "after_stop", "search_started": False,
                    "verification_resume_phase": "complete",
                })
                await self._set_current_async(campaign_id, account_id, target_id, cycle, "waiting")
                await self._wait_for_cycle(
                    self.timing.AFTER_STOP_SECONDS, ban_event, daily_event, verification_event,
                    account_id, target_id, campaign_id,
                )
            await self._finish_task_async(task, success=True)
            await self._notify(runtime, await self._get_async(campaign_id))
            return True
        except _TargetDailyLimitReached:
            await self._await_daily_limit_persistence(target_bot_id, username)
            limit_record = (
                self.daily_limit_service.active_for(target_bot_id, username)
                if self.daily_limit_service is not None else None
            )
            if limit_record is not None:
                try:
                    await self._mark_daily_limit_task_async(task, limit_record)
                except Exception as exc:
                    logger.exception(
                        "Could not checkpoint daily-limited campaign task (account_id=%s, target=%s, error=%s)",
                        account_id, username, type(exc).__name__,
                    )
            await self._notify(runtime, await self._get_async(campaign_id))
            return False
        except _TargetOptedOut:
            self._fail_target_remaining(campaign, account_id, target_id, "target_opted_out")
            await self._notify(runtime, await self._get_async(campaign_id))
            return False
        except _VerificationPaused:
            confirmed = await self._wait_for_verification_resolution(
                campaign_id, account_id, target_id,
            )
            if confirmed:
                return await self._execute_cycle(
                    campaign, task, client, entity, cycle, runtime,
                )
            await self._notify(runtime, await self._get_async(campaign_id))
            return False
        except _TargetAccessDenied:
            await self._await_restriction_persistence(account_id, target_id)
            restriction = self.ban_detection.result_for(account_id, target_id)
            if restriction is None:
                try:
                    restriction = self._task_restriction({"account_id": account_id, "target_id": target_id})
                except Exception:
                    restriction = None
            self._mark_target_restricted(campaign, account_id, target_id, restriction)
            await self._notify(runtime, await self._get_async(campaign_id))
            return False
        except _RestrictionCheckUnavailable:
            self._fail_target_remaining(campaign, account_id, target_id, "restriction_store_unavailable")
            await self._notify(runtime, await self._get_async(campaign_id))
            return False
        except FloodWaitError as exc:
            if self.ban_detection.result_for(account_id, target_id) is not None:
                await self._await_restriction_persistence(account_id, target_id)
                self._mark_target_restricted(
                    campaign, account_id, target_id, self.ban_detection.result_for(account_id, target_id)
                )
                await self._notify(runtime, await self._get_async(campaign_id))
                return False
            current_task = await self._task_for_async(campaign_id, account_id, target_id) or task
            if (
                current_task.get("search_started") is True
                and await self._storage_io(self._is_target_opted_in, target_id)
            ):
                await self._send_stop(client, entity)
            wait_seconds = max(1, int(exc.seconds))
            wait_id = None
            if self._is_scheduled_pair(campaign_id, account_id):
                wait_id = (f"flood-wait:{self._scheduled_runtimes[account_id].assignment_id}:"
                           f"{target_id}:{cycle}")
                self._record_scheduled_event(
                    "flood_wait", identity=wait_id,
                    campaign_id=campaign_id, account_id=account_id, target_id=target_id,
                    wait_id=wait_id, seconds=wait_seconds,
                )
            await self._set_task_async(
                campaign, account_id, target_id, {"flood_wait_seconds": wait_seconds},
            )
            daily_limit = await self._wait_for_daily_limit(
                wait_seconds, daily_event, target_bot_id, username,
            )
            if daily_limit is not None:
                await self._await_daily_limit_persistence(target_bot_id, username)
                latest_limit = self.daily_limit_service.active_for(
                    target_bot_id, username,
                ) if self.daily_limit_service is not None else daily_limit
                if latest_limit is not None:
                    await self._mark_daily_limit_task_async(task, latest_limit)
                await self._notify(runtime, await self._get_async(campaign_id))
                return False
            if wait_id is not None:
                self._record_scheduled_event(
                    "flood_wait_completed", identity=f"{wait_id}:completed",
                    wait_id=wait_id, assignment_id=self._scheduled_runtimes[account_id].assignment_id,
                    campaign_id=campaign_id, account_id=account_id, target_id=target_id,
                )
            await self._finish_task_async(task, success=False, error="FloodWaitError")
            await self._notify(runtime, await self._get_async(campaign_id))
            logger.warning("Campaign account is waiting for Telegram FloodWait (%s seconds)", wait_seconds)
            return False
        except Exception as exc:
            if self.ban_detection.result_for(account_id, target_id) is not None:
                await self._await_restriction_persistence(account_id, target_id)
                self._mark_target_restricted(
                    campaign, account_id, target_id, self.ban_detection.result_for(account_id, target_id)
                )
                await self._notify(runtime, await self._get_async(campaign_id))
                return False
            current_task = await self._task_for_async(campaign_id, account_id, target_id) or task
            if (
                current_task.get("search_started") is True
                and await self._storage_io(self._is_target_opted_in, target_id)
            ):
                await self._send_stop(client, entity)
            logger.warning("Campaign target cycle failed (account_id=%s, target=%s, error=%s)", account_id, username, type(exc).__name__)
            await self._finish_task_async(task, success=False, error=type(exc).__name__)
            await self._notify(runtime, await self._get_async(campaign_id))
            return False
    async def _recovery_stop(self, task: dict) -> None:
        account_id = str(task.get("account_id", ""))
        target_id = str(task.get("target_id", ""))
        try:
            if not self._is_target_opted_in(target_id):
                return
            if self._task_restriction({"account_id": account_id, "target_id": target_id}) is not None:
                return
        except Exception:
            # Fail closed: cleanup is an operation against the target too.
            return
        target = self.targets.get(target_id)
        client = self.client_manager.get_client(account_id)
        if target is None:
            return
        if client is None or not client.is_connected():
            if not await self.client_manager.connect(account_id):
                return
            client = self.client_manager.get_client(account_id)
        if client is None or not client.is_connected():
            return
        await self._send_stop(client, await client.get_entity(target["username"]))

    async def _send_stop(self, client, entity) -> None:
        try:
            await client.send_message(entity, "/stop")
        except FloodWaitError as exc:
            logger.warning("Campaign cleanup /stop was rate limited (%s seconds)", int(exc.seconds))
        except Exception as exc:
            logger.warning("Campaign cleanup /stop failed (error=%s)", type(exc).__name__)

    def _task_id(self, campaign_id: str, account_id: str, target_id: str) -> str:
        return f"{campaign_id}:{account_id}:{target_id}"

    def _task_for(self, campaign_id: str, account_id: str, target_id: str) -> dict | None:
        return self._task_store(campaign_id).get(self._task_id(campaign_id, account_id, target_id))

    async def _storage_io(self, func, *args, **kwargs):
        """Bound synchronous JSON lock/read/write work away from the event loop."""
        async with self._storage_io_slots:
            return await asyncio.to_thread(func, *args, **kwargs)

    async def _get_async(self, campaign_id: str) -> dict | None:
        return await self._storage_io(self.get, campaign_id)

    async def _task_for_async(
        self, campaign_id: str, account_id: str, target_id: str,
    ) -> dict | None:
        return await self._storage_io(self._task_for, campaign_id, account_id, target_id)

    async def _set_task_async(
        self, campaign: dict, account_id: str, target_id: str, values: dict,
    ) -> dict:
        async with self._campaign_state_lock:
            return await self._storage_io(
                self._set_task, campaign, account_id, target_id, values,
            )

    async def _ensure_tasks_async(self, campaign: dict) -> None:
        async with self._campaign_state_lock:
            await self._storage_io(self._ensure_tasks, campaign)

    async def _refresh_progress_async(self, campaign_id: str, **kwargs) -> dict:
        async with self._campaign_state_lock:
            return await self._storage_io(
                self._refresh_progress, campaign_id, **kwargs,
            )

    async def _set_current_async(
        self, campaign_id: str, account_id: str, target_id: str,
        cycle: int, operation: str,
    ) -> None:
        await self._storage_io(
            self._set_current, campaign_id, account_id, target_id, cycle, operation,
        )

    async def _restriction_for_async(
        self, account_id: str, target_id: str, target: dict, *, target_bot_id=None,
    ) -> dict | None:
        return await self._storage_io(
            self._restriction_for, account_id, target_id, target,
            target_bot_id=target_bot_id,
        )

    async def _target_content_config_async(
        self, campaign: dict, target_id: str,
    ) -> dict | None:
        return await self._storage_io(
            self._target_content_config, campaign, target_id,
        )

    async def _raise_if_target_opted_out_async(self, target_id: str) -> None:
        if not await self._storage_io(self._is_target_opted_in, target_id):
            raise _TargetOptedOut

    async def _raise_if_banned_async(self, account_id: str, target_id: str) -> None:
        if self.ban_detection.result_for(account_id, target_id) is not None:
            raise _TargetAccessDenied
        target = await self._storage_io(self.targets.get, str(target_id)) or {}
        try:
            restricted = await self._restriction_for_async(account_id, target_id, target)
        except Exception as exc:
            raise _RestrictionCheckUnavailable from exc
        if restricted is not None:
            raise _TargetAccessDenied

    def _ensure_tasks(self, campaign: dict) -> None:
        existing = self.task_records(campaign["id"])
        progress = campaign.get("progress") or {}
        legacy_completed = int(progress.get("completed", 0)) if not existing else 0
        legacy_successful = int(progress.get("successful", 0)) if not existing else 0
        remaining_completed = max(0, legacy_completed)
        remaining_successful = max(0, min(legacy_successful, remaining_completed))
        cycles = int(campaign["total_cycles"])
        for account_index, account_id in enumerate(campaign["account_ids"]):
            for target_id in campaign["target_ids"]:
                task_id = self._task_id(campaign["id"], account_id, target_id)
                if self._task_store(campaign["id"]).get(task_id) is None:
                    # Legacy campaign progress was aggregate-only. Distribute it
                    # in execution order so a migration never replays recorded
                    # cycles when resumable task records are first introduced.
                    completed_here = min(cycles, remaining_completed) if account_index == 0 else 0
                    succeeded_here = min(completed_here, remaining_successful)
                    restriction = None
                    restriction_error = False
                    try:
                        restriction = self._task_restriction({"account_id": account_id, "target_id": target_id})
                    except Exception as exc:
                        logger.warning("Could not check restriction while assigning campaign task (error=%s)", type(exc).__name__)
                        restriction_error = True
                    remaining_for_task = (1 if self._is_standalone_scheduled(str(campaign["id"]))
                                          else max(0, cycles - completed_here))
                    restricted_cycles = remaining_for_task if restriction is not None else 0
                    failed_here = completed_here - succeeded_here
                    task_status = "restricted" if restriction is not None else ("blocked" if restriction_error else "pending")
                    self._task_store(campaign["id"]).create({
                        "id": task_id, "campaign_id": campaign["id"],
                        "account_id": account_id, "target_id": target_id,
                        "next_cycle": cycles + 1 if restriction is not None or restriction_error else 1 + completed_here,
                        "successful": succeeded_here,
                        "failed": failed_here + (remaining_for_task if restriction_error else 0),
                        "restricted": restricted_cycles,
                        "status": task_status,
                        "restriction_reason": (
                            str(restriction.get("detection_reason", "Active account-target restriction"))
                            if isinstance(restriction, dict) else
                            "Restriction status could not be verified" if restriction_error else None
                        ),
                        "phase": "idle", "search_started": False,
                    })
                    if account_index == 0:
                        remaining_completed -= completed_here
                        remaining_successful -= succeeded_here
        if self._is_standalone_scheduled(str(campaign["id"])):
            for task in self.task_records(str(campaign["id"])):
                if task.get("status") != "restricted":
                    continue
                try:
                    restriction = self._task_restriction(task)
                except Exception:
                    continue
                if restriction is None:
                    self._task_store(campaign["id"]).update(task["id"], {
                        "next_cycle": 1, "status": "pending", "phase": "idle",
                        "search_started": False, "restriction_reason": None,
                        "updated_at": self._now(),
                    })
        if not campaign.get("legacy_progress_migrated"):
            self._update_campaign(campaign["id"], {"legacy_progress_migrated": True})
    def _set_task(self, campaign: dict, account_id: str, target_id: str, values: dict) -> dict:
        task_id = self._task_id(campaign["id"], account_id, target_id)
        store = self._task_store(campaign["id"])
        if store.get(task_id) is None:
            store.create({
                "id": task_id, "campaign_id": campaign["id"],
                "account_id": account_id, "target_id": target_id,
                "next_cycle": 1, "successful": 0, "failed": 0,
                "phase": "idle", "search_started": False,
            })
        values["updated_at"] = self._now()
        if values.get("phase") not in {None, "idle"} and values.get("status") != "restricted":
            values.setdefault("status", "in_progress")
        return store.update(task_id, values)

    def _finish_task_persist(
        self, task: dict, *, success: bool, error: str | None = None,
    ) -> tuple[dict, int] | None:
        task_id = str(task["id"])
        store = self._task_store(str(task.get("campaign_id", "")))
        current = store.get(task_id) or task
        cycle = int(current.get("current_cycle", current.get("next_cycle", 1)))
        # A task's next cycle advances once. Checkpoint recovery and a late worker
        # callback cannot count the same cycle twice.
        if current.get("phase") in {None, "idle"} and int(current.get("next_cycle", 1)) > cycle:
            return None
        changes = {
            "next_cycle": cycle + 1,
            "successful": int(current.get("successful", 0)) + int(success),
            "failed": int(current.get("failed", 0)) + int(not success),
            "phase": "idle", "search_started": False,
            "verification_resume_phase": "search",
            "verification_state": "idle",
            "last_error": error, "status": "complete" if success else "failed",
            "updated_at": self._now(),
        }
        store.update(task_id, changes)
        self._refresh_progress(str(current["campaign_id"]))
        return current, cycle

    def _record_task_completion(
        self, current: dict, cycle: int, *, success: bool, error: str | None,
    ) -> None:
        campaign_id = str(current["campaign_id"])
        account_id = str(current.get("account_id", ""))
        if self._is_scheduled_pair(campaign_id, account_id):
            event_type = "cycle_completed" if success else "cycle_failed"
            assignment_id = self._scheduled_runtimes[account_id].assignment_id
            self._record_scheduled_event(
                event_type,
                identity=f"{event_type}:{assignment_id}:{current.get('target_id')}:{cycle}",
                campaign_id=campaign_id, account_id=account_id,
                target_id=str(current.get("target_id", "")), cycle=cycle,
                assignment_id=assignment_id, error=error,
            )

    def _finish_task(self, task: dict, *, success: bool, error: str | None = None) -> None:
        completed = self._finish_task_persist(task, success=success, error=error)
        if completed is not None:
            current, cycle = completed
            self._record_task_completion(current, cycle, success=success, error=error)

    async def _finish_task_async(
        self, task: dict, *, success: bool, error: str | None = None,
    ) -> None:
        async with self._campaign_state_lock:
            completed = await self._storage_io(
                self._finish_task_persist, task, success=success, error=error,
            )
        if completed is not None:
            current, cycle = completed
            self._record_task_completion(current, cycle, success=success, error=error)
            verification_key = self._verification_key(
                str(current.get("campaign_id", "")),
                str(current.get("account_id", "")),
                str(current.get("target_id", "")),
            )
            self._verification_events.pop(verification_key, None)
            self._verification_confirmed_events.pop(verification_key, None)
            self._verification_registered_monotonic_ns.pop(verification_key, None)

    def _fail_target_remaining(self, campaign: dict, account_id: str, target_id: str, error: str) -> None:
        task = self._task_for(campaign["id"], account_id, target_id)
        if task is None:
            task = self._set_task(campaign, account_id, target_id, {"phase": "idle"})
        next_cycle = int(task.get("next_cycle", 1))
        remaining = (1 if self._is_standalone_scheduled(str(campaign["id"])) else
                     max(0, int(campaign["total_cycles"]) - next_cycle + 1))
        if remaining:
            if self._is_scheduled_pair(str(campaign["id"]), str(account_id)):
                self._record_scheduled_event(
                    "task_failed_remaining",
                    identity=(f"task-failed:{self._scheduled_runtimes[str(account_id)].assignment_id}:"
                              f"{target_id}:{next_cycle}:{error}"),
                    campaign_id=str(campaign["id"]), account_id=str(account_id),
                    target_id=str(target_id), cycles=remaining,
                    assignment_id=self._scheduled_runtimes[str(account_id)].assignment_id,
                    error=str(error),
                )
            self._task_store(str(campaign["id"])).update(task["id"], {
                "next_cycle": int(campaign["total_cycles"]) + 1,
                "failed": int(task.get("failed", 0)) + remaining,
                "phase": "idle", "search_started": False,
                "last_error": error, "status": "failed", "updated_at": self._now(),
            })
            self._refresh_progress(campaign["id"])

    def _restriction_for(self, account_id: str, target_id: str, target: dict, *, target_bot_id=None) -> dict | None:
        if self.restriction_service is None:
            return None
        return self.restriction_service.active_for(
            account_id,
            target_id,
            target_bot_id=target_bot_id if target_bot_id is not None else target.get("telegram_user_id"),
            username=str(target.get("username", "")),
        )

    def _task_restriction(self, task: dict) -> dict | None:
        target_id = str(task.get("target_id", ""))
        target = self.targets.get(target_id) or {}
        return self._restriction_for(str(task.get("account_id", "")), target_id, target)

    def _mark_target_restricted(self, campaign: dict, account_id: str, target_id: str, restriction) -> None:
        task = self._task_for(campaign["id"], account_id, target_id)
        if task is None:
            task = self._set_task(campaign, account_id, target_id, {"phase": "idle"})
        if task.get("status") == "restricted":
            return
        total_cycles = int(campaign.get("total_cycles", 1))
        next_cycle = int(task.get("next_cycle", 1))
        remaining = (1 if self._is_standalone_scheduled(str(campaign["id"])) else
                     max(0, total_cycles - next_cycle + 1))
        if isinstance(restriction, BanDetectionResult):
            reason = f"Explicit target bot access denial (matched phrase: {restriction.matched_phrase})"
            restriction_id = None
        elif isinstance(restriction, dict):
            reason = str(restriction.get("detection_reason") or restriction.get("matched_phrase") or "Active restriction")
            restriction_id = restriction.get("id")
        else:
            reason = "Explicit target bot access denial"
            restriction_id = None
        store = self._task_store(str(campaign["id"]))
        current = store.get(str(task["id"])) or task
        if self._is_scheduled_pair(str(campaign["id"]), str(account_id)):
            restriction_key = f"{account_id}:{target_id}"
            self._record_scheduled_event(
                "target_restriction", identity=f"scheduled-restriction:{account_id}:{target_id}",
                campaign_id=str(campaign["id"]), account_id=str(account_id),
                target_id=str(target_id), restriction_key=restriction_key,
            )
        store.update(task["id"], {
            "next_cycle": total_cycles + 1,
            "restricted": int(current.get("restricted", 0)) + remaining,
            "status": "restricted",
            "restriction_reason": reason,
            "restriction_id": restriction_id,
            "last_error": "target_access_denied" if isinstance(restriction, BanDetectionResult) else "active_restriction",
            "phase": "idle", "search_started": False,
            "updated_at": self._now(),
        })
        self._refresh_progress(campaign["id"])

    def _fail_account_remaining(self, campaign: dict, account_id: str, error: str) -> None:
        campaign_id = str(campaign.get("id", ""))
        if self._is_scheduled_pair(campaign_id, str(account_id)):
            day = self._now()[:10]
            account_restricted = any(word in str(error).casefold()
                                      for word in ("banned", "deactivated", "authkeyunregistered"))
            self._record_scheduled_event(
                "account_restricted" if account_restricted else "account_unavailable",
                identity=f"scheduled-account-unavailable:{day}:{campaign_id}:{account_id}:{error}",
                campaign_id=campaign_id, account_id=str(account_id), reason=str(error),
            )
        for target_id in campaign["target_ids"]:
            self._fail_target_remaining(campaign, account_id, target_id, error)

    def _refresh_progress(self, campaign_id: str, *, status: str | None = None, operation: str | None = None, **extra) -> dict:
        tasks = self.task_records(campaign_id)
        completed = sum(
            int(item.get("successful", 0)) + int(item.get("failed", 0)) + int(item.get("restricted", 0))
            for item in tasks
        )
        successful = sum(int(item.get("successful", 0)) for item in tasks)
        failed = sum(int(item.get("failed", 0)) for item in tasks)
        restricted = sum(int(item.get("restricted", 0)) for item in tasks)
        campaign = self.get(campaign_id)
        if campaign is None:
            raise RuntimeError("Campaign record disappeared")
        total = int(campaign.get("total_cycles", 1)) * len(campaign.get("account_ids", [])) * len(campaign.get("target_ids", []))
        progress = {"completed": completed, "successful": successful, "failed": failed, "restricted": restricted, "total": total}
        values = {"progress": progress, "updated_at": self._now(), **extra}
        if status is not None:
            values["status"] = status
            values["execution_state"] = status
            values["pause_requested"] = False
            values["stop_requested"] = status == "stopped"
        if operation is not None:
            values["current_operation"] = operation
        return self._update_campaign(campaign_id, values)

    def _set_current(self, campaign_id: str, account_id: str, target_id: str, cycle: int, operation: str) -> None:
        self._update_campaign(campaign_id, {
            "current_account_id": account_id, "current_target_id": target_id,
            "current_cycle": cycle, "current_operation": operation,
        })

    def _update_campaign(self, campaign_id: str, changes: dict) -> dict:
        changes.setdefault("updated_at", self._now())
        if self._is_standalone_scheduled(campaign_id):
            if self._scheduled_promotion_repository is None:
                raise RuntimeError("Scheduled promotion storage is unavailable")
            config = self._scheduled_promotion_repository.get_config()
            model = dict(config.get("scheduled_execution") or {})
            model.update(changes)
            config["scheduled_execution"] = model
            self._scheduled_promotion_repository.save_config(config)
            return model
        return self.campaigns.update(campaign_id, changes)

    async def _reserve_rotation(self, campaign_id: str) -> tuple[int, str]:
        lock = self._rotation_locks.setdefault(campaign_id, asyncio.Lock())
        async with lock:
            campaign = self.get(campaign_id) or {}
            index = int(campaign.get("rotation_index", 0)) % len(PROMOTION_MESSAGES)
            self._update_campaign(campaign_id, {"rotation_index": index + 1})
            return index, PROMOTION_MESSAGES[index]

    def _sticker_path(self, sticker_id: str) -> Path:
        record = self.stickers.get(sticker_id)
        if record is None:
            raise RuntimeError("campaign sticker is unavailable")
        path = Path(record.get("file_path", ""))
        if not path.is_absolute():
            path = self.client_manager.settings.stickers_dir / path
        root = self.client_manager.settings.stickers_dir.resolve()
        resolved = path.resolve()
        if root not in resolved.parents or not resolved.is_file():
            raise RuntimeError("campaign sticker file is unavailable")
        return resolved

    @staticmethod
    def _sticker_attributes(sticker: dict, path: Path) -> list:
        """Mark the uploaded document as a Telegram sticker for MTProto."""
        attributes = []
        if sticker.get("is_animated") is True or path.suffix.lower() == ".tgs":
            attributes.append(types.DocumentAttributeAnimated())
        elif sticker.get("is_video") is True or path.suffix.lower() == ".webm":
            attributes.append(types.DocumentAttributeVideo(
                duration=3.0,
                w=int(sticker.get("width") or 512),
                h=int(sticker.get("height") or 512),
                nosound=True,
            ))
        attributes.append(types.DocumentAttributeSticker(
            alt=str(sticker.get("emoji") or "⭐"),
            stickerset=types.InputStickerSetEmpty(),
        ))
        return attributes

    def _raise_if_target_opted_out(self, target_id: str) -> None:
        if not self._is_target_opted_in(target_id):
            raise _TargetOptedOut

    def _is_target_opted_in(self, target_id: str) -> bool:
        target = self.targets.get(str(target_id))
        return bool(target and target.get("opted_in") is True)

    def _target_content_config(self, campaign: dict, target_id: str) -> dict | None:
        target = self.targets.get(str(target_id))
        if target is None or target.get("opted_in") is not True or not target.get("username"):
            return None
        configs = campaign.get("target_configs")
        if isinstance(configs, dict):
            saved = configs.get(str(target_id))
            if not isinstance(saved, dict) or (
                saved.get("target_id") is not None
                and str(saved.get("target_id")) != str(target_id)
            ):
                return None
        else:
            sticker_id = campaign.get("sticker_id")
            if not sticker_id:
                return None
            saved = {
                "target_id": str(target_id),
                "text_enabled": True,
                "promotional_text": None,
                "legacy_rotating_text": True,
                "sticker_enabled": True,
                "sticker_id": str(sticker_id),
            }
        text_enabled = saved.get("text_enabled") is True
        sticker_enabled = saved.get("sticker_enabled") is True
        if not text_enabled and not sticker_enabled:
            return None
        promotional_text = saved.get("promotional_text")
        legacy_rotating_text = saved.get("legacy_rotating_text") is True
        if text_enabled and not legacy_rotating_text and (
            not isinstance(promotional_text, str) or not promotional_text.strip()
            or len(promotional_text.strip()) > 4096
        ):
            return None
        sticker_id = saved.get("sticker_id")
        if sticker_enabled:
            if not isinstance(sticker_id, str):
                return None
            try:
                self._sticker_path(sticker_id)
            except (RuntimeError, OSError, ValueError):
                return None
        return {
            "target_id": str(target_id),
            "target_username": str(target["username"]),
            "target_display_name": f"@{target['username']}",
            "text_enabled": text_enabled,
            "promotional_text": (
                promotional_text.strip()
                if text_enabled and isinstance(promotional_text, str) else None
            ),
            "legacy_rotating_text": legacy_rotating_text,
            "sticker_enabled": sticker_enabled,
            "sticker_id": str(sticker_id) if sticker_enabled else None,
        }

    def _validated_campaign(self, campaign_id: str) -> dict | None:
        campaign = self.get(campaign_id)
        if campaign is None:
            return None
        account_ids = campaign.get("account_ids")
        if not account_ids and campaign.get("account_id"):
            account_ids = [campaign["account_id"]]
        if not isinstance(account_ids, list) or not account_ids:
            return None
        if any(self.accounts.get(str(item)) is None for item in account_ids):
            return None
        target_ids = campaign.get("target_ids")
        if not isinstance(target_ids, list) or not target_ids:
            return None
        for target_id in target_ids:
            target = self.targets.get(str(target_id))
            if target is None or target.get("opted_in") is not True or not target.get("username"):
                return None
            if self._target_content_config(campaign, str(target_id)) is None:
                return None
        try:
            cycles = int(campaign.get("total_cycles", 0))
        except (TypeError, ValueError):
            return None
        if cycles < 1 or cycles > 100000:
            return None
        configs = campaign.get("target_configs")
        if isinstance(configs, dict) and set(configs) != {str(value) for value in target_ids}:
            return None
        execution_mode = campaign.get("execution_mode", "sequential")
        if execution_mode not in {"sequential", "parallel"}:
            return None
        try:
            concurrency_limit = int(campaign.get("concurrency_limit", 3))
        except (TypeError, ValueError):
            return None
        if not 1 <= concurrency_limit <= 5:
            return None
        changes = {}
        if campaign.get("account_ids") != account_ids:
            changes["account_ids"] = account_ids
        if campaign.get("total_cycles") != cycles:
            changes["total_cycles"] = cycles
        if campaign.get("execution_mode") != execution_mode:
            changes["execution_mode"] = execution_mode
        if campaign.get("concurrency_limit") != concurrency_limit:
            changes["concurrency_limit"] = concurrency_limit
        try:
            rotation_index = max(0, int(campaign.get("rotation_index", 0)))
        except (ValueError, TypeError):
            rotation_index = 0
        if campaign.get("rotation_index") != rotation_index:
            changes["rotation_index"] = rotation_index
        if changes:
            campaign = self._update_campaign(campaign_id, changes)
        return campaign

    async def _wait(self, seconds: float) -> None:
        if seconds > 0:
            await asyncio.sleep(seconds)

    @staticmethod
    def _normalize_emoji(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", str(value or "")).strip()
        return "".join(
            char for char in normalized
            if unicodedata.category(char) != "Cf" and char not in {"\ufe0e", "\ufe0f"}
        ).casefold()

    def _verification_key(self, campaign_id: str, account_id: str, target_id: str) -> tuple[str, str, str]:
        return str(campaign_id), str(account_id), str(target_id)

    def _verification_records(self, campaign_id: str, account_id: str, target_id: str) -> list[dict]:
        key = self._verification_key(campaign_id, account_id, target_id)
        return [
            record for record in self._verification_store.list()
            if self._verification_key(
                str(record.get("campaign_id", "")),
                str(record.get("account_id", "")),
                str(record.get("target_id", "")),
            ) == key
        ]

    def _active_verification_record(self, campaign_id: str, account_id: str, target_id: str) -> dict | None:
        records = self._verification_records(campaign_id, account_id, target_id)
        active = [
            record for record in records
            if record.get("status") in {"detected", "awaiting_button", "submitted", "awaiting_confirmation"}
        ]
        active.sort(key=lambda record: str(record.get("detected_at", "")), reverse=True)
        return active[0] if active else None

    def _resume_phase_for_task(self, task: dict) -> str:
        existing = str(task.get("verification_resume_phase") or "").strip()
        if existing:
            return existing
        phase = str(task.get("phase") or "idle")
        if phase in {"preparing", "opening", "search_pending", "idle", "after_search"}:
            return "sticker" if phase == "after_search" else "search"
        if phase in {"sticker_pending", "after_sticker"}:
            return "message"
        if phase in {"message_pending", "after_message"}:
            return "stop"
        if phase in {"stop_pending", "after_stop"}:
            return "complete"
        return "search"

    async def _handle_ordinary_verification(
        self, event, account_id: str, target_id: str, target_username: str,
        emoji: str, campaign_id: str | None,
    ) -> None:
        campaign_id = str(campaign_id or "")
        account_id, target_id = str(account_id), str(target_id)
        if not campaign_id:
            return
        key = self._verification_key(campaign_id, account_id, target_id)
        active = self._active_verification_record(campaign_id, account_id, target_id)
        if active is not None:
            return

        message_id = getattr(event.message, "id", None)
        record_id = f"{campaign_id}:{account_id}:{target_id}:{message_id or uuid.uuid4().hex}"
        now = self._now()
        record = {
            "id": record_id,
            "campaign_id": campaign_id,
            "account_id": account_id,
            "target_id": target_id,
            "target_username": target_username,
            "kind": "ordinary_emoji",
            "emoji": emoji,
            "message_id": message_id,
            "status": "detected",
            "detected_at": now,
            "submitted_at": None,
            "confirmed_at": None,
            "last_error": None,
        }
        record, created = self._verification_store.create_if_absent(record)
        if not created:
            return

        verification_event = self._verification_events.setdefault(key, asyncio.Event())
        verification_event.set()
        confirmed_event = self._verification_confirmed_events.setdefault(key, asyncio.Event())
        confirmed_event.clear()

        campaign = self.get(campaign_id) or {}
        task = self._task_for(campaign_id, account_id, target_id)
        resume_phase = self._resume_phase_for_task(task or {})
        if task is not None:
            self._task_store(campaign_id).update(task["id"], {
                "status": "verification_paused",
                "phase": "idle",
                "search_started": False,
                "verification_state": "awaiting_confirmation",
                "verification_resume_phase": resume_phase,
                "verification_emoji": emoji,
                "suspension_reason": "Ordinary emoji verification",
                "updated_at": now,
            })

        paused_pairs = {
            f"{str(pair)}"
            for pair in (campaign.get("verification_paused_pairs") or [])
        }
        paused_pairs.add(f"{account_id}:{target_id}")
        self._update_campaign(campaign_id, {"verification_paused_pairs": sorted(paused_pairs)})
        registered_monotonic_ns = time.monotonic_ns()
        self._verification_registered_monotonic_ns[key] = registered_monotonic_ns
        logger.info(
            "Verification pending state registered "
            "(monotonic_ns=%d, task_state_registered=%s)",
            registered_monotonic_ns,
            task is not None,
        )
        self._refresh_progress(campaign_id, status="running", operation="ordinary emoji verification paused")
        await self._notify_for_verification(campaign_id)

        try:
            get_buttons = getattr(event.message, "get_buttons", None)
            if get_buttons is None:
                raise RuntimeError("Target bot message did not expose inline buttons")
            rows = await get_buttons()
            matching_button = None
            target_emoji = self._normalize_emoji(emoji)
            for row in rows or []:
                for button in row:
                    button_text = getattr(button, "text", "")
                    if self._normalize_emoji(str(button_text)) == target_emoji:
                        matching_button = button
                        break
                if matching_button is not None:
                    break
            if matching_button is None:
                raise RuntimeError("requested emoji button not found")

            self._verification_store.update(record_id, {
                "status": "awaiting_confirmation",
                "button_text": str(getattr(matching_button, "text", "")),
                "submitted_at": self._now(),
            })
            await matching_button.click()
            record_after_click = self._verification_store.update(
                record_id,
                lambda current: (
                    current if current.get("status") == "verified"
                    else {**current, "status": "submitted"}
                ),
            )
            if task is not None and record_after_click.get("status") != "verified":
                self._task_store(campaign_id).update(task["id"], {
                    "verification_state": "awaiting_confirmation",
                    "updated_at": self._now(),
                })
            asyncio.create_task(
                self._timeout_ordinary_verification(record_id, key),
                name=f"emoji-verification-timeout-{record_id}",
            )
            logger.info(
                "Ordinary emoji verification submitted (account_id=%s, target=%s, emoji=%s)",
                account_id,
                target_username,
                emoji,
            )
        except Exception as exc:
            self._verification_registered_monotonic_ns.pop(key, None)
            self._verification_store.update(record_id, {
                "status": "failed",
                "failed_at": self._now(),
                "last_error": type(exc).__name__,
            })
            if task is not None:
                self._task_store(campaign_id).update(task["id"], {
                    "status": "verification_failed",
                    "phase": "idle",
                    "verification_state": "failed",
                    "last_error": type(exc).__name__,
                    "updated_at": self._now(),
                })
            logger.warning(
                "Ordinary emoji verification submission failed (account_id=%s, target=%s, error=%s)",
                account_id,
                target_username,
                type(exc).__name__,
            )
            await self._notify_for_verification(campaign_id)

    async def _timeout_ordinary_verification(
        self, record_id: str, key: tuple[str, str, str],
    ) -> None:
        await asyncio.sleep(120)
        record = self._verification_store.get(record_id)
        if record is None or record.get("status") not in {"detected", "awaiting_button", "submitted", "awaiting_confirmation"}:
            return
        now = self._now()
        self._verification_store.update(record_id, {
            "status": "timeout",
            "failed_at": now,
            "last_error": "confirmation_timeout",
        })
        campaign_id, account_id, target_id = key
        self._verification_registered_monotonic_ns.pop(key, None)
        task = self._task_for(campaign_id, account_id, target_id)
        if task is not None:
            self._task_store(campaign_id).update(task["id"], {
                "status": "verification_failed",
                "phase": "idle",
                "verification_state": "timeout",
                "last_error": "confirmation_timeout",
                "updated_at": now,
            })
        logger.warning(
            "Ordinary emoji verification timed out (account_id=%s, target=%s)",
            account_id,
            target_id,
        )
        await self._notify_for_verification(campaign_id)

    async def _handle_ordinary_verification_confirmation(
        self, event, account_id: str, target_id: str, text: str,
        campaign_id: str | None, received_monotonic_ns: int | None = None,
    ) -> None:
        campaign_id = str(campaign_id or "")
        key = self._verification_key(campaign_id, str(account_id), str(target_id))
        lookup_started_monotonic_ns = time.monotonic_ns()
        record = self._active_verification_record(campaign_id, str(account_id), str(target_id))
        target = self.targets.get(str(target_id))
        sender_id = getattr(event, "sender_id", None)
        try:
            expected_bot_id = int((target or {}).get("telegram_user_id"))
        except (TypeError, ValueError):
            expected_bot_id = None
        task = self._task_for(campaign_id, str(account_id), str(target_id))
        lookup_completed_monotonic_ns = time.monotonic_ns()
        registered_monotonic_ns = self._verification_registered_monotonic_ns.get(key)
        bot_match = expected_bot_id is not None and sender_id == expected_bot_id
        pending_task_match = bool(
            task is not None
            and task.get("status") == "verification_paused"
            and task.get("verification_state") == "awaiting_confirmation"
        )
        record_match = record is not None
        logger.info(
            "Target confirmation correlation "
            "(received_monotonic_ns=%s, registered_monotonic_ns=%s, "
            "lookup_started_monotonic_ns=%d, lookup_completed_monotonic_ns=%d, "
            "receipt_before_registration=%s, bot_match=%s, pending_task_match=%s, "
            "active_record_match=%s)",
            received_monotonic_ns,
            registered_monotonic_ns,
            lookup_started_monotonic_ns,
            lookup_completed_monotonic_ns,
            (
                received_monotonic_ns < registered_monotonic_ns
                if received_monotonic_ns is not None and registered_monotonic_ns is not None
                else None
            ),
            bot_match,
            pending_task_match,
            record_match,
        )
        if not (bot_match and pending_task_match and record_match):
            return

        now = self._now()
        self._verification_store.update(record["id"], {
            "status": "verified",
            "confirmed_at": now,
            "confirmation_text": text[:500],
            "last_error": None,
        })
        self._task_store(campaign_id).update(task["id"], {
            "status": "pending",
            "phase": "idle",
            "search_started": False,
            "verification_state": "verified",
            "last_error": None,
            "updated_at": now,
        })
        campaign = self.get(campaign_id) or {}
        paused_pairs = {
            str(pair) for pair in (campaign.get("verification_paused_pairs") or [])
            if str(pair) != f"{account_id}:{target_id}"
        }
        self._update_campaign(campaign_id, {"verification_paused_pairs": sorted(paused_pairs)})
        self._verification_events.setdefault(key, asyncio.Event()).clear()
        self._verification_confirmed_events.setdefault(key, asyncio.Event()).set()
        self._verification_registered_monotonic_ns.pop(key, None)
        self._refresh_progress(campaign_id, status="running", operation="ordinary emoji verification confirmed")
        await self._notify_for_verification(campaign_id)
        logger.info(
            "Target task confirmation matched to a pending account-target task",
        )

    async def _notify_for_verification(self, campaign_id: str) -> None:
        campaign = self.get(campaign_id)
        if campaign is not None:
            await self._refresh_and_notify(campaign_id)

    async def _refresh_and_notify(self, campaign_id: str) -> None:
        campaign = self.get(campaign_id)
        for runtime in list(self._runtimes.values()):
            if runtime.task is not None:
                await self._notify(runtime, campaign)

    async def _wait_for_verification_resolution(
        self, campaign_id: str, account_id: str, target_id: str,
    ) -> bool:
        key = self._verification_key(campaign_id, account_id, target_id)
        confirmed_event = self._verification_confirmed_events.setdefault(key, asyncio.Event())
        deadline = asyncio.get_running_loop().time() + 120
        while asyncio.get_running_loop().time() < deadline:
            task = self._task_for(campaign_id, account_id, target_id)
            if task is not None:
                state = str(task.get("verification_state") or "")
                status = str(task.get("status") or "")
                if state == "verified" and status == "pending":
                    logger.info("Promotion resume triggered after confirmed target task")
                    return True
                if status in {"verification_failed", "failed"}:
                    return False
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                await asyncio.wait_for(confirmed_event.wait(), timeout=min(0.25, remaining))
            except asyncio.TimeoutError:
                continue
        return False

    def _raise_if_verification_paused(self, account_id: str, target_id: str, campaign_id: str) -> None:
        key = self._verification_key(campaign_id, account_id, target_id)
        event = self._verification_events.get(key)
        if event is not None and event.is_set():
            raise _VerificationPaused

    def _signal_ban(self, result: BanDetectionResult) -> None:
        key = (result.account_id, result.target_id)
        self._ban_events.setdefault(key, asyncio.Event()).set()
        if self.restriction_service is not None:
            try:
                loop = asyncio.get_running_loop()
                task = loop.create_task(
                    self.restriction_service.handle_detection(result)
                )
                self._restriction_tasks[key] = task
                task.add_done_callback(
                    lambda completed, pair=key: self._restriction_task_done(pair, completed),
                )
            except RuntimeError:
                logger.exception("Could not schedule restriction persistence (detection_id=%s)", result.detection_id)

    def _daily_limit_event(self, target_bot_id, username: str) -> asyncio.Event:
        keys = (
            self.daily_limit_service.target_keys(target_bot_id, username)
            if self.daily_limit_service is not None
            else (f"bot:{target_bot_id}",) if target_bot_id is not None
            else (f"username:{str(username).casefold().lstrip('@')}",)
        )
        key = keys[0]
        return self._daily_limit_events.setdefault(key, asyncio.Event())

    def _daily_limit_changed(self, record: dict) -> None:
        keys = DailyChatLimitService.target_keys(
            record.get("target_bot_id"), str(record.get("target_username", "")),
        )
        if record.get("status") == "Daily Limit Reached":
            for key in keys:
                self._daily_limit_events.setdefault(key, asyncio.Event()).set()
            return
        for key in keys:
            event = self._daily_limit_events.get(key)
            if event is not None:
                event.clear()
        bot_id = record.get("target_bot_id")
        if bot_id is not None:
            self.ban_detection.clear_daily_limit(int(bot_id))

    def _signal_daily_limit(self, result: DailyChatLimitDetectionResult) -> None:
        event = self._daily_limit_event(result.target_bot_id, result.target_username)
        event.set()
        if self.daily_limit_service is None:
            logger.error(
                "Daily chat limit detected without persistence service (target=%s)",
                result.target_username,
            )
            return
        try:
            record = self.daily_limit_service.activate(result)
            record_id = str(record["id"])
            existing = self._daily_limit_tasks.get(record_id)
            if existing is not None and not existing.done():
                return
            task = asyncio.create_task(
                self.daily_limit_service.persist_detection(result),
                name=f"daily-limit-persist-{record_id[-12:]}",
            )
            self._daily_limit_tasks[record_id] = task
            task.add_done_callback(
                lambda completed, key=record_id: self._daily_limit_task_done(key, completed),
            )
        except Exception:
            logger.exception(
                "Could not activate daily chat limit (detection_id=%s)",
                result.detection_id,
            )

    def _daily_limit_task_done(self, record_id: str, task: asyncio.Task) -> None:
        if self._daily_limit_tasks.get(record_id) is task:
            self._daily_limit_tasks.pop(record_id, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Daily chat limit persistence failed (record_id=%s, error=%s)",
                record_id, type(error).__name__,
            )

    async def _await_daily_limit_persistence(self, target_bot_id, username: str) -> None:
        if self.daily_limit_service is None:
            return
        record = self.daily_limit_service.active_for(target_bot_id, username)
        if record is None:
            return
        task = self._daily_limit_tasks.get(str(record.get("id", "")))
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    async def _raise_if_daily_limited_async(self, target_bot_id, username: str) -> None:
        if self.daily_limit_service is None:
            return
        record = self.daily_limit_service.active_for(target_bot_id, username)
        keys = self.daily_limit_service.target_keys(target_bot_id, username)
        event = self._daily_limit_events.get(keys[0]) if keys else None
        if record is not None:
            if event is not None:
                event.set()
            raise _TargetDailyLimitReached
        if event is not None and event.is_set():
            event.clear()

    async def _wait_for_daily_limit(
        self, seconds: int, event: asyncio.Event, target_bot_id, username: str,
    ) -> dict | None:
        if self.daily_limit_service is None:
            await asyncio.sleep(seconds)
            return None
        if not event.is_set():
            try:
                await asyncio.wait_for(event.wait(), timeout=seconds)
            except asyncio.TimeoutError:
                return None
        return self.daily_limit_service.active_for(target_bot_id, username)

    async def _mark_daily_limit_task_async(self, task: dict, record: dict) -> None:
        campaign_id = str(task.get("campaign_id", ""))
        task_id = str(task["id"])
        async with self._campaign_state_lock:
            await self._storage_io(
                self._task_store(campaign_id).update,
                task_id,
                {
                    "status": "daily_limit_reached",
                    "phase": "idle",
                    "current_cycle": None,
                    "search_started": False,
                    "restriction_reason": "Daily Limit Reached",
                    "daily_limit_record_id": str(record.get("id", "")),
                    "daily_limit_until": str(record.get("next_eligibility_at", "")),
                    "last_error": "Daily Limit Reached",
                    "updated_at": self._now(),
                },
            )
        await self._refresh_progress_async(campaign_id)

    def _restriction_task_done(
        self, key: tuple[str, str], task: asyncio.Task,
    ) -> None:
        if task.cancelled():
            self._restriction_tasks.pop(key, None)
            return
        error = task.exception()
        if error is None:
            if self._restriction_tasks.get(key) is task:
                self._restriction_tasks.pop(key, None)
            return
        logger.error(
            "Restriction persistence task failed (account_id=%s, target_id=%s, error=%s)",
            key[0], key[1], type(error).__name__,
        )

    async def _await_restriction_persistence(self, account_id: str, target_id: str) -> None:
        task = self._restriction_tasks.get((str(account_id), str(target_id)))
        if task is not None:
            await asyncio.shield(task)

    async def _wait_for_cycle(
        self,
        seconds: float,
        ban_event: asyncio.Event,
        daily_limit_event: asyncio.Event,
        verification_event: asyncio.Event,
        account_id: str,
        target_id: str,
        campaign_id: str,
    ) -> None:
        if ban_event.is_set():
            await self._raise_if_banned_async(account_id, target_id)
        if daily_limit_event.is_set():
            target = await self._storage_io(self.targets.get, str(target_id)) or {}
            await self._raise_if_daily_limited_async(
                target.get("telegram_user_id"), str(target.get("username", "")),
            )
        if verification_event.is_set():
            self._raise_if_verification_paused(account_id, target_id, campaign_id)
        if seconds > 0:
            try:
                ban_task = asyncio.create_task(ban_event.wait())
                daily_task = asyncio.create_task(daily_limit_event.wait())
                verification_task = asyncio.create_task(verification_event.wait())
                done, pending = await asyncio.wait(
                    {ban_task, daily_task, verification_task},
                    timeout=seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            except asyncio.TimeoutError:
                return
            await self._raise_if_banned_async(account_id, target_id)
            if daily_limit_event.is_set():
                target = await self._storage_io(self.targets.get, str(target_id)) or {}
                await self._raise_if_daily_limited_async(
                    target.get("telegram_user_id"), str(target.get("username", "")),
                )
            if verification_event.is_set():
                self._raise_if_verification_paused(account_id, target_id, campaign_id)

    async def _notify(self, runtime: _Runtime, campaign: dict | None) -> None:
        if runtime.callback is not None and campaign is not None:
            async with runtime.notify_lock:
                try:
                    latest = await self._get_async(str(campaign.get("id", ""))) or campaign
                    await runtime.callback(latest)
                except Exception as exc:
                    logger.debug("Campaign progress callback failed (error=%s)", type(exc).__name__)

    async def _progress_ticker(self, campaign_id: str, runtime: _Runtime) -> None:
        """Refresh the single dashboard message periodically during long cycles."""
        while True:
            await asyncio.sleep(5)
            campaign = await self._get_async(campaign_id)
            if campaign is None or campaign.get("status") != "running":
                return
            await self._notify(runtime, campaign)

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
