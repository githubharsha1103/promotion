"""Sequential, resumable MTProto campaign execution."""

import asyncio
from dataclasses import dataclass, field
import logging
from pathlib import Path
import time
from typing import Awaitable, Callable
import uuid

from telethon.errors import FloodWaitError
from telethon.tl import types

from app.storage.repositories.account_repository import AccountRepository
from app.storage.repositories.campaign_repository import CampaignRepository
from app.storage.repositories.sticker_repository import StickerRepository
from app.storage.repositories.target_repository import TargetRepository
from app.storage.json_store import JsonStore
from app.telegram.client_manager import TelegramClientManager
from app.telegram.ban_detection import BanDetectionEngine, BanDetectionResult, DEFAULT_BAN_PHRASES
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


class _RestrictionCheckUnavailable(Exception):
    """Fail closed when persistent restriction state cannot be read."""


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
        self.ban_detection = BanDetectionEngine(phrases)
        self.restriction_service = restriction_service
        self._ban_events: dict[tuple[str, str], asyncio.Event] = {}
        self._restriction_tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._scheduled_runtimes: dict[str, _Runtime] = {}
        self._scheduled_claims: set[str] = set()
        self._normal_account_workers: dict[str, dict[asyncio.Task, tuple[str, _Runtime]]] = {}
        self._runtimes: dict[str, _Runtime] = {}
        self._account_locks: dict[str, asyncio.Lock] = {}
        self._rotation_locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()
        self._scheduled_event_recorder = None
        self._scheduled_promotion_repository = None
        self._scheduled_task_store = JsonStore(campaign_repository.records_dir / "scheduled_promotion_tasks.json")

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
                    or not campaign.get("target_ids") or not campaign.get("sticker_id")):
                return None
            target = self.targets.get(str(campaign["target_ids"][0])) or {}
            if target.get("opted_in") is not True or not target.get("username"):
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
        self._ensure_tasks(campaign)
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
        self._record_scheduled_event(
            "assignment_started", identity=f"assignment-start:{assignment_id}",
            assignment_id=assignment_id, account_id=account_id, campaign_id=campaign_id,
            tasks=self._scheduled_task_snapshot(campaign_id, account_id, runtime.scheduled_target_ids),
        )
        try:
            await self._run_account(campaign, account_id, runtime)
        except Exception as exc:
            self._record_scheduled_event(
                "scheduled_execution_error", identity=f"scheduled-execution-error:{assignment_id}",
                assignment_id=assignment_id, campaign_id=campaign_id, account_id=account_id,
                reason=type(exc).__name__,
            )
            raise
        finally:
            snapshot = self._scheduled_task_snapshot(campaign_id, account_id, runtime.scheduled_target_ids)
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
            if self._scheduled_runtimes.get(account_id) is runtime:
                self._scheduled_runtimes.pop(account_id, None)

    def _scheduled_task_snapshot(self, campaign_id: str, account_id: str,
                                 target_ids: list[str] | None = None) -> list[dict]:
        return [dict(task) for task in self.task_records(str(campaign_id))
                if str(task.get("account_id")) == str(account_id)
                and (target_ids is None or str(task.get("target_id")) in {str(value) for value in target_ids})]

    def stop_scheduled_assignments(self) -> None:
        """Stop workers at their next existing cycle boundary."""
        for account_id, runtime in self._scheduled_runtimes.items():
            campaign_id = str(runtime.scheduled_campaign_id or "")
            campaign = self.get(campaign_id) or {}
            snapshot = self._scheduled_task_snapshot(campaign_id, account_id, runtime.scheduled_target_ids)
            unfinished = any(
                int(task.get("next_cycle", 1)) <= int(campaign.get("total_cycles", 1))
                and task.get("status") != "restricted"
                for task in snapshot
            )
            self._record_scheduled_event(
                "assignment_stop_requested",
                identity=f"assignment-stop:{runtime.assignment_id}",
                assignment_id=runtime.assignment_id, campaign_id=campaign_id,
                account_id=str(account_id), unfinished=unfinished, tasks=snapshot,
            )
            runtime.stop.set()

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

    @staticmethod
    def _account_progress(campaign_id: str, account_id: str) -> dict:
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
        campaign = self._validated_campaign(campaign_id)
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
            self._refresh_progress(campaign_id, status="running", operation="manually resumed")
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
                if task.get("phase") not in {None, "idle"}
                and task.get("status") in {"pending", "in_progress", "suspended"}
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
                self._refresh_progress(campaign_id, status="paused", operation="paused after restart")
    async def start(self, campaign_id: str, callback=None) -> bool:
        async with self._guard:
            campaign = self._validated_campaign(campaign_id)
            if campaign is None or campaign.get("status") in {"running", "completed", "suspended"}:
                return False
            runtime = self._runtimes.get(campaign_id)
            if runtime is not None and runtime.task is not None and not runtime.task.done():
                return False
            runtime = _Runtime(callback=callback)
            self._runtimes[campaign_id] = runtime
            self._ensure_tasks(campaign)
            self._refresh_progress(campaign_id, status="running", operation="starting")
            runtime.task = asyncio.create_task(self._run(campaign_id, runtime), name=f"campaign-{campaign_id}")
            return True

    async def pause(self, campaign_id: str) -> bool:
        runtime = self._runtimes.get(campaign_id)
        campaign = self.get(campaign_id)
        if runtime is None or runtime.task is None or campaign is None or campaign.get("status") != "running":
            return False
        runtime.pause.set()
        self._update_campaign(campaign_id, {"pause_requested": True})
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
            self._refresh_progress(campaign_id, status="stopped", operation="stopped")
            return True
        runtime.stop.set()
        runtime.pause.clear()
        self._update_campaign(campaign_id, {"stop_requested": True})
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
            campaign = self._validated_campaign(campaign_id)
            if campaign is None:
                raise RuntimeError("Campaign selections are no longer valid")
            ticker = asyncio.create_task(
                self._progress_ticker(campaign_id, runtime),
                name=f"campaign-progress-{campaign_id}",
            )
            account_ids = [str(item) for item in (account_ids or campaign["account_ids"])]
            results = await asyncio.gather(*(
                self._run_normal_account(campaign, account_id, runtime)
                for account_id in account_ids
            ), return_exceptions=True)
            for account_id, result in zip(account_ids, results):
                if isinstance(result, Exception):
                    logger.warning("Campaign account worker failed independently (error=%s)", type(result).__name__)
                    self._fail_account_remaining(campaign, account_id, type(result).__name__)
            latest = self.get(campaign_id) or {}
            if runtime.stop.is_set():
                status, operation = "stopped", "stopped"
            elif runtime.pause.is_set():
                status, operation = "paused", "paused"
            elif runtime.failed_suspensions:
                status, operation = "paused", "suspension could not reach a safe checkpoint"
            elif runtime.suspended_accounts or (self.get(campaign_id) or {}).get("suspended_accounts"):
                status, operation = "suspended", "suspended for scheduled priority"
            else:
                status, operation = "completed", "complete"
            self._refresh_progress(campaign_id, status=status, operation=operation)
            await self._notify(runtime, self.get(campaign_id))
        except asyncio.CancelledError:
            self._refresh_progress(campaign_id, status="paused", operation="interrupted")
            raise
        except Exception as exc:
            logger.warning("Campaign coordinator failed (campaign_id=%s, error=%s)", campaign_id, type(exc).__name__)
            self._refresh_progress(campaign_id, status="failed", operation="failed", last_error=type(exc).__name__)
            await self._notify(runtime, self.get(campaign_id))
        finally:
            if ticker is not None:
                ticker.cancel()
                await asyncio.gather(ticker, return_exceptions=True)
            self._runtimes.pop(campaign_id, None)

    async def _run_normal_account(self, campaign: dict, account_id: str, runtime: _Runtime) -> None:
        task = asyncio.current_task()
        workers = self._normal_account_workers.setdefault(str(account_id), {})
        if task is not None:
            workers[task] = (str(campaign["id"]), runtime)
        if self.scheduled_account_claimed(account_id):
            runtime.suspend_accounts.add(str(account_id))
        try:
            await self._run_account(campaign, str(account_id), runtime)
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

    async def _run_account(self, campaign: dict, account_id: str, runtime: _Runtime) -> None:
        account_lock = self.account_execution_lock(account_id)
        async with account_lock:
            if self._normal_suspension_requested(runtime, account_id):
                self._suspend_normal_account(campaign, account_id, runtime)
                return
            eligible_targets: list[tuple[str, dict]] = []
            for target_id in campaign["target_ids"]:
                if self._normal_suspension_requested(runtime, account_id):
                    self._suspend_normal_account(campaign, account_id, runtime)
                    return
                task = self._task_for(campaign["id"], account_id, target_id)
                if task is None:
                    task = self._set_task(campaign, account_id, target_id, {"phase": "idle"})
                if task.get("status") == "restricted":
                    continue
                target = self.targets.get(target_id)
                if target is None or target.get("opted_in") is not True:
                    self._fail_target_remaining(campaign, account_id, target_id, "target_not_opted_in")
                    continue
                try:
                    restriction = self._restriction_for(account_id, target_id, target)
                except Exception as exc:
                    logger.warning("Could not check account-target restriction (error=%s)", type(exc).__name__)
                    self._fail_target_remaining(campaign, account_id, target_id, "restriction_store_unavailable")
                    # An unavailable restriction store makes eligibility unknown.
                    for remaining_id in campaign["target_ids"]:
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
                task = self._task_for(campaign["id"], account_id, target_id)
                if task is None or task.get("status") == "restricted":
                    continue
                task = self._task_for(campaign["id"], account_id, target_id)
                try:
                    restriction = self._restriction_for(account_id, target_id, target)
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
                restriction = self._restriction_for(account_id, target_id, target, target_bot_id=bot_id)
                if restriction is not None:
                    self._mark_target_restricted(campaign, account_id, target_id, restriction)
                    continue

                for cycle in range(int(task.get("next_cycle", 1)), int(campaign["total_cycles"]) + 1):
                    if self._normal_suspension_requested(runtime, account_id):
                        self._suspend_normal_account(campaign, account_id, runtime)
                        return
                    if runtime.stop.is_set() or runtime.pause.is_set():
                        return
                    if task.get("status") == "restricted":
                        break
                    detected = self.ban_detection.result_for(account_id, target_id)
                    if detected is not None:
                        self._mark_target_restricted(campaign, account_id, target_id, detected)
                        break
                    # Record which cycle is being prepared before reconnecting.
                    # If that reconnect fails, the failure is attributed to this
                    # cycle instead of the previous completed one.
                    self._set_task(campaign, account_id, target_id, {
                        "phase": "preparing", "current_cycle": cycle,
                        "search_started": False,
                    })
                    if not await self.client_manager.is_connected(account_id):
                        try:
                            if not await self.client_manager.connect(account_id):
                                self._finish_task(task, success=False, error="account_disconnected")
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
                            self._finish_task(task, success=False, error=type(exc).__name__)
                            continue
                    restriction = self._restriction_for(account_id, target_id, target, target_bot_id=bot_id)
                    if restriction is not None:
                        self._mark_target_restricted(campaign, account_id, target_id, restriction)
                        break
                    self.ban_detection.watch_target(
                        account_id, target_id, str(target["username"]), client, entity,
                        self._signal_ban, campaign_id=str(campaign["id"]),
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
        ban_event = self._ban_events.setdefault((str(account_id), str(target_id)), asyncio.Event())
        username = str((self.targets.get(target_id) or {}).get("username", "unknown"))
        self._set_current(campaign_id, account_id, target_id, cycle, "searching")
        self._set_task(campaign, account_id, target_id, {
            "phase": "opening", "current_cycle": cycle, "search_started": False,
        })
        try:
            self._raise_if_banned(account_id, target_id)
            self._set_task(campaign, account_id, target_id, {
                "phase": "search_pending", "search_started": True,
            })
            await client.send_message(entity, "/search")
            self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "search")
            self._raise_if_banned(account_id, target_id)
            self._set_task(campaign, account_id, target_id, {"phase": "after_search", "search_started": True})
            self._set_current(campaign_id, account_id, target_id, cycle, "waiting")
            await self._wait_for_cycle(self.timing.AFTER_SEARCH_SECONDS, ban_event, account_id, target_id)

            sticker_path = self._sticker_path(campaign["sticker_id"])
            self._set_current(campaign_id, account_id, target_id, cycle, "sending sticker")
            self._set_task(campaign, account_id, target_id, {"phase": "sticker_pending", "search_started": True})
            sticker = self.stickers.get(campaign["sticker_id"]) or {}
            await client.send_file(
                entity, str(sticker_path),
                attributes=self._sticker_attributes(sticker, sticker_path),
            )
            self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "sticker")
            self._raise_if_banned(account_id, target_id)
            self._set_task(campaign, account_id, target_id, {"phase": "after_sticker", "search_started": True})
            self._set_current(campaign_id, account_id, target_id, cycle, "waiting")
            await self._wait_for_cycle(self.timing.AFTER_STICKER_SECONDS, ban_event, account_id, target_id)

            rotation_index, promotion_text = await self._reserve_rotation(campaign_id)
            self._set_task(campaign, account_id, target_id, {
                "phase": "message_pending", "rotation_index": rotation_index,
                "search_started": True,
            })
            self._set_current(campaign_id, account_id, target_id, cycle, "sending message")
            await client.send_message(entity, promotion_text)
            self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "promotion_message")
            self._raise_if_banned(account_id, target_id)
            self._set_task(campaign, account_id, target_id, {"phase": "after_message", "search_started": True})
            self._set_current(campaign_id, account_id, target_id, cycle, "waiting")
            await self._wait_for_cycle(self.timing.AFTER_TEXT_SECONDS, ban_event, account_id, target_id)

            self._set_current(campaign_id, account_id, target_id, cycle, "stopping")
            self._set_task(campaign, account_id, target_id, {"phase": "stop_pending", "search_started": True})
            await client.send_message(entity, "/stop")
            self._record_scheduled_operation(campaign_id, account_id, target_id, cycle, "stop")
            self._raise_if_banned(account_id, target_id)
            self._set_task(campaign, account_id, target_id, {"phase": "after_stop", "search_started": False})
            self._set_current(campaign_id, account_id, target_id, cycle, "waiting")
            await self._wait_for_cycle(self.timing.AFTER_STOP_SECONDS, ban_event, account_id, target_id)
            self._finish_task(task, success=True)
            await self._notify(runtime, self.get(campaign_id))
            return True
        except _TargetAccessDenied:
            await self._await_restriction_persistence(account_id, target_id)
            restriction = self.ban_detection.result_for(account_id, target_id)
            if restriction is None:
                try:
                    restriction = self._task_restriction({"account_id": account_id, "target_id": target_id})
                except Exception:
                    restriction = None
            self._mark_target_restricted(campaign, account_id, target_id, restriction)
            await self._notify(runtime, self.get(campaign_id))
            return False
        except _RestrictionCheckUnavailable:
            self._fail_target_remaining(campaign, account_id, target_id, "restriction_store_unavailable")
            await self._notify(runtime, self.get(campaign_id))
            return False
        except FloodWaitError as exc:
            if self.ban_detection.result_for(account_id, target_id) is not None:
                await self._await_restriction_persistence(account_id, target_id)
                self._mark_target_restricted(
                    campaign, account_id, target_id, self.ban_detection.result_for(account_id, target_id)
                )
                await self._notify(runtime, self.get(campaign_id))
                return False
            current_task = self._task_for(campaign_id, account_id, target_id) or task
            if current_task.get("search_started") is True:
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
            self._set_task(campaign, account_id, target_id, {"flood_wait_seconds": wait_seconds})
            await asyncio.sleep(wait_seconds)
            if wait_id is not None:
                self._record_scheduled_event(
                    "flood_wait_completed", identity=f"{wait_id}:completed",
                    wait_id=wait_id, assignment_id=self._scheduled_runtimes[account_id].assignment_id,
                    campaign_id=campaign_id, account_id=account_id, target_id=target_id,
                )
            self._finish_task(task, success=False, error="FloodWaitError")
            await self._notify(runtime, self.get(campaign_id))
            logger.warning("Campaign account is waiting for Telegram FloodWait (%s seconds)", wait_seconds)
            return False
        except Exception as exc:
            if self.ban_detection.result_for(account_id, target_id) is not None:
                await self._await_restriction_persistence(account_id, target_id)
                self._mark_target_restricted(
                    campaign, account_id, target_id, self.ban_detection.result_for(account_id, target_id)
                )
                await self._notify(runtime, self.get(campaign_id))
                return False
            current_task = self._task_for(campaign_id, account_id, target_id) or task
            if current_task.get("search_started") is True:
                await self._send_stop(client, entity)
            logger.warning("Campaign target cycle failed (account_id=%s, target=%s, error=%s)", account_id, username, type(exc).__name__)
            self._finish_task(task, success=False, error=type(exc).__name__)
            await self._notify(runtime, self.get(campaign_id))
            return False

    async def _recovery_stop(self, task: dict) -> None:
        account_id = str(task.get("account_id", ""))
        target_id = str(task.get("target_id", ""))
        try:
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

    def _finish_task(self, task: dict, *, success: bool, error: str | None = None) -> None:
        task_id = str(task["id"])
        store = self._task_store(str(task.get("campaign_id", "")))
        current = store.get(task_id) or task
        cycle = int(current.get("current_cycle", current.get("next_cycle", 1)))
        # A task's next cycle advances once. Checkpoint recovery and a late worker
        # callback cannot count the same cycle twice.
        if current.get("phase") in {None, "idle"} and int(current.get("next_cycle", 1)) > cycle:
            return
        changes = {
            "next_cycle": cycle + 1,
            "successful": int(current.get("successful", 0)) + int(success),
            "failed": int(current.get("failed", 0)) + int(not success),
            "phase": "idle", "search_started": False,
            "last_error": error, "status": "complete" if success else "failed",
            "updated_at": self._now(),
        }
        store.update(task_id, changes)
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
        self._refresh_progress(str(current["campaign_id"]))

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
        try:
            cycles = int(campaign.get("total_cycles", 0))
        except (TypeError, ValueError):
            return None
        if cycles < 1 or cycles > 100000 or self.stickers.get(str(campaign.get("sticker_id", ""))) is None:
            return None
        changes = {}
        if campaign.get("account_ids") != account_ids:
            changes["account_ids"] = account_ids
        if campaign.get("total_cycles") != cycles:
            changes["total_cycles"] = cycles
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

    def _signal_ban(self, result: BanDetectionResult) -> None:
        key = (result.account_id, result.target_id)
        self._ban_events.setdefault(key, asyncio.Event()).set()
        if self.restriction_service is not None:
            try:
                loop = asyncio.get_running_loop()
                self._restriction_tasks[key] = loop.create_task(
                    self.restriction_service.handle_detection(result)
                )
            except RuntimeError:
                logger.exception("Could not schedule restriction persistence (detection_id=%s)", result.detection_id)

    def _raise_if_banned(self, account_id: str, target_id: str) -> None:
        target = self.targets.get(str(target_id)) or {}
        if self.ban_detection.result_for(account_id, target_id) is not None:
            raise _TargetAccessDenied
        try:
            restricted = self._restriction_for(account_id, target_id, target)
        except Exception as exc:
            raise _RestrictionCheckUnavailable from exc
        if restricted is not None:
            raise _TargetAccessDenied

    async def _await_restriction_persistence(self, account_id: str, target_id: str) -> None:
        task = self._restriction_tasks.get((str(account_id), str(target_id)))
        if task is not None:
            await asyncio.shield(task)

    async def _wait_for_cycle(
        self,
        seconds: float,
        ban_event: asyncio.Event,
        account_id: str,
        target_id: str,
    ) -> None:
        if ban_event.is_set():
            self._raise_if_banned(account_id, target_id)
        if seconds > 0:
            try:
                await asyncio.wait_for(ban_event.wait(), timeout=seconds)
            except asyncio.TimeoutError:
                return
            self._raise_if_banned(account_id, target_id)

    async def _notify(self, runtime: _Runtime, campaign: dict | None) -> None:
        if runtime.callback is not None and campaign is not None:
            async with runtime.notify_lock:
                try:
                    latest = self.get(str(campaign.get("id", ""))) or campaign
                    await runtime.callback(latest)
                except Exception as exc:
                    logger.debug("Campaign progress callback failed (error=%s)", type(exc).__name__)

    async def _progress_ticker(self, campaign_id: str, runtime: _Runtime) -> None:
        """Refresh the single dashboard message periodically during long cycles."""
        while True:
            await asyncio.sleep(5)
            campaign = self.get(campaign_id)
            if campaign is None or campaign.get("status") != "running":
                return
            await self._notify(runtime, campaign)

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
