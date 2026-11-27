"""Phase 7 scheduler foundation; workload allocation is intentionally separate."""

from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta, timezone
from enum import Enum
import asyncio
import logging
import time as time_module
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.storage.repositories.scheduler_repository import SchedulerRepository
from app.campaigns.execution_service import SCHEDULED_PROMOTION_ID


logger = logging.getLogger(__name__)
_RECONCILE_SECONDS = 10


class SchedulerState(str, Enum):
    DISABLED = "disabled"
    WAITING = "waiting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    AWAITING_RECOVERY_CONFIRMATION = "awaiting_recovery_confirmation"


@dataclass(frozen=True)
class SchedulerSettings:
    timezone: str = "Asia/Kolkata"
    operating_start: str = "15:00"
    operating_end: str = "03:00"
    peak_start: str = "18:00"
    peak_end: str = "23:00"
    outside_peak_concurrency_1_20: int = 1
    outside_peak_concurrency_21_50: int = 2
    outside_peak_concurrency_51_plus: int = 3
    peak_concurrency: int = 5
    parallel_target_concurrency: int = 3
    progress_report_time: str = "21:00"
    final_report_time: str = "03:15"


class SchedulerService:
    """Own durable scheduler configuration/state and expose Phase 7.2 interfaces."""

    def __init__(self, repository: SchedulerRepository, campaign_repository, account_repository,
                 execution_service, restriction_service, reporting_service=None) -> None:
        self.repository = repository
        self.campaign_repository = campaign_repository
        self.account_repository = account_repository
        self.execution_service = execution_service
        self.restriction_service = restriction_service
        self.daily_limit_service = execution_service.daily_limit_service
        self.reporting_service = reporting_service
        self.execution_service.bind_scheduled_promotion_repository(repository)
        stored_config = repository.get_config()
        defaults = asdict(SchedulerSettings())
        defaults.update({key: value for key, value in stored_config.items() if key in defaults})
        self._settings = SchedulerSettings(**defaults)
        self.repository.save_config(asdict(self._settings))
        self._ensure_scheduled_promotion_model()
        state = repository.get_state()
        self._enabled = bool(state.get("enabled", False))
        self._manually_paused = bool(state.get("manually_paused", False))
        previous_assignments = state.get("active_assignments") or {}
        lifecycle_was_active = state.get("lifecycle_status") in {"running", "starting"}
        legacy_enabled_state = "lifecycle_status" not in state and self._enabled
        previously_awaiting = bool(
            state.get("recovery_confirmation_required", False)
            or state.get("state") == SchedulerState.AWAITING_RECOVERY_CONFIRMATION.value
        )
        self._recovery_confirmation_required = bool(
            previously_awaiting
            or ((lifecycle_was_active or legacy_enabled_state)
                and (self._enabled or bool(previous_assignments)))
        )
        self._recovery_snapshot = dict(state.get("recovery_snapshot") or {})
        if self._recovery_confirmation_required and (
            not previously_awaiting or not self._recovery_snapshot
        ):
            self._recovery_snapshot = self._build_recovery_snapshot(
                previous_assignments, state.get("state", "unknown"), state.get("snapshot") or {}
            )
        self._recovery_decision = state.get("recovery_decision")
        self._recovery_notified = bool(state.get("recovery_notified", False)) if previously_awaiting else False
        self._recovery_no_repeat_pairs = {
            (str(item[0]), str(item[1])) for item in state.get("recovery_no_repeat_pairs", [])
            if isinstance(item, (list, tuple)) and len(item) == 2
        }
        self._recovery_no_repeat_day = state.get("recovery_no_repeat_day")
        self._lifecycle_status = "starting"
        self._state = (SchedulerState.AWAITING_RECOVERY_CONFIRMATION
                       if self._recovery_confirmation_required
                       else SchedulerState.PAUSED if self._manually_paused
                       else SchedulerState.DISABLED if not self._enabled
                       else SchedulerState.WAITING)
        self._persist_state()
        self._validate_settings(self._settings)
        self._coordinator: asyncio.Task | None = None
        self._shutdown_requested = False
        self._wake = asyncio.Event()
        self._reconcile_guard = asyncio.Lock()
        self._recovery_notification_guard = asyncio.Lock()
        self._workers: dict[str, tuple[str, asyncio.Task]] = {}
        self._assignment_counts: dict[tuple[str, str], int] = {}
        self._reconnect_attempts: dict[str, int] = {}
        self._reconnect_after: dict[str, float] = {}
        self._account_assignment_counts: dict[str, int] = {
            str(account_id): int(count) for account_id, count in (state.get("account_assignment_counts") or {}).items()
        }
        self._snapshot: dict = {}
        self._takeover_notifier = None
        self._report_notifier = None
        self._recovery_notifier = None
        self._report_delivery_tasks: set[asyncio.Task] = set()
        self._report_jobs: set[str] = set()
        self._report_retry_attempts: dict[str, int] = {}
        self._report_retry_after: dict[str, float] = {}
        if self.reporting_service is not None:
            self.reporting_service.seed_enabled_state(self._enabled)
            self.execution_service.set_scheduled_event_recorder(self.reporting_service.record_event)
            self._report_first_day = self.reporting_service.first_event_date()
            self._report_last_days = {
                kind: self.reporting_service.last_report_date(kind) for kind in ("progress", "final")
            }
            self._pending_report_records = {
                str(item["id"]): item for item in self.reporting_service.pending_reports()
            }
        else:
            self._report_first_day = None
            self._report_last_days = {}
            self._pending_report_records = {}
        if self.restriction_service is not None:
            self.restriction_service.add_detection_listener(self._restriction_changed)
        if self.daily_limit_service is not None:
            self.daily_limit_service.add_detection_listener(self._restriction_changed)

    @property
    def settings(self) -> SchedulerSettings:
        return self._settings

    @property
    def state(self) -> SchedulerState:
        return self._state

    @property
    def enabled(self) -> bool:
        return self._enabled

    def status(self) -> dict:
        return {"enabled": self._enabled, "state": self._state.value,
                "settings": asdict(self._settings), **self._snapshot,
                "manually_paused": self._manually_paused,
                "promotion": self.promotion_config(),
                "recovery_required": self._recovery_confirmation_required,
                "recovery": dict(self._recovery_snapshot),
                "recovery_decision": self._recovery_decision,
                "recovery_confirmation_status": (
                    "awaiting_confirmation" if self._recovery_confirmation_required
                    else (self._recovery_decision or "not_required")
                )}

    def set_enabled(self, enabled: bool) -> dict:
        self._enabled = bool(enabled)
        if enabled:
            self._manually_paused = False
        self._report_event("scheduler_enabled", identity=None, enabled=self._enabled)
        self._state = (
            SchedulerState.AWAITING_RECOVERY_CONFIRMATION
            if self._enabled and self._recovery_confirmation_required
            else SchedulerState.WAITING if self._enabled else SchedulerState.STOPPING
        )
        self._persist_state()
        if not self._enabled:
            self.execution_service.stop_scheduled_assignments()
            if self._coordinator is None or self._coordinator.done():
                self._state = SchedulerState.DISABLED
                self._persist_state()
        self._wake.set()
        return self.status()

    def promotion_config(self) -> dict:
        config = self.repository.get_config().get("promotion") or {}
        return dict(config) if isinstance(config, dict) else {}

    def _ensure_scheduled_promotion_model(self) -> dict | None:
        promotion = self.promotion_config()
        target_ids = promotion.get("target_ids") or (
            [promotion.get("target_id")] if promotion.get("target_id") else []
        )
        target_ids = list(dict.fromkeys(str(item) for item in target_ids if item))
        if not target_ids:
            return None
        config = self.repository.get_config()
        model = dict(config.get("scheduled_execution") or {})
        stickers = sorted(self.execution_service.stickers.list(), key=lambda item: str(item.get("id", "")))
        default_sticker_id = model.get("sticker_id")
        if not self.execution_service.scheduled_sticker_available(default_sticker_id):
            default_sticker_id = next((
                str(item["id"]) for item in stickers
                if self.execution_service.scheduled_sticker_available(str(item.get("id", "")))
            ), None)
        saved_configs = promotion.get("target_configs")
        target_configs = {}
        for target_id in target_ids:
            target = self.execution_service.targets.get(target_id) or {}
            if target.get("opted_in") is not True or not target.get("username"):
                return None
            saved = saved_configs.get(target_id) if isinstance(saved_configs, dict) else None
            if saved is None and isinstance(saved_configs, dict):
                return None
            if saved is None and default_sticker_id:
                saved = {
                    "target_id": target_id, "text_enabled": True,
                    "promotional_text": None, "legacy_rotating_text": True,
                    "sticker_enabled": True, "sticker_id": default_sticker_id,
                }
            if saved is None:
                saved = target.get("promotion_config")
            if not isinstance(saved, dict):
                return None
            validated = self.execution_service._target_content_config(
                {"target_configs": {target_id: saved}}, target_id,
            )
            if validated is None:
                return None
            target_configs[target_id] = validated
        if isinstance(saved_configs, dict) and set(saved_configs) != set(target_ids):
            return None
        first_target_id = target_ids[0]
        first_config = target_configs[first_target_id]
        execution_mode = promotion.get("execution_mode", "sequential")
        if execution_mode not in {"sequential", "parallel"}:
            return None
        model.update({
            "id": SCHEDULED_PROMOTION_ID, "name": str(promotion.get("name") or "Promotion"),
            "target_ids": target_ids, "target_id": first_target_id,
            "target_configs": target_configs,
            "execution_mode": execution_mode,
            "concurrency_limit": self._settings.parallel_target_concurrency,
            "account_ids": sorted(str(item["id"]) for item in self.account_repository.list()
                                   if item.get("id")),
            "sticker_id": first_config.get("sticker_id"), "total_cycles": 1,
        })
        config["scheduled_execution"] = model
        self.repository.save_config(config)
        return model

    async def save_promotion_config(
        self, name: str, target_ids: str | list[str], execution_mode: str = "sequential",
    ) -> dict:
        name = str(name).strip()
        if not name or len(name) > 80:
            raise ValueError("Promotion name must be between 1 and 80 characters")
        selected_ids = [str(target_ids)] if isinstance(target_ids, str) else [
            str(item) for item in target_ids
        ]
        selected_ids = list(dict.fromkeys(selected_ids))
        if not selected_ids:
            raise ValueError("Select at least one existing target with confirmed opt-in")
        if execution_mode not in {"sequential", "parallel"}:
            raise ValueError("Choose sequential or parallel target execution")
        previous = self.promotion_config()
        previous_ids = previous.get("target_ids") or (
            [previous.get("target_id")] if previous.get("target_id") else []
        )
        legacy_model = self.execution_service.scheduled_promotion_model() or {}
        target_configs = {}
        target_usernames = {}
        for target_id in selected_ids:
            target = self.execution_service.targets.get(target_id)
            if target is None or target.get("opted_in") is not True or not target.get("username"):
                raise ValueError("Every selected target must have confirmed opt-in")
            saved = target.get("promotion_config")
            if (
                not isinstance(saved, dict) and len(selected_ids) == 1
                and [str(item) for item in previous_ids] == [target_id]
                and self.execution_service.scheduled_sticker_available(legacy_model.get("sticker_id"))
            ):
                saved = {
                    "target_id": target_id, "text_enabled": True,
                    "promotional_text": None, "legacy_rotating_text": True,
                    "sticker_enabled": True, "sticker_id": legacy_model["sticker_id"],
                }
            validated = self.execution_service._target_content_config(
                {"target_configs": {target_id: saved}}, target_id,
            ) if isinstance(saved, dict) else None
            if validated is None:
                raise ValueError(f"Configure valid text and/or sticker content for @{target['username']} first")
            target_configs[target_id] = validated
            target_usernames[target_id] = str(target["username"])
        value = {
            "name": name, "target_id": selected_ids[0],
            "target_ids": selected_ids, "target_usernames": target_usernames,
            "target_configs": target_configs, "execution_mode": execution_mode,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        must_reconcile = (
            [str(item) for item in previous_ids] != selected_ids
            or previous.get("execution_mode", "sequential") != execution_mode
        ) and any(
            not task.done() for _, task in self._workers.values()
        )
        was_paused = self._manually_paused
        was_enabled = self._enabled
        if must_reconcile:
            self._manually_paused = True
            self._persist_state()
            self.execution_service.stop_scheduled_assignments()
            await self._reconcile()
        config = self.repository.get_config()
        config["promotion"] = value
        self.repository.save_config(config)
        self._ensure_scheduled_promotion_model()
        if not previous_ids and not self._recovery_confirmation_required:
            self._enabled = False
            self._manually_paused = False
            self._state = SchedulerState.DISABLED
            self._persist_state()
        if must_reconcile and not was_paused:
            self._manually_paused = False
            self._enabled = was_enabled
            self._state = SchedulerState.WAITING if was_enabled else SchedulerState.DISABLED
            self._persist_state()
        self._wake.set()
        if must_reconcile and was_enabled and not was_paused:
            await self._reconcile()
        return value

    async def start_promotion(self) -> dict:
        if not (self.promotion_config().get("target_ids") or self.promotion_config().get("target_id")):
            raise ValueError("Promotion not configured")
        if self._recovery_confirmation_required:
            raise ValueError("Confirm scheduler recovery before starting promotion")
        if not self._schedulable_campaigns():
            raise ValueError("The configured target or scheduled promotion content is unavailable.")
        promotion_model = self.execution_service.scheduled_promotion_model() or {}
        if promotion_model is None or not isinstance(promotion_model.get("target_configs"), dict):
            raise ValueError("Configure valid promotional text and/or a sticker for every opted-in target.")
        self._manually_paused = False
        self._enabled = True
        self._state = SchedulerState.WAITING
        self._report_event("scheduler_enabled", identity=None, enabled=True)
        self._persist_state()
        self._wake.set()
        await self._reconcile()
        return self.status()

    async def pause_promotion(self) -> dict:
        self._manually_paused = True
        self._state = SchedulerState.PAUSED
        self._persist_state()
        self.execution_service.stop_scheduled_assignments()
        self._wake.set()
        await self._reconcile()
        return self.status()

    async def resume_promotion(self) -> dict:
        if not (self.promotion_config().get("target_ids") or self.promotion_config().get("target_id")):
            raise ValueError("Promotion not configured")
        if self._recovery_confirmation_required:
            raise ValueError("Confirm scheduler recovery before resuming promotion")
        if not self._schedulable_campaigns():
            raise ValueError("The configured target or scheduled promotion content is unavailable.")
        promotion_model = self.execution_service.scheduled_promotion_model() or {}
        if promotion_model is None or not isinstance(promotion_model.get("target_configs"), dict):
            raise ValueError("Configure valid promotional text and/or a sticker for every opted-in target.")
        self._manually_paused = False
        self._enabled = True
        self._state = SchedulerState.WAITING
        self._persist_state()
        self._wake.set()
        await self._reconcile()
        return self.status()

    async def promotion_dashboard_data(self, page: int = 0, page_size: int = 5) -> dict:
        """Return dashboard values backed by current services and persisted records."""
        config = self.promotion_config()
        target_ids = config.get("target_ids") or (
            [config.get("target_id")] if config.get("target_id") else []
        )
        target_ids = [str(item) for item in target_ids if item]
        if not target_ids:
            return {"configured": False, "status": self.status()}
        targets = [self.execution_service.targets.get(target_id) or {} for target_id in target_ids]
        campaigns = self._schedulable_campaigns()
        campaign_ids = {str(item["id"]) for item in campaigns}
        account_ids = sorted(
            str(item.get("id")) for item in self.account_repository.list() if item.get("id")
        )
        accounts = {str(item.get("id")): item for item in self.account_repository.list()
                    if str(item.get("id")) in set(account_ids)}
        active = {str(account_id): (str(campaign_id), task)
                  for account_id, (campaign_id, task) in self._workers.items()
                  if not task.done() and str(campaign_id) in campaign_ids}
        all_events = ([item for item in self.reporting_service.repository.list()
                       if item.get("record_type") == "event"]
                      if self.reporting_service is not None else [])
        scheduled_tasks = self.execution_service.task_records(SCHEDULED_PROMOTION_ID)
        now = datetime.now(ZoneInfo(self._settings.timezone))
        operating_day = self._operating_day(now)
        window_start = datetime.combine(
            operating_day, self._parse_time(self._settings.operating_start), now.tzinfo,
        )
        window_end = datetime.combine(
            operating_day, self._parse_time(self._settings.operating_end), now.tzinfo,
        )
        if self._parse_time(self._settings.operating_start) > self._parse_time(self._settings.operating_end):
            window_end += timedelta(days=1)
        effective_end = min(now, window_end)
        scheduled_campaign_ids = {SCHEDULED_PROMOTION_ID}
        parsed_events = []
        for item in all_events:
            if str(item.get("campaign_id", "")) not in scheduled_campaign_ids:
                continue
            if item.get("target_id") is not None and str(item.get("target_id")) not in set(target_ids):
                continue
            try:
                occurred = datetime.fromisoformat(str(item["occurred_at"]))
                if occurred.tzinfo is None:
                    occurred = occurred.replace(tzinfo=timezone.utc)
                occurred = occurred.astimezone(now.tzinfo)
            except (KeyError, ValueError):
                continue
            if window_start <= occurred <= effective_end:
                parsed_events.append({**item, "_occurred": occurred})
        tasks_by_account: dict[str, list[dict]] = {}
        for task in scheduled_tasks:
            account_id = str(task.get("account_id", ""))
            tasks_by_account.setdefault(account_id, []).append(task)
        lifetime_by_account = {
            account_id: sum(max(0, int(task.get("successful", 0))) for task in tasks)
            for account_id, tasks in tasks_by_account.items()
        }
        window_by_account: dict[str, int] = {}
        for event in parsed_events:
            if event.get("event_type") == "cycle_completed":
                account_id = str(event.get("account_id", ""))
                window_by_account[account_id] = window_by_account.get(account_id, 0) + 1

        completed_waits = {
            str(item.get("wait_id")) for item in all_events
            if item.get("event_type") == "flood_wait_completed" and item.get("wait_id")
        }
        rate_limited_until: dict[str, datetime] = {}
        for event in all_events:
            if event.get("record_type") != "event" or event.get("event_type") != "flood_wait":
                continue
            wait_id = str(event.get("wait_id", ""))
            if not wait_id or wait_id in completed_waits:
                continue
            try:
                wait_start = datetime.fromisoformat(str(event["occurred_at"]))
                if wait_start.tzinfo is None:
                    wait_start = wait_start.replace(tzinfo=timezone.utc)
                until = wait_start.astimezone(now.tzinfo) + timedelta(
                    seconds=max(0, int(event.get("seconds", 0)))
                )
            except (KeyError, ValueError, TypeError):
                continue
            account_id = str(event.get("account_id", ""))
            if until > now and until > rate_limited_until.get(account_id, now):
                rate_limited_until[account_id] = until

        restricted_pairs = self._restricted_pairs([{
            "account_ids": account_ids, "target_ids": target_ids,
        }])
        restricted_by_account = {
            str(item["account_id"]): item for item in restricted_pairs
            if item.get("status") != "Daily Limit Reached"
        }
        current = self.status()
        eligible = set(str(item) for item in current.get("eligible_account_ids", [])) & set(account_ids)
        page_count = max(1, (len(account_ids) + page_size - 1) // page_size)
        page = min(max(0, int(page)), page_count - 1)
        account_rows = []
        all_account_rows = []
        for account_id in account_ids:
            account = accounts.get(account_id) or {}
            tasks = tasks_by_account.get(account_id, [])
            worker = active.get(account_id)
            daily_limits = []
            if self.daily_limit_service is not None:
                for target in targets:
                    daily_record = self.daily_limit_service.active_for(
                        account_id, target.get("telegram_user_id"),
                        str(target.get("username", "")),
                    )
                    if daily_record is not None and all(
                        item.get("id") != daily_record.get("id") for item in daily_limits
                    ):
                        daily_limits.append(daily_record)
            all_targets_daily_limited = bool(target_ids) and len(daily_limits) >= len(target_ids)
            last_error = next((str(task.get("last_error")) for task in sorted(
                tasks, key=lambda value: str(value.get("updated_at", "")), reverse=True
            ) if task.get("last_error")), None)
            restriction = restricted_by_account.get(account_id)
            if self._recovery_confirmation_required:
                account_state = "Waiting"
            elif restriction:
                account_state = "Restricted"
            elif all_targets_daily_limited and not worker:
                account_state = "Daily Limit Reached"
            elif account_id in rate_limited_until:
                account_state = "Rate Limited"
            elif account.get("authenticated") is not True:
                account_state = "Unavailable"
            elif not await self.execution_service.client_manager.is_connected(account_id) or not await self.execution_service.client_manager.is_authorized(account_id):
                account_state = "Unavailable"
            elif self._manually_paused:
                account_state = "Paused"
            elif worker:
                account_state = "Running"
            else:
                account_state = "Waiting"
            all_account_rows.append({
                "account_id": account_id,
                "account_name": str(account.get("name") or account.get("phone") or "Unnamed account"),
                "status": account_state,
                "completed_cycles": lifetime_by_account.get(account_id, 0),
                "window_completed_cycles": window_by_account.get(account_id, 0),
                "last_activity": max((str(task.get("updated_at", "")) for task in tasks), default=None),
                "last_error": restriction.get("reason") if restriction else last_error,
                "daily_limits": [
                    {
                        "target_username": item.get("target_username", "unknown"),
                        "matched_phrase": item.get("matched_phrase", ""),
                        "next_eligibility_at": item.get("next_eligibility_at", ""),
                    }
                    for item in daily_limits
                ],
                "rate_limit_seconds": max(
                    0, int((rate_limited_until[account_id] - now).total_seconds())
                ) if account_id in rate_limited_until else None,
            })
        account_rows = all_account_rows[page * page_size:(page + 1) * page_size]
        state_counts = {
            state: sum(1 for row in all_account_rows if row["status"] == state)
            for state in (
                "Running", "Waiting", "Paused", "Restricted", "Daily Limit Reached",
                "Rate Limited", "Unavailable",
            )
        }
        return {
            "configured": True, "promotion": config,
            "target_username": ", @".join(
                str(target.get("username", "unavailable")).lstrip("@") for target in targets
            ),
            "target_usernames": [str(target.get("username", "unavailable")) for target in targets],
            "status": current, "accounts": account_rows, "account_page": page,
            "account_pages": page_count, "account_count": len(account_ids),
            "promotion_content_available": bool(
                self.execution_service.scheduled_promotion_model()
                and (self.execution_service.scheduled_promotion_model() or {}).get("target_configs")
            ),
            "eligible_total": len(eligible), "running_accounts": state_counts["Running"],
            "idle_accounts": state_counts["Waiting"],
            "paused_accounts": state_counts["Paused"],
            "completed_cycles": sum(max(0, int(task.get("successful", 0))) for task in scheduled_tasks),
            "window_completed_cycles": sum(window_by_account.values()),
            "restricted_accounts": state_counts["Restricted"],
            "daily_limited_accounts": state_counts["Daily Limit Reached"],
            "rate_limited_accounts": state_counts["Rate Limited"],
            "unavailable_accounts": state_counts["Unavailable"],
            "restricted_pairs": restricted_pairs,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        }

    async def refresh_promotion_dashboard_data(self, page: int = 0, page_size: int = 5) -> dict:
        """Serialize an immediate refresh through the single scheduler coordinator."""
        await self._reconcile()
        return await self.promotion_dashboard_data(page, page_size)

    def update_settings(self, **changes) -> SchedulerSettings:
        values = {**asdict(self._settings), **changes}
        unknown = set(changes) - set(asdict(SchedulerSettings()))
        if unknown:
            raise ValueError(f"Unknown scheduler setting: {sorted(unknown)[0]}")
        settings = SchedulerSettings(**values)
        self._validate_settings(settings)
        self.repository.save_config(asdict(settings))
        self._settings = settings
        self._wake.set()
        return settings

    def restore_default_settings(self) -> SchedulerSettings:
        """Restore the scheduler's built-in defaults and persist them as one update."""
        return self.update_settings(**asdict(SchedulerSettings()))

    def set_campaign_enabled(self, campaign_id: str, enabled: bool) -> dict:
        campaign = self.campaign_repository.get(str(campaign_id))
        if campaign is None:
            raise KeyError("Campaign not found")
        targets = campaign.get("target_ids") or []
        if enabled:
            if campaign.get("status") == "running":
                raise ValueError("Pause or stop the campaign before enabling scheduling")
            if not targets:
                raise ValueError("Campaign has no targets")
            account_ids = campaign.get("account_ids") or ([campaign.get("account_id")] if campaign.get("account_id") else [])
            accounts = {str(item.get("id")): item for item in self.account_repository.list()}
            if not account_ids or any(
                accounts.get(str(account_id), {}).get("authenticated") is not True
                for account_id in account_ids
            ):
                raise ValueError("Every scheduled account must be authenticated")
            for target_id in targets:
                target = self.execution_service.targets.get(str(target_id))
                if target is None or target.get("opted_in") is not True:
                    raise ValueError("Every scheduled target must have confirmed opt-in")
                for account_id in account_ids:
                    restriction = self.restriction_service.active_for(
                        str(account_id), str(target_id),
                        target_bot_id=target.get("telegram_user_id"),
                        username=target.get("username"),
                    )
                    if restriction is not None:
                        raise ValueError("A campaign account-target pair is restricted")
        updated = self.campaign_repository.update(str(campaign_id), {
            "scheduling_enabled": bool(enabled),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        self._wake.set()
        return updated

    def enabled_campaigns(self) -> list[dict]:
        return [campaign for campaign in self.campaign_repository.list()
                if campaign.get("scheduling_enabled") is True]

    def eligible_accounts(self, campaign: dict | None = None) -> list[dict]:
        """Read-only Phase 7.2 seam; does not allocate or take over accounts."""
        if campaign is None:
            return [item for item in self.account_repository.list()
                    if item.get("authenticated") is True]
        ids = {str(value) for value in campaign.get("account_ids", [])}
        return [item for item in self.account_repository.list()
                if str(item.get("id")) in ids and item.get("authenticated") is True]

    def account_execution_lock(self, account_id: str):
        """Return the existing manual campaign lock for future scheduled workers."""
        return self.execution_service.account_execution_lock(str(account_id))

    def set_takeover_notifier(self, notifier) -> None:
        self._takeover_notifier = notifier

    def set_report_notifier(self, notifier) -> None:
        self._report_notifier = notifier

    def set_recovery_notifier(self, notifier) -> None:
        self._recovery_notifier = notifier

    async def notify_recovery_required(self) -> bool:
        """Deliver the durable recovery prompt once; retry on a later startup if delivery fails."""
        async with self._recovery_notification_guard:
            if not self._recovery_confirmation_required or self._recovery_notified:
                return False
            if self._recovery_notifier is None:
                return False
            # Persist the one-shot claim before sending so a restart cannot duplicate a
            # notification whose Telegram delivery succeeded just before a process exit.
            self._recovery_notified = True
            self._persist_state()
            try:
                delivered = await self._recovery_notifier(self.status())
                if delivered is False:
                    self._recovery_notified = False
                    self._persist_state()
                    return False
                return True
            except Exception as exc:
                self._recovery_notified = False
                self._persist_state()
                logger.warning("Could not deliver scheduler recovery prompt (error=%s)", type(exc).__name__)
                return False

    async def confirm_recovery(self, resume: bool) -> dict:
        """Apply an owner decision and, when requested, immediately reconcile in-window work."""
        if not self._recovery_confirmation_required:
            return {"accepted": False, "reason": "no_recovery_pending", **self.status()}
        now = datetime.now(ZoneInfo(self._settings.timezone))
        self._validate_settings(self._settings)
        self._recovery_confirmation_required = False
        self._recovery_notified = True
        self._recovery_decision = {
            "decision": "resume" if resume else "keep_stopped",
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }
        if resume:
            self._enabled = True
            self._manually_paused = False
            self._state = SchedulerState.WAITING
            self._recovery_no_repeat_day = self._operating_day(now).isoformat()
            self._recovery_no_repeat_pairs.update(
                (str(item.get("account_id", "")), str(item.get("campaign_id", "")))
                for item in (self._recovery_snapshot.get("assignments") or [])
            )
        else:
            self._enabled = False
            self._state = SchedulerState.DISABLED
        self._persist_state()
        self._wake.set()
        eligible_accounts = set()
        campaign_ids = set()
        if resume:
            try:
                campaigns = self._schedulable_campaigns()
                eligible = await self._eligible_by_account(campaigns)
                eligible_accounts = set(eligible)
                campaign_ids = {str(item["id"]) for rows in eligible.values() for item in rows}
                if self._period(now) != "inactive":
                    # The coordinator and explicit confirmation share this guard, so there is
                    # exactly one allocator and no duplicate account workers.
                    await self._reconcile()
            except Exception:
                logger.exception("Scheduler recovery reconciliation failed")
                self._recovery_confirmation_required = True
                self._state = SchedulerState.AWAITING_RECOVERY_CONFIRMATION
                self._recovery_decision = None
                self._persist_state()
                return {"accepted": False, "reason": "revalidation_failed", **self.status()}
        return {
            "accepted": True,
            "inside_operating_window": self._period(now) != "inactive",
            "eligible_account_count": len(eligible_accounts),
            "eligible_campaign_count": len(campaign_ids),
            **self.status(),
        }

    def _build_recovery_snapshot(self, assignments: dict, previous_state: str,
                                 previous_snapshot: dict) -> dict:
        affected = []
        for account_id, assignment in assignments.items():
            if isinstance(assignment, dict):
                campaign_id = str(assignment.get("campaign_id", ""))
                assignment_id = assignment.get("assignment_id")
            else:
                campaign_id = str(assignment)
                assignment_id = None
            campaign = self.execution_service.get(str(campaign_id)) or {}
            tasks = self.execution_service._scheduled_task_snapshot(str(campaign_id), str(account_id))
            affected.append({
                "assignment_id": assignment_id,
                "campaign_id": str(campaign_id),
                "campaign_name": campaign.get("name", "Unknown campaign"),
                "account_id": str(account_id),
                "account_name": self._account_name(str(account_id)),
                "tasks": tasks,
                "progress": self.execution_service._account_progress(str(campaign_id), str(account_id)),
            })
        return {
            "interrupted": bool(affected),
            "assignments": affected,
            "last_known_scheduler_status": previous_state,
            "last_operating_period": previous_snapshot.get("operating_period", "unknown"),
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

    def _account_name(self, account_id: str) -> str:
        account = self.account_repository.get(account_id) or {}
        return str(account.get("name") or account.get("phone") or "Unnamed account")

    def _operating_day(self, now: datetime):
        local = now.astimezone(ZoneInfo(self._settings.timezone))
        if local.timetz().replace(tzinfo=None) < self._parse_time(self._settings.operating_start):
            return local.date() - timedelta(days=1)
        return local.date()

    async def start(self) -> None:
        """Start the single lifecycle-owned scheduler coordinator."""
        if self._coordinator is not None and not self._coordinator.done():
            return
        self._shutdown_requested = False
        self._lifecycle_status = "running"
        self._persist_state()
        self._coordinator = asyncio.create_task(self._coordinate(), name="campaign-scheduler")

    async def stop(self) -> None:
        """Gracefully stop coordinator and let active safe execution units finish."""
        if self._coordinator is None:
            return
        was_enabled = self._enabled
        self._state = SchedulerState.STOPPING
        self._persist_state()
        self.execution_service.stop_scheduled_assignments()
        self._wake.set()
        self._shutdown_requested = True
        self._wake.set()
        await self._coordinator
        self._coordinator = None
        workers = [task for _, task in self._workers.values()]
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self._workers.clear()
        report_tasks = list(self._report_delivery_tasks)
        if report_tasks:
            await asyncio.gather(*report_tasks, return_exceptions=True)
        self._state = (SchedulerState.PAUSED if self._manually_paused
                       else SchedulerState.WAITING if was_enabled else SchedulerState.DISABLED)
        self._lifecycle_status = "clean"
        self._persist_state()

    def _restriction_changed(self, _record: dict) -> None:
        self._wake.set()

    def _report_event(self, event_type: str, *, identity: str | None = None, **values) -> None:
        if self.reporting_service is not None:
            self.reporting_service.record_event(event_type, identity=identity, **values)

    def _schedule_due_reports(self, now: datetime) -> None:
        if self.reporting_service is None:
            return
        if any(not task.done() for task in self._report_delivery_tasks):
            return
        if self._report_notifier is not None:
            for report_key, report in list(self._pending_report_records.items()):
                if not report_key or report_key in self._report_jobs:
                    continue
                if time_module.monotonic() < self._report_retry_after.get(report_key, 0):
                    return
                self._report_jobs.add(report_key)
                task = asyncio.create_task(self._claim_and_deliver_report(report, report_key),
                                           name=f"scheduled-report-pending-{report_key[-10:]}")
                self._report_delivery_tasks.add(task)
                task.add_done_callback(self._report_delivery_tasks.discard)
                return
        zone = ZoneInfo(self._settings.timezone)
        local_now = now.astimezone(zone)
        for kind, setting_name in (
            ("progress", "progress_report_time"),
            ("final", "final_report_time"),
        ):
            report_time = self._parse_time(getattr(self._settings, setting_name))
            hour, minute = report_time.hour, report_time.minute
            last_day = self._report_last_days.get(kind)
            day = last_day + timedelta(days=1) if last_day else self._report_first_day
            while day <= local_now.date():
                deadline = datetime.combine(day, time(hour, minute), zone)
                if deadline <= local_now:
                    report_key = f"report:{kind}:{day.isoformat()}"
                    if report_key not in self._report_jobs:
                        self._report_jobs.add(report_key)
                        task = asyncio.create_task(
                            self._generate_and_deliver_report(
                                kind, day, report_key,
                                delivery_time=f"{hour:02d}:{minute:02d}",
                            ),
                            name=f"scheduled-report-{kind}-{day.isoformat()}",
                        )
                        self._report_delivery_tasks.add(task)
                        task.add_done_callback(self._report_delivery_tasks.discard)
                    return
                day += timedelta(days=1)

    async def _generate_and_deliver_report(
        self, kind, day, report_key: str, *, delivery_time: str,
    ) -> None:
        try:
            report = await asyncio.to_thread(
                self.reporting_service.build_daily_report, kind, day,
                scheduler_enabled=self._enabled,
                operating_start=self._settings.operating_start,
                operating_end=self._settings.operating_end,
                delivery_time=delivery_time,
                scheduler_status=self._state.value,
            )
            last_day = self._report_last_days.get(kind)
            self._report_last_days[kind] = day if last_day is None else max(last_day, day)
            if self._report_notifier is None:
                self._pending_report_records[str(report["id"])] = report
                return
            delivery = await asyncio.to_thread(
                self.reporting_service.claim_delivery, str(report["id"]),
            )
            if delivery is not None and self._report_notifier is not None:
                await self._deliver_report(delivery)
            else:
                self._pending_report_records.pop(str(report["id"]), None)
        except Exception as exc:
            self._report_event("scheduler_error", reason=f"report_{kind}_{type(exc).__name__}")
            logger.warning("Scheduled report background job failed (kind=%s, error=%s)",
                           kind, type(exc).__name__)
        finally:
            self._report_jobs.discard(report_key)

    async def _claim_and_deliver_report(self, report: dict, report_key: str) -> None:
        try:
            if self._report_notifier is None:
                return
            delivery = await asyncio.to_thread(
                self.reporting_service.claim_delivery, report_key,
            )
            if delivery is not None and self._report_notifier is not None:
                await self._deliver_report(delivery)
            else:
                self._pending_report_records.pop(report_key, None)
        except Exception as exc:
            self._report_event("scheduler_error", reason=f"report_delivery_{type(exc).__name__}")
            logger.warning(
                "Scheduled report delivery background job failed (error=%s)",
                type(exc).__name__,
            )
        finally:
            self._report_jobs.discard(report_key)

    async def _deliver_report(self, report: dict) -> None:
        try:
            result = await self._report_notifier(report)
            if result is False:
                await asyncio.to_thread(
                    self.reporting_service.mark_delivery, str(report["id"]),
                    sent=False, error="control_bot_unavailable",
                )
                self._pending_report_records[str(report["id"])] = report
                report_id = str(report["id"])
                attempts = self._report_retry_attempts.get(report_id, 0) + 1
                self._report_retry_attempts[report_id] = attempts
                delay = min(900.0, 30.0 * (2 ** min(attempts - 1, 5)))
                self._report_retry_after[report_id] = time_module.monotonic() + delay
            else:
                await asyncio.to_thread(
                    self.reporting_service.mark_delivery, str(report["id"]), sent=True,
                )
                report_id = str(report["id"])
                self._pending_report_records.pop(report_id, None)
                self._report_retry_attempts.pop(report_id, None)
                self._report_retry_after.pop(report_id, None)
        except Exception as exc:
            await asyncio.to_thread(
                self.reporting_service.mark_delivery, str(report["id"]),
                sent=False, error=type(exc).__name__,
            )
            self._pending_report_records.pop(str(report["id"]), None)
            logger.warning("Could not deliver scheduled report (error=%s)", type(exc).__name__)

    async def _coordinate(self) -> None:
        """Reconcile time window, eligible accounts, assignments and worker state."""
        while True:
            try:
                await self._reconcile()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Scheduler reconciliation failed (error=%s)", type(exc).__name__)
                self._report_event("scheduler_error", reason=type(exc).__name__)
            if self._shutdown_requested:
                break
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=_RECONCILE_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def _reconcile(self) -> None:
        async with self._reconcile_guard:
            await self._reconcile_once()

    async def _reconcile_once(self) -> None:
        now = datetime.now(ZoneInfo(self._settings.timezone))
        if self.daily_limit_service is not None:
            await asyncio.to_thread(self.daily_limit_service.expire_due)
        self._schedule_due_reports(now)
        period = self._period(now)
        if self._shutdown_requested:
            self.execution_service.stop_scheduled_assignments()
            await self._settle_workers(reset_rounds=False)
            self.execution_service.release_scheduled_account_claims()
            self._state = SchedulerState.STOPPING
            self._persist_state()
            return
        if not self._enabled or self._recovery_confirmation_required:
            if self._recovery_confirmation_required:
                await self.notify_recovery_required()
            self.execution_service.stop_scheduled_assignments()
            await self._settle_workers()
            self.execution_service.release_scheduled_account_claims()
            self._state = (SchedulerState.AWAITING_RECOVERY_CONFIRMATION
                           if self._enabled else SchedulerState.DISABLED)
            campaigns = self._schedulable_campaigns()
            eligible = sorted(await self._eligible_by_account(campaigns))
            maximum = self._concurrency(len(eligible), period) if period != "inactive" else 0
            self._update_snapshot(period, eligible, [], eligible, [], now,
                                  restricted=self._restricted_pairs(campaigns), maximum=maximum)
            self._persist_state()
            return
        if self._manually_paused:
            self.execution_service.stop_scheduled_assignments()
            await self._settle_workers(reset_rounds=False)
            self.execution_service.release_scheduled_account_claims()
            campaigns = self._schedulable_campaigns()
            eligible = sorted(await self._eligible_by_account(campaigns))
            self._state = SchedulerState.PAUSED
            self._update_snapshot(period, eligible, [], eligible, [], now,
                                  restricted=self._restricted_pairs(campaigns), maximum=0)
            self._persist_state()
            return
        if period == "inactive":
            self.execution_service.stop_scheduled_assignments()
            await self._settle_workers()
            self.execution_service.release_scheduled_account_claims()
            self._state = SchedulerState.WAITING
            campaigns = self._schedulable_campaigns()
            eligible = sorted(await self._eligible_by_account(campaigns))
            self._update_snapshot(period, eligible, [], eligible, [], now,
                                  restricted=self._restricted_pairs(campaigns), maximum=0)
            self._persist_state()
            return

        campaigns = self._schedulable_campaigns()
        eligible_by_account = await self._eligible_by_account(campaigns)
        now = datetime.now(ZoneInfo(self._settings.timezone))
        period = self._period(now)
        if not self._enabled or self._recovery_confirmation_required or period == "inactive":
            self.execution_service.stop_scheduled_assignments()
            await self._settle_workers()
            self._state = (
                SchedulerState.AWAITING_RECOVERY_CONFIRMATION
                if self._enabled and self._recovery_confirmation_required
                else SchedulerState.WAITING if self._enabled else SchedulerState.DISABLED
            )
            self._update_snapshot(period, [], [], [], [], now)
            self._persist_state()
            return
        eligible_ids = set(eligible_by_account)
        self.execution_service.release_scheduled_account_claims(keep=eligible_ids)
        maximum = min(self._concurrency(len(eligible_ids), period), len(eligible_ids))
        if self.promotion_config().get("execution_mode", "sequential") == "sequential":
            maximum = min(maximum, 1)

        # Retire completed work and start another configured-cycle round.
        for account_id, (campaign_id, task) in list(self._workers.items()):
            if task.done():
                try:
                    task.result()
                except Exception as exc:
                    logger.warning("Scheduled account worker failed (account_id=%s, error=%s)", account_id, type(exc).__name__)
                if (str(account_id), str(campaign_id)) not in self._recovery_no_repeat_pairs:
                    self.execution_service.reset_scheduled_account_round(
                        campaign_id, account_id, target_ids=self._configured_target_ids(),
                    )
                self._workers.pop(account_id, None)

        # Existing workers keep their safe execution unit; no new work is assigned
        # to an account that became unavailable or lost every eligible pair.
        for account_id, (campaign_id, _task) in list(self._workers.items()):
            if account_id not in eligible_ids or campaign_id not in {
                c["id"] for c in eligible_by_account.get(account_id, [])
            }:
                runtime = self.execution_service._scheduled_runtimes.get(account_id)
                if runtime is not None:
                    runtime.stop.set()

        active_accounts = set(self._workers)
        available = sorted(eligible_ids - active_accounts,
                           key=lambda account_id: (self._account_assignment_counts.get(account_id, 0), account_id))
        slots = max(0, maximum - len(active_accounts))
        started = 0
        for account_id in available:
            if started >= slots:
                break
            choices = eligible_by_account[account_id]
            choices = [item for item in choices
                       if (str(account_id), str(item["id"])) not in self._recovery_no_repeat_pairs]
            if not choices:
                continue
            campaign = min(choices, key=lambda item: (
                sum(1 for assigned_campaign, _ in self._workers.values()
                    if assigned_campaign == str(item["id"])),
                self._assignment_counts.get((account_id, str(item["id"])), 0),
                str(item["id"]),
            ))
            campaign_id = str(campaign["id"])
            takeover = await self.execution_service.claim_scheduled_account(account_id)
            if not takeover.get("ok"):
                self.execution_service.release_scheduled_account_claim(account_id)
                self._report_event(
                    "work_skipped",
                    identity=f"takeover-skip:{datetime.now(ZoneInfo(self._settings.timezone)).date()}:{account_id}",
                    account_id=account_id, campaign_id=campaign_id,
                    reason="account_takeover_or_safe_suspension_failed",
                )
                continue
            for suspended in takeover.get("suspended", []):
                if self._takeover_notifier is not None:
                    try:
                        await self._takeover_notifier(suspended)
                    except Exception as exc:
                        logger.warning("Could not notify owner about normal campaign suspension (error=%s)", type(exc).__name__)
            boundary_now = datetime.now(ZoneInfo(self._settings.timezone))
            if (not self._enabled or self._recovery_confirmation_required
                    or self._period(boundary_now) == "inactive"):
                self.execution_service.release_scheduled_account_claim(account_id)
                break
            task = self.execution_service.start_scheduled_assignment(
                campaign_id, account_id, target_ids=campaign.get("target_ids"),
            )
            if task is not None:
                self._workers[account_id] = (campaign_id, task)
                task.add_done_callback(lambda _task: self._wake.set())
                key = (account_id, campaign_id)
                self._assignment_counts[key] = self._assignment_counts.get(key, 0) + 1
                self._account_assignment_counts[account_id] = self._account_assignment_counts.get(account_id, 0) + 1
                started += 1
            else:
                self.execution_service.release_scheduled_account_claim(account_id)
                self._report_event(
                    "work_skipped",
                    identity=f"assignment-skip:{datetime.now(ZoneInfo(self._settings.timezone)).date()}:{account_id}:{campaign_id}",
                    account_id=account_id, campaign_id=campaign_id,
                    reason="scheduled_assignment_could_not_start",
                )

        active_accounts = set(self._workers)
        idle = sorted(eligible_ids - active_accounts)
        running_campaigns = sorted({campaign_id for campaign_id, _ in self._workers.values()})
        restricted = self._restricted_pairs(campaigns)
        self._state = SchedulerState.RUNNING if active_accounts else SchedulerState.WAITING
        self._update_snapshot(period, sorted(eligible_ids), sorted(active_accounts), idle,
                              running_campaigns, now, restricted=restricted, maximum=maximum)
        self._persist_state()

    async def _settle_workers(self, *, reset_rounds: bool = True) -> None:
        workers = [task for _, task in self._workers.values()]
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        for account_id, (campaign_id, task) in list(self._workers.items()):
            if (reset_rounds and task.done()
                    and (str(account_id), str(campaign_id)) not in self._recovery_no_repeat_pairs):
                self.execution_service.reset_scheduled_account_round(
                    campaign_id, account_id, target_ids=self._configured_target_ids(),
                )
        self._workers.clear()

    def _schedulable_campaigns(self) -> list[dict]:
        config = self.promotion_config()
        target_ids = config.get("target_ids") or (
            [config.get("target_id")] if config.get("target_id") else []
        )
        target_ids = list(dict.fromkeys(str(item) for item in target_ids if item))
        targets = [self.execution_service.targets.get(target_id) or {} for target_id in target_ids]
        model = self.execution_service.scheduled_promotion_model()
        if (not target_ids or any(
            target.get("opted_in") is not True or not target.get("username") for target in targets
        ) or model is None):
            return []
        account_ids = sorted(str(item.get("id")) for item in self.account_repository.list()
                             if item.get("id"))
        if account_ids != sorted(str(item) for item in model.get("account_ids", [])):
            model["account_ids"] = account_ids
            self.execution_service._update_campaign(SCHEDULED_PROMOTION_ID, {"account_ids": account_ids})
            model = self.execution_service.scheduled_promotion_model() or model
        return [{**model, "id": SCHEDULED_PROMOTION_ID,
                 "target_ids": target_ids, "account_ids": account_ids,
                 "execution_mode": config.get("execution_mode", "sequential")}]

    def _configured_target_ids(self) -> list[str] | None:
        config = self.promotion_config()
        target_ids = config.get("target_ids") or (
            [config.get("target_id")] if config.get("target_id") else []
        )
        return [str(item) for item in target_ids if item] or None

    def _reporting_active_now(self) -> bool:
        return bool(self.reporting_service is not None and self._enabled
                    and not self._recovery_confirmation_required
                    and self._period(datetime.now(ZoneInfo(self._settings.timezone))) != "inactive")

    async def _eligible_by_account(self, campaigns: list[dict]) -> dict[str, list[dict]]:
        current_day = self._operating_day(datetime.now(ZoneInfo(self._settings.timezone))).isoformat()
        if self._recovery_no_repeat_day and current_day != self._recovery_no_repeat_day:
            self._recovery_no_repeat_pairs.clear()
            self._recovery_no_repeat_day = None
        accounts = {str(item.get("id")): item for item in self.account_repository.list()
                    if item.get("authenticated") is True}
        eligible: dict[str, list[dict]] = {}
        manager = self.execution_service.client_manager
        for campaign in campaigns:
            for account_id in campaign.get("account_ids", []):
                account_id = str(account_id)
                if (str(campaign.get("id", "")) != SCHEDULED_PROMOTION_ID
                        and (account_id, str(campaign.get("id", ""))) in self._recovery_no_repeat_pairs):
                    tasks = [task for task in self.execution_service.task_records(str(campaign["id"]))
                             if str(task.get("account_id")) == account_id]
                    if tasks and all(
                        task.get("status") == "restricted"
                        or int(task.get("next_cycle", 1)) > int(campaign.get("total_cycles", 1))
                        for task in tasks
                    ):
                        continue
                if account_id not in accounts:
                    self._record_skip(account_id, "account_not_authenticated")
                    continue
                if not await manager.is_connected(account_id):
                    if time_module.monotonic() < self._reconnect_after.get(account_id, 0):
                        self._record_skip(account_id, "account_unavailable")
                        continue
                    try:
                        connected = await manager.connect(account_id)
                    except Exception as exc:
                        logger.warning(
                            "Could not reconnect scheduled account (account_id=%s, error=%s)",
                            account_id, type(exc).__name__,
                        )
                        connected = False
                    if not connected:
                        attempts = self._reconnect_attempts.get(account_id, 0) + 1
                        self._reconnect_attempts[account_id] = attempts
                        delay = min(300.0, _RECONCILE_SECONDS * (2 ** min(attempts - 1, 5)))
                        self._reconnect_after[account_id] = time_module.monotonic() + delay
                        self._record_skip(account_id, "account_unavailable")
                        continue
                    self._reconnect_attempts.pop(account_id, None)
                    self._reconnect_after.pop(account_id, None)
                if not await manager.is_authorized(account_id):
                    self._record_skip(account_id, "account_unavailable")
                    continue
                viable_pair = False
                for target_id in campaign.get("target_ids", []):
                    target = self.execution_service.targets.get(str(target_id)) or {}
                    if target.get("opted_in") is not True:
                        self._record_skip(account_id, "target_not_opted_in", str(target_id))
                        continue
                    daily_limit = (
                        self.daily_limit_service.active_for(
                            account_id, target.get("telegram_user_id"),
                            str(target.get("username", "")),
                        )
                        if self.daily_limit_service is not None else None
                    )
                    if daily_limit is not None:
                        self._record_skip(
                            account_id, "daily_chat_limit_active", str(target_id),
                            str(daily_limit.get("id", "unknown")),
                        )
                        continue
                    try:
                        restriction = self.restriction_service.active_for(
                            account_id, str(target_id), target_bot_id=target.get("telegram_user_id"),
                            username=target.get("username"),
                        ) if self.restriction_service is not None else None
                    except Exception:
                        restriction = {"status": "unknown"}
                    if restriction is None:
                        viable_pair = True
                        break
                    self._record_skip(account_id, "account_target_restricted", str(target_id),
                                      str(restriction.get("id", "unknown")))
                if viable_pair:
                    eligible.setdefault(account_id, []).append(campaign)
        return eligible

    def _record_skip(self, account_id: str, reason: str, target_id: str = "", restriction_id: str = "") -> None:
        if not self._reporting_active_now():
            return
        day = datetime.now(ZoneInfo(self._settings.timezone)).date().isoformat()
        identity = f"skip:{day}:{account_id}:{reason}:{target_id}:{restriction_id}"
        event_type = "target_restriction" if reason == "account_target_restricted" else (
            "account_unavailable" if reason in {"account_not_authenticated", "account_unavailable"} else "work_skipped"
        )
        values = {"account_id": str(account_id), "reason": reason}
        if target_id:
            values["target_id"] = str(target_id)
        if target_id and reason == "account_target_restricted":
            values["restriction_key"] = f"{account_id}:{target_id}"
        self._report_event(event_type, identity=identity, **values)

    def _restricted_pairs(self, campaigns: list[dict]) -> list[dict]:
        pairs_by_key = {}
        for campaign in campaigns:
            for account_id in campaign.get("account_ids", []):
                for target_id in campaign.get("target_ids", []):
                    target = self.execution_service.targets.get(str(target_id)) or {}
                    daily_limit = (
                        self.daily_limit_service.active_for(
                            str(account_id), target.get("telegram_user_id"),
                            str(target.get("username", "")),
                        )
                        if self.daily_limit_service is not None else None
                    )
                    if daily_limit is not None:
                        restriction = daily_limit
                    else:
                        try:
                            restriction = self.restriction_service.active_for(
                                str(account_id), str(target_id),
                                target_bot_id=target.get("telegram_user_id"),
                                username=target.get("username"),
                            ) if self.restriction_service is not None else None
                        except Exception:
                            restriction = {"status": "unknown"}
                    if restriction is not None:
                        key = (str(account_id), str(target_id))
                        pairs_by_key[key] = {
                            "account_id": key[0], "target_id": key[1],
                            "status": restriction.get("status", "restricted"),
                            "reason": (
                                restriction.get("detection_reason")
                                or restriction.get("matched_phrase")
                                or restriction.get("reason")
                            ),
                        }
        return list(pairs_by_key.values())

    def _update_snapshot(self, period: str, eligible: list[str], running: list[str], idle: list[str],
                         campaigns: list[str], now: datetime, *, restricted: list[dict] | None = None,
                         maximum: int = 0) -> None:
        self._snapshot = {
            "operating_period": period,
            "eligible_account_count": len(eligible),
            "eligible_account_ids": eligible,
            "maximum_concurrency": maximum,
            "running_scheduled_accounts": running,
            "idle_eligible_accounts": idle,
            "active_scheduled_campaigns": campaigns,
            "restricted_or_excluded_pairs": restricted or [],
            "next_transition": self._next_transition(now).isoformat(),
        }

    def _period(self, now: datetime) -> str:
        local_time = now.timetz().replace(tzinfo=None)
        start = self._parse_time(self._settings.operating_start)
        end = self._parse_time(self._settings.operating_end)
        peak_start = self._parse_time(self._settings.peak_start)
        peak_end = self._parse_time(self._settings.peak_end)
        active = self._time_in_window(local_time, start, end)
        if not active:
            return "inactive"
        return "peak" if self._time_in_window(local_time, peak_start, peak_end) else "outside_peak"

    @staticmethod
    def _time_in_window(value: time, start: time, end: time) -> bool:
        return (value >= start or value < end) if start > end else start <= value < end

    def _next_transition(self, now: datetime) -> datetime:
        candidates = []
        for date in (now.date(), (now + timedelta(days=1)).date()):
            for name in ("operating_start", "operating_end"):
                candidate = datetime.combine(date, self._parse_time(getattr(self._settings, name)), now.tzinfo)
                if candidate > now:
                    candidates.append(candidate)
        return min(candidates) if candidates else now + timedelta(hours=1)

    def _concurrency(self, eligible_count: int, period: str) -> int:
        if eligible_count <= 0:
            return 0
        if period == "peak":
            return self._settings.peak_concurrency
        if eligible_count <= 20:
            return self._settings.outside_peak_concurrency_1_20
        if eligible_count <= 50:
            return self._settings.outside_peak_concurrency_21_50
        return self._settings.outside_peak_concurrency_51_plus

    @staticmethod
    def _parse_time(value: str) -> time:
        hour, minute = (int(part) for part in value.split(":"))
        return time(hour, minute)

    def _persist_state(self) -> None:
        self.repository.save_state({
            "enabled": self._enabled,
            "manually_paused": self._manually_paused,
            "state": self._state.value,
            "recovery_confirmation_required": self._recovery_confirmation_required,
            "recovery_snapshot": getattr(self, "_recovery_snapshot", {}),
            "recovery_decision": getattr(self, "_recovery_decision", None),
            "recovery_notified": getattr(self, "_recovery_notified", False),
            "recovery_no_repeat_pairs": [list(item) for item in sorted(
                getattr(self, "_recovery_no_repeat_pairs", set())
            )],
            "recovery_no_repeat_day": getattr(self, "_recovery_no_repeat_day", None),
            "lifecycle_status": getattr(self, "_lifecycle_status", "starting"),
            "active_assignments": {
                account_id: {
                    "campaign_id": campaign_id,
                    "assignment_id": getattr(
                        getattr(self.execution_service, "_scheduled_runtimes", {}).get(account_id),
                        "assignment_id", None,
                    ),
                }
                for account_id, (campaign_id, task) in getattr(self, "_workers", {}).items()
                if not task.done()
            },
            "snapshot": getattr(self, "_snapshot", {}),
            "account_assignment_counts": getattr(self, "_account_assignment_counts", {}),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

    @staticmethod
    def _validate_settings(settings: SchedulerSettings) -> None:
        try:
            ZoneInfo(settings.timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("Invalid scheduler timezone") from exc
        minute_values = {}
        for name in (
            "operating_start", "operating_end", "peak_start", "peak_end",
            "progress_report_time", "final_report_time",
        ):
            value = getattr(settings, name)
            try:
                hour, minute = (int(part) for part in value.split(":"))
            except (ValueError, AttributeError) as exc:
                raise ValueError(f"Invalid time for {name}") from exc
            if hour not in range(24) or minute not in range(60):
                raise ValueError(f"Invalid time for {name}")
            minute_values[name] = hour * 60 + minute
        if not (1 <= settings.outside_peak_concurrency_1_20 <= 20
                and 1 <= settings.outside_peak_concurrency_21_50 <= 50
                and 1 <= settings.outside_peak_concurrency_51_plus
                and 1 <= settings.peak_concurrency <= 5
                and 1 <= settings.parallel_target_concurrency <= 5):
            raise ValueError("Scheduler concurrency settings are outside supported limits")
        operating_duration = (
            minute_values["operating_end"] - minute_values["operating_start"]
        ) % (24 * 60)
        peak_duration = (minute_values["peak_end"] - minute_values["peak_start"]) % (24 * 60)
        peak_offset = (
            minute_values["peak_start"] - minute_values["operating_start"]
        ) % (24 * 60)
        if (
            operating_duration == 0 or peak_duration == 0
            or peak_offset >= operating_duration
            or peak_offset + peak_duration > operating_duration
        ):
            raise ValueError("Peak hours must fall entirely within operating hours")
