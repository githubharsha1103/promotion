"""Scheduled-only daily reporting backed by durable JSON event records."""

from datetime import date, datetime, time, timedelta, timezone
import asyncio
import logging
import uuid
from zoneinfo import ZoneInfo

from app.storage.repositories.scheduled_reporting_repository import ScheduledReportingRepository
from app.campaigns.execution_service import SCHEDULED_PROMOTION_ID


logger = logging.getLogger(__name__)


class ScheduledReportingService:
    def __init__(self, repository: ScheduledReportingRepository, execution_service,
                 campaign_repository, timezone_name: str = "Asia/Kolkata") -> None:
        self.repository = repository
        self.execution_service = execution_service
        self.campaign_repository = campaign_repository
        self.timezone_name = timezone_name
        self._event_queue: asyncio.Queue[dict | None] = asyncio.Queue()
        self._event_writer_task: asyncio.Task | None = None
        self._event_ids: set[str] = set()
        self._queued_event_ids: set[str] = set()

    async def start(self) -> None:
        """Load event identities off-loop and start the single persistence worker."""
        if self._event_writer_task is not None and not self._event_writer_task.done():
            return
        records = await asyncio.to_thread(self.repository.list)
        self._event_ids = {
            str(item["id"]) for item in records
            if item.get("record_type") == "event" and item.get("id")
        }
        self._event_writer_task = asyncio.create_task(
            self._write_events(), name="scheduled-report-event-writer",
        )

    async def stop(self) -> None:
        """Persist queued events before the application closes."""
        task = self._event_writer_task
        if task is None:
            return
        await self._event_queue.join()
        await self._event_queue.put(None)
        await task
        self._event_writer_task = None

    async def _write_events(self) -> None:
        while True:
            record = await self._event_queue.get()
            try:
                if record is None:
                    return
                await asyncio.to_thread(self.repository.put_once, record)
                self._event_ids.add(str(record["id"]))
            except Exception as exc:
                logger.warning(
                    "Could not persist scheduled report event (type=%s, error=%s)",
                    record.get("event_type", "unknown") if record is not None else "shutdown",
                    type(exc).__name__,
                )
            finally:
                if record is not None:
                    self._queued_event_ids.discard(str(record["id"]))
                self._event_queue.task_done()

    def record_event(self, event_type: str, *, identity: str | None = None, **values) -> dict | None:
        """Queue one execution or scheduler event without blocking campaign work."""
        try:
            record_id = str(identity or uuid.uuid4().hex)
            occurred_at = values.pop("occurred_at", datetime.now(timezone.utc).isoformat())
            record = {
                "id": f"event:{record_id}", "record_type": "event", "event_type": event_type,
                "occurred_at": occurred_at, **values,
            }
            if record["id"] in self._event_ids or record["id"] in self._queued_event_ids:
                return record
            if self._event_writer_task is not None and not self._event_writer_task.done():
                self._queued_event_ids.add(record["id"])
                self._event_queue.put_nowait(record)
                return record
            saved = self.repository.put_once(record)[0]
            self._event_ids.add(record["id"])
            return saved
        except Exception as exc:
            logger.warning("Could not persist scheduled report event (type=%s, error=%s)",
                           event_type, type(exc).__name__)
            return None

    def close_orphaned_assignments(self) -> None:
        """Close reporting intervals left open by a prior process, without resuming work."""
        events = [item for item in self.repository.list() if item.get("record_type") == "event"]
        starts = {str(item.get("assignment_id")): item for item in events
                  if item.get("event_type") == "assignment_started"}
        finished = {str(item.get("assignment_id")) for item in events
                    if item.get("event_type") == "assignment_finished"}
        for assignment_id, start in starts.items():
            if not assignment_id or assignment_id in finished:
                continue
            related = [item for item in events if str(item.get("assignment_id")) == assignment_id]
            last = max(related, key=lambda item: item.get("occurred_at", ""), default=start)
            campaign_id, account_id = str(start.get("campaign_id", "")), str(start.get("account_id", ""))
            tasks = [dict(task) for task in self.execution_service.task_records(campaign_id)
                     if str(task.get("account_id")) == account_id]
            campaign = self.campaign_repository.get(campaign_id) or {}
            total_cycles = int(start.get("total_cycles", campaign.get("total_cycles", 1)))
            unfinished = any(
                int(task.get("next_cycle", 1)) <= total_cycles
                and task.get("status") != "restricted" for task in tasks
            )
            self.record_event(
                "assignment_finished", identity=f"assignment-finish:{assignment_id}",
                assignment_id=assignment_id, campaign_id=campaign_id, account_id=account_id,
                interrupted=unfinished, tasks=tasks, occurred_at=last.get("occurred_at"),
            )

    def seed_enabled_state(self, enabled: bool) -> None:
        self.record_event("scheduler_enabled", identity="scheduler-enabled-initial",
                          enabled=bool(enabled))

    def latest(self, kind: str) -> dict | None:
        reports = [item for item in self.repository.list()
                   if item.get("record_type") == "report" and item.get("report_kind") == kind]
        return max(reports, key=lambda item: item.get("window_end", ""), default=None)

    def last_report_date(self, kind: str) -> date | None:
        report = self.latest(kind)
        try:
            return date.fromisoformat(str(report.get("report_date"))) if report else None
        except ValueError:
            return None

    def first_event_date(self) -> date:
        zone = ZoneInfo(self.timezone_name)
        dates = []
        for item in self.repository.list():
            if item.get("record_type") != "event":
                continue
            try:
                dates.append(datetime.fromisoformat(item["occurred_at"]).astimezone(zone).date())
            except (KeyError, ValueError):
                continue
        return min(dates, default=datetime.now(zone).date())

    def get_report(self, report_id: str) -> dict | None:
        item = self.repository.get(report_id)
        return item if item and item.get("record_type") == "report" else None

    def pending_reports(self) -> list[dict]:
        return [item for item in self.repository.list()
                if item.get("record_type") == "report" and item.get("delivery_status") == "pending"]

    def build_daily_report(
        self, kind: str, report_date: date, *, scheduler_enabled: bool,
        operating_start: str = "15:00", operating_end: str = "03:00",
        delivery_time: str | None = None, scheduler_status: str | None = None,
    ) -> dict:
        """Build a daily report with local-time windows and persist it idempotently."""
        zone = ZoneInfo(self.timezone_name)
        operating_start_time = self._parse_time(operating_start)
        operating_end_time = self._parse_time(operating_end)
        if kind == "progress":
            delivery = self._parse_time(delivery_time or "21:00")
            label_kind = "progress"
        elif kind == "final":
            delivery = self._parse_time(delivery_time or "03:15")
            label_kind = "final"
        else:
            raise ValueError("Unknown scheduled report kind")
        delivery_local = datetime.combine(report_date, delivery, zone)
        start_date = report_date if delivery >= operating_start_time else report_date - timedelta(days=1)
        start_local = datetime.combine(start_date, operating_start_time, zone)
        end_date = start_date + timedelta(days=int(operating_end_time <= operating_start_time))
        operating_end_local = datetime.combine(end_date, operating_end_time, zone)
        end_local = min(delivery_local, operating_end_local)
        label = (
            f"{delivery_local.strftime('%I:%M %p').lstrip('0')} IST "
            f"{label_kind} report"
        )
        start_utc, end_utc = start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)
        report_id = f"report:{kind}:{report_date.isoformat()}"
        existing = self.get_report(report_id)
        if existing is not None:
            return existing
        report = self._build(start_utc, end_utc, scheduler_enabled=scheduler_enabled,
                             title=label, report_id=report_id, kind=kind,
                             date_key=report_date.isoformat(), scheduler_status=scheduler_status)
        return self.repository.put_once(report)[0]

    def build_current_report(
        self, now: datetime | None = None, *, scheduler_enabled: bool,
        operating_start: str = "15:00", operating_end: str = "03:00",
        scheduler_status: str | None = None,
    ) -> dict:
        zone = ZoneInfo(self.timezone_name)
        now_local = (now or datetime.now(zone)).astimezone(zone)
        start_time, end_time = self._parse_time(operating_start), self._parse_time(operating_end)
        start_date = now_local.date() if now_local.timetz().replace(tzinfo=None) >= start_time \
            else now_local.date() - timedelta(days=1)
        start_local = datetime.combine(start_date, start_time, zone)
        end_date = start_date + timedelta(days=int(end_time <= start_time))
        end_local = min(now_local, datetime.combine(end_date, end_time, zone))
        return self._build(start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc),
                           scheduler_enabled=scheduler_enabled, title="Promotion operating-window report",
                           report_id=None, kind="current", date_key=None,
                           scheduler_status=scheduler_status)

    def claim_delivery(self, report_id: str) -> dict | None:
        """Persist an at-most-once delivery claim before contacting Telegram."""
        current = self.get_report(report_id)
        if current is None:
            return None
        token = uuid.uuid4().hex
        claimed_at = datetime.now(timezone.utc).isoformat()
        updated = self.repository.update(report_id, lambda record: (
            {**record, "delivery_status": "claimed", "delivery_claimed_at": claimed_at,
             "delivery_claim_token": token}
            if record.get("delivery_status") == "pending" else record
        ))
        return updated if updated.get("delivery_claim_token") == token else None

    def mark_delivery(self, report_id: str, *, sent: bool, error: str | None = None) -> None:
        current = self.get_report(report_id)
        if current is None:
            return
        self.repository.update(report_id, {
            "delivery_status": ("sent" if sent else
                                "pending" if error == "control_bot_unavailable" else "failed"),
            "delivery_error": error,
            "delivery_updated_at": datetime.now(timezone.utc).isoformat(),
        })

    def format_report(self, report: dict) -> str:
        stats = report.get("statistics", {})
        account_workload = stats.get("account_workload", {})
        accounts = {
            str(item.get("id")): item
            for item in self.execution_service.accounts.list()
            if item.get("id")
        }
        account_lines = []
        for account_id, values in sorted(
            account_workload.items(),
            key=lambda item: str(
                (accounts.get(str(item[0])) or {}).get("name")
                or (accounts.get(str(item[0])) or {}).get("phone")
                or "Unnamed account"
            ).casefold(),
        ):
            account = accounts.get(str(account_id), {})
            name = str(account.get("name") or account.get("phone") or "Unnamed account")
            account_lines.append(
                f"• {name}: {values.get('window_completed_cycles', values.get('completed_cycles', 0))} "
                f"this window · {values.get('lifetime_completed_cycles', 0)} lifetime"
            )
        state_names = {
            "running": "Running", "waiting": "Waiting", "paused": "Paused",
            "disabled": "Stopped", "stopping": "Stopping",
            "awaiting_recovery_confirmation": "Awaiting recovery",
        }
        current_state = state_names.get(
            str(report.get("scheduler_status") or ""),
            "Enabled" if report.get("scheduler_enabled") else "Stopped",
        )
        target_names = stats.get("target_usernames", [])
        lines = [
            f"📊 {report.get('title', 'Promotion report')}",
            f"🕒 {report.get('window_start_local')} – {report.get('window_end_local')}",
            f"⚙️ Scheduler: {current_state}",
            f"🔁 Completed cycles: {stats.get('promotion_completed_cycles', stats.get('completed_cycles', 0))} "
            "this window · "
            f"{stats.get('lifetime_completed_cycles', 0)} lifetime",
            f"👥 Account availability: {stats.get('authenticated_accounts', 0)}/"
            f"{stats.get('total_accounts', 0)} authenticated",
            f"🔴 Unavailable: {stats.get('unavailable_accounts', 0)}",
            f"🚫 Restricted: {stats.get('account_level_restrictions', 0)} accounts · "
            f"{stats.get('target_restrictions_detected', 0)} target pairs · "
            f"⏳ Rate limited: {stats.get('rate_limited_accounts', 0)}",
            "📈 Account cycles:",
        ]
        if target_names:
            lines.insert(2, f"🤖 Targets: {', '.join('@' + name for name in target_names)}")
        lines.extend(account_lines or ["• No account cycle activity recorded."])
        return "\n".join(lines)

    def split_report(self, report: dict, limit: int = 3900) -> list[str]:
        return self.split_text(self.format_report(report), limit)

    @staticmethod
    def split_text(text: str, limit: int = 3900) -> list[str]:
        chunks, current = [], ""
        for line in text.splitlines():
            addition = ("\n" if current else "") + line
            if current and len(current) + len(addition) > limit:
                chunks.append(current)
                current = line
            else:
                current += addition
        if current:
            chunks.append(current)
        return chunks or ["No scheduled campaign data is available."]

    def _build(
        self, start_utc: datetime, end_utc: datetime, *, scheduler_enabled: bool,
        title: str, report_id: str | None, kind: str, date_key: str | None,
        scheduler_status: str | None = None,
    ) -> dict:
        events = []
        all_records = self.repository.list()
        for record in all_records:
            if record.get("record_type") != "event":
                continue
            try:
                occurred = datetime.fromisoformat(str(record["occurred_at"]))
                if occurred.tzinfo is None:
                    occurred = occurred.replace(tzinfo=timezone.utc)
            except (KeyError, ValueError):
                continue
            if start_utc <= occurred.astimezone(timezone.utc) <= end_utc:
                events.append(record)
        events.sort(key=lambda item: item.get("occurred_at", ""))
        zone = ZoneInfo(self.timezone_name)
        campaigns = {str(item.get("id")): item for item in self.campaign_repository.list()}
        scheduled_names = {str(item.get("campaign_id")): str(item.get("campaign_name"))
                           for item in events if item.get("campaign_id") and item.get("campaign_name")}
        starts = {}
        ends = {}
        distinct_accounts = set()
        campaign_ids = set()
        restricted_accounts = set()
        unavailable_accounts = set()
        restriction_keys = set()
        completed_cycles = failed_cycles = activity = other_wait_count = 0
        interrupted_assignments = set()
        failed_assignments = 0
        scheduler_errors, skipped = [], []
        account_workload = {}
        promotion_window_by_account: dict[str, int] = {}
        promotion_completed_cycles = 0
        for event in events:
            kind_name = event.get("event_type")
            account_id = str(event.get("account_id", ""))
            if kind_name == "assignment_started":
                starts[str(event.get("assignment_id"))] = event
                distinct_accounts.add(account_id)
                campaign_ids.add(str(event.get("campaign_id", "")))
                account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0,
                                                         "activity": 0, "active_seconds": 0})
            elif kind_name == "assignment_finished":
                ends[str(event.get("assignment_id"))] = event
                if event.get("interrupted"):
                    interrupted_assignments.add(str(event.get("assignment_id")))
            elif kind_name == "assignment_stop_requested":
                if event.get("unfinished"):
                    interrupted_assignments.add(str(event.get("assignment_id")))
            elif kind_name == "scheduled_execution_error":
                failed_assignments += 1
                distinct_accounts.add(account_id)
                campaign_ids.add(str(event.get("campaign_id", "")))
                row = account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0, "activity": 0})
                row["failed_cycles"] += 1
            elif kind_name == "cycle_completed":
                completed_cycles += 1
                distinct_accounts.add(account_id)
                campaign_ids.add(str(event.get("campaign_id", "")))
                row = account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0, "activity": 0})
                row["completed_cycles"] += 1
                if str(event.get("campaign_id", "")) == SCHEDULED_PROMOTION_ID:
                    promotion_completed_cycles += 1
                    promotion_window_by_account[account_id] = (
                        promotion_window_by_account.get(account_id, 0) + 1
                    )
            elif kind_name == "cycle_failed":
                failed_cycles += 1
                distinct_accounts.add(account_id)
                campaign_ids.add(str(event.get("campaign_id", "")))
                account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0, "activity": 0})["failed_cycles"] += 1
            elif kind_name == "task_failed_remaining":
                failed_cycles += max(0, int(event.get("cycles", 0)))
                distinct_accounts.add(account_id)
                campaign_ids.add(str(event.get("campaign_id", "")))
                account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0, "activity": 0})["failed_cycles"] += max(0, int(event.get("cycles", 0)))
            elif kind_name == "telegram_operation":
                activity += 1
                distinct_accounts.add(account_id)
                campaign_ids.add(str(event.get("campaign_id", "")))
                row = account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0, "activity": 0})
                row["activity"] += 1
            elif kind_name == "account_unavailable":
                unavailable_accounts.add(account_id)
                skipped.append({"account_id": account_id or None,
                                "campaign_id": event.get("campaign_id"),
                                "reason": event.get("reason", "account_unavailable")})
            elif kind_name == "account_restricted":
                restricted_accounts.add(account_id)
                skipped.append({"account_id": account_id or None,
                                "campaign_id": event.get("campaign_id"),
                                "reason": event.get("reason", "account_restricted")})
            elif kind_name == "target_restriction":
                restriction_keys.add(str(event.get("restriction_key") or f"{account_id}:{event.get('target_id')}"))
                skipped.append({"account_id": account_id or None,
                                "campaign_id": event.get("campaign_id"),
                                "reason": "account_target_restricted"})
            elif kind_name == "platform_wait":
                other_wait_count += 1
            elif kind_name == "scheduler_error":
                scheduler_errors.append({"reason": event.get("reason", "unknown")})
            elif kind_name == "work_skipped":
                skipped.append({"account_id": account_id or None,
                                "campaign_id": event.get("campaign_id"),
                                "reason": event.get("reason", "unknown")})

        flood_ends = {str(item.get("wait_id")): item for item in all_records
                      if item.get("record_type") == "event" and item.get("event_type") == "flood_wait_completed"}
        flood_count = wait_seconds = 0
        flood_accounts = set()
        for event in all_records:
            if event.get("record_type") != "event" or event.get("event_type") != "flood_wait":
                continue
            try:
                wait_start = datetime.fromisoformat(str(event["occurred_at"])).astimezone(timezone.utc)
            except (KeyError, ValueError):
                continue
            wait_id = str(event.get("wait_id", ""))
            completion = flood_ends.get(wait_id)
            try:
                wait_end = (datetime.fromisoformat(str(completion["occurred_at"])).astimezone(timezone.utc)
                            if completion else wait_start + timedelta(seconds=max(0, int(event.get("seconds", 0)))))
            except (KeyError, ValueError):
                wait_end = wait_start + timedelta(seconds=max(0, int(event.get("seconds", 0))))
            overlap_start, overlap_end = max(wait_start, start_utc), min(wait_end, end_utc)
            if overlap_end > overlap_start:
                flood_count += 1
                wait_seconds += int((overlap_end - overlap_start).total_seconds())
                if event.get("account_id"):
                    flood_accounts.add(str(event["account_id"]))

        intervals = []
        utilization_seconds = {}
        account_periods = {}
        for assignment_id, start_event in starts.items():
            try:
                begin = datetime.fromisoformat(start_event["occurred_at"]).astimezone(timezone.utc)
            except (KeyError, ValueError):
                continue
            finish_event = ends.get(assignment_id)
            try:
                finish = datetime.fromisoformat(finish_event["occurred_at"]).astimezone(timezone.utc) if finish_event else end_utc
            except (KeyError, ValueError):
                finish = end_utc
            begin, finish = max(begin, start_utc), min(max(finish, begin), end_utc)
            if finish > begin:
                account_id = str(start_event.get("account_id", ""))
                intervals.append((begin, finish, account_id))
                account_periods.setdefault(account_id, []).append((begin, finish))
                distinct_accounts.add(account_id)
                campaign_ids.add(str(start_event.get("campaign_id", "")))

        peak = 0
        points = []
        for begin, finish, _account_id in intervals:
            points.extend(((begin, 1), (finish, -1)))
        active = 0
        for _instant, change in sorted(points, key=lambda item: (item[0], item[1])):
            active += change
            peak = max(peak, active)
        window_seconds = max(1, int((end_utc - start_utc).total_seconds()))
        for account_id, spans in account_periods.items():
            spans.sort()
            merged = []
            for begin, finish in spans:
                if merged and begin <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], finish))
                else:
                    merged.append((begin, finish))
            utilization_seconds[account_id] = sum(int((finish - begin).total_seconds()) for begin, finish in merged)
            workload = account_workload.setdefault(account_id, {"completed_cycles": 0, "failed_cycles": 0,
                                                                 "activity": 0})
            workload["active_seconds"] = utilization_seconds[account_id]
            workload["utilization_percent"] = round(100 * utilization_seconds[account_id] / window_seconds, 1)
        denominator = window_seconds * max(1, len(distinct_accounts))
        utilization = round(100 * sum(utilization_seconds.values()) / denominator, 1) if distinct_accounts else 0

        suspended_tasks, unfinished_cycles = self._unfinished_snapshot(events, starts, ends, end_utc)
        enabled_at_window = scheduler_enabled
        state_events = [item for item in all_records
                        if item.get("record_type") == "event" and item.get("event_type") == "scheduler_enabled"]
        prior = []
        for item in state_events:
            try:
                stamp = datetime.fromisoformat(item["occurred_at"]).astimezone(timezone.utc)
            except (KeyError, ValueError):
                continue
            if stamp <= start_utc:
                prior.append((stamp, bool(item.get("enabled"))))
        transitions = [(datetime.fromisoformat(item["occurred_at"]).astimezone(timezone.utc), bool(item.get("enabled")))
                       for item in events if item.get("event_type") == "scheduler_enabled"]
        if prior:
            enabled_at_window = sorted(prior)[-1][1]
        if transitions:
            enabled_at_window = sorted(transitions)[-1][1]

        stats = {
            "scheduled_activity": bool(starts or completed_cycles or failed_cycles or activity),
            "distinct_accounts_used": len({item for item in distinct_accounts if item}),
            "accounts_restricted_or_unavailable": len({item for item in restricted_accounts | unavailable_accounts if item}),
            "account_level_restrictions": len({item for item in restricted_accounts if item}),
            "target_restrictions_detected": len(restriction_keys),
            "unavailable_accounts": len({item for item in unavailable_accounts if item}),
            "scheduled_campaigns_executed": len({item for item in campaign_ids if item}),
            "scheduled_campaign_names": sorted({
                str(campaigns[item].get("name", item)) if item in campaigns
                else scheduled_names.get(item, item)
                for item in campaign_ids
            }),
            "completed_cycles": completed_cycles,
            "promotion_completed_cycles": promotion_completed_cycles,
            "failed_executions": failed_cycles,
            "failed_assignments": failed_assignments,
            "interrupted_executions": len({item for item in interrupted_assignments if item}),
            "suspended_tasks": suspended_tasks,
            "unfinished_cycles": unfinished_cycles,
            "promotion_activity": activity,
            "peak_concurrency": peak,
            "account_utilization_percent": utilization,
            "flood_wait_count": flood_count,
            "platform_wait_seconds": wait_seconds,
            "other_platform_wait_count": other_wait_count,
            "scheduler_errors": len(scheduler_errors),
            "scheduler_error_details": scheduler_errors[:20],
            "skipped_work": skipped[:40],
            "account_workload": account_workload,
        }
        target_usernames = []
        for target_id in sorted({
            str(item.get("target_id")) for item in events if item.get("target_id")
        }):
            target = self.execution_service.targets.get(target_id) or {}
            if target.get("username"):
                target_usernames.append(str(target["username"]).lstrip("@"))
        lifetime_by_account: dict[str, int] = {}
        for task in self.execution_service.task_records(SCHEDULED_PROMOTION_ID):
            account_id = str(task.get("account_id", ""))
            if account_id:
                lifetime_by_account[account_id] = (
                    lifetime_by_account.get(account_id, 0)
                    + max(0, int(task.get("successful", 0)))
                )
        account_records = [
            item for item in self.execution_service.accounts.list()
            if item.get("id")
        ]
        for account in account_records:
            account_id = str(account["id"])
            workload = account_workload.setdefault(
                account_id, {"completed_cycles": 0, "failed_cycles": 0, "activity": 0},
            )
            workload["window_completed_cycles"] = promotion_window_by_account.get(account_id, 0)
            workload["lifetime_completed_cycles"] = lifetime_by_account.get(account_id, 0)
        stats.update({
            "lifetime_completed_cycles": sum(lifetime_by_account.values()),
            "authenticated_accounts": sum(1 for item in account_records if item.get("authenticated") is True),
            "total_accounts": len(account_records),
            "rate_limited_accounts": len(flood_accounts),
            "target_usernames": target_usernames,
        })
        local_start, local_end = start_utc.astimezone(zone), end_utc.astimezone(zone)
        report = {
            "id": report_id or f"manual:{uuid.uuid4().hex}", "record_type": "report",
            "report_kind": kind, "report_date": date_key, "title": title,
            "window_start": start_utc.isoformat(), "window_end": end_utc.isoformat(),
            "window_start_local": local_start.strftime("%Y-%m-%d %I:%M %p %Z"),
            "window_end_local": local_end.strftime("%Y-%m-%d %I:%M %p %Z"),
            "scheduler_enabled": enabled_at_window, "statistics": stats,
            "scheduler_status": scheduler_status,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "delivery_status": "pending" if report_id else "manual",
        }
        if not report_id:
            report["id"] = f"manual:{uuid.uuid4().hex}"
        return report

    @staticmethod
    def _parse_time(value: str) -> time:
        try:
            hour, minute = (int(part) for part in value.split(":"))
            if hour not in range(24) or minute not in range(60):
                raise ValueError
            return time(hour, minute)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid report time: {value}") from exc

    def _unfinished_snapshot(self, events, starts, ends, window_end):
        latest_by_pair = {}
        candidates = list(starts.values()) + [item for item in events
                                               if item.get("event_type") == "assignment_stop_requested"]
        for start_event in candidates:
            assignment_id = str(start_event.get("assignment_id", ""))
            finish = ends.get(assignment_id)
            source = finish or start_event
            key = (str(start_event.get("campaign_id")), str(start_event.get("account_id")))
            try:
                stamp = datetime.fromisoformat(source["occurred_at"]).astimezone(timezone.utc)
            except (KeyError, ValueError):
                continue
            if stamp <= window_end and (key not in latest_by_pair or stamp > latest_by_pair[key][0]):
                latest_by_pair[key] = (stamp, source)
        suspended = unfinished = 0
        for _stamp, event in latest_by_pair.values():
            campaign = self.campaign_repository.get(str(event.get("campaign_id", ""))) or {}
            total = int(event.get("total_cycles", campaign.get("total_cycles", 1)))
            snapshots = event.get("tasks") or []
            assignment_id = str(event.get("assignment_id", ""))
            has_finished = any(str(done.get("assignment_id")) == assignment_id for done in ends.values())
            if not snapshots or (event.get("event_type") == "assignment_started" and not has_finished):
                account_id = str(event.get("account_id", ""))
                snapshots = [task for task in self.execution_service.task_records(str(event.get("campaign_id", "")))
                             if str(task.get("account_id")) == account_id]
            for task in snapshots:
                remaining = max(0, total - int(task.get("next_cycle", 1)) + 1)
                if remaining and (event.get("event_type") == "assignment_stop_requested"
                                  or task.get("status") in {"suspended", "in_progress"}):
                    suspended += 1
                unfinished += remaining
        return suspended, unfinished
