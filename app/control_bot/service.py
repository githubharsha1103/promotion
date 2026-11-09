"""Owner-only Telegram Bot API control and account enrollment interface."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import re
import secrets
import uuid

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

from app.campaigns import CampaignExecutionService
from app.campaigns.execution_service import PROMOTION_MESSAGES
from app.scheduler import SchedulerService
from app.config import Settings, get_settings
from app.storage.repositories.account_repository import AccountRepository
from app.storage.repositories.campaign_repository import CampaignRepository
from app.storage.repositories.message_repository import MessageRepository
from app.storage.repositories.sticker_repository import StickerRepository
from app.storage.repositories.target_repository import TargetRepository
from app.storage.repositories.restriction_repository import RestrictionRepository
from app.storage.file_manager import FileManager
from app.telegram.auth_service import AuthenticationFlowError, TelegramAuthenticationService
from app.telegram.client_manager import TelegramClientManager


logger = logging.getLogger(__name__)
_FLOW_TIMEOUT = timedelta(minutes=10)
_CONFIRM_TIMEOUT = timedelta(minutes=2)
_PAGE_SIZE = 8
_ACCOUNT_ID_PATTERN = re.compile(r"^[a-f0-9]{32}$")
_RECORD_ID_PATTERN = re.compile(r"^[a-f0-9]{32}$")
_USERNAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")
_CAMPAIGN_NAME_PATTERN = re.compile(r"^.{1,80}$", re.DOTALL)
_TEXT_LIMIT = 4096


@dataclass
class BotAuthenticationFlow:
    """Non-secret conversation state for one owner chat."""

    stage: str
    expires_at: datetime
    auth_token: str | None = None
    account_name: str | None = None


@dataclass
class RemovalConfirmation:
    account_id: str
    chat_id: int
    expires_at: datetime


@dataclass
class Workflow:
    """Temporary non-secret state for owner-only bot conversations."""

    kind: str
    step: str
    data: dict
    expires_at: datetime


@dataclass
class ActionConfirmation:
    """Short-lived confirmation tied to one private chat and record."""

    kind: str
    record_id: str
    chat_id: int
    expires_at: datetime


class ControlBotService:
    """Run a polling control bot with owner checks on every update."""

    def __init__(
        self,
        account_repository: AccountRepository,
        auth_service: TelegramAuthenticationService,
        client_manager: TelegramClientManager,
        settings: Settings | None = None,
        campaign_execution: CampaignExecutionService | None = None,
        scheduler_service: SchedulerService | None = None,
        reporting_service=None,
    ) -> None:
        self.settings = settings or get_settings()
        self.account_repository = account_repository
        self.target_repository = TargetRepository(self.settings.records_dir)
        self.restriction_repository = RestrictionRepository(self.settings.records_dir)
        self.sticker_repository = StickerRepository(self.settings.records_dir)
        self.message_repository = MessageRepository(self.settings.records_dir)
        self.campaign_repository = CampaignRepository(self.settings.records_dir)
        self.sticker_file_manager = FileManager(self.settings.stickers_dir)
        self.auth_service = auth_service
        self.client_manager = client_manager
        self.campaign_execution = campaign_execution
        self.scheduler_service = scheduler_service
        self.reporting_service = reporting_service
        self._application: Application | None = None
        self._initialized = False
        self._started = False
        self._polling = False
        self._expiry_task: asyncio.Task[None] | None = None
        self._auth_flows: dict[tuple[int, int], BotAuthenticationFlow] = {}
        self._active_flow_key: tuple[int, int] | None = None
        self._removal_confirmations: dict[str, RemovalConfirmation] = {}
        self._workflows: dict[tuple[int, int], Workflow] = {}
        self._action_confirmations: dict[str, ActionConfirmation] = {}
        self._dashboard_sections: dict[int, str] = {}
        self._flow_lock = asyncio.Lock()
        self._campaign_last_edit: dict[str, float] = {}
        self._suspended_resume_tokens: dict[str, tuple[str, str]] = {}

    async def start(self) -> bool:
        """Start polling; failures leave the FastAPI service available."""

        token = self.settings.control_bot_token.get_secret_value()
        if not token or self.settings.admin_id is None:
            logger.info("Control bot disabled: CONTROL_BOT_TOKEN or ADMIN_ID is not configured")
            return False

        try:
            application = Application.builder().token(token).build()
            application.bot_data["control_bot_service"] = self
            # This handler runs first and stops all processing for unauthorized updates.
            application.add_handler(TypeHandler(Update, self._authorize_update), group=-100)
            application.add_handler(CommandHandler("start", self._start_command))
            application.add_handler(CommandHandler("help", self._help_command))
            application.add_handler(CommandHandler("status", self._status_command))
            application.add_handler(CommandHandler("accounts", self._accounts_command))
            application.add_handler(CommandHandler("account", self._account_command))
            application.add_handler(CommandHandler("disconnect", self._disconnect_command))
            application.add_handler(CommandHandler("reconnect", self._reconnect_command))
            application.add_handler(CommandHandler("removeaccount", self._remove_account_command))
            application.add_handler(CommandHandler("addaccount", self._add_account_command))
            application.add_handler(CommandHandler("cancel", self._cancel_command))
            application.add_handler(CommandHandler("addtarget", self._add_target_command))
            application.add_handler(CommandHandler("targets", self._targets_command))
            application.add_handler(CommandHandler("target", self._target_command))
            application.add_handler(CommandHandler("removetarget", self._remove_target_command))
            application.add_handler(CommandHandler("uploadsticker", self._upload_sticker_command))
            application.add_handler(CommandHandler("stickers", self._stickers_command))
            application.add_handler(CommandHandler("removesticker", self._remove_sticker_command))
            application.add_handler(CommandHandler("createmessage", self._create_message_command))
            application.add_handler(CommandHandler("messages", self._messages_command))
            application.add_handler(CommandHandler("message", self._message_command))
            application.add_handler(CommandHandler("removemessage", self._remove_message_command))
            application.add_handler(CommandHandler("createcampaign", self._create_campaign_command))
            application.add_handler(CommandHandler("campaigns", self._campaigns_command))
            application.add_handler(CommandHandler("campaign", self._campaign_command))
            application.add_handler(CommandHandler("deletecampaign", self._delete_campaign_command))
            application.add_handler(CommandHandler("teststicker", self._test_sticker_command))
            application.add_handler(CallbackQueryHandler(self._callback_router))
            application.add_handler(
                MessageHandler(filters.TEXT & ~filters.COMMAND, self._authentication_message)
            )
            application.add_handler(MessageHandler(filters.Sticker.ALL, self._sticker_upload_message))

            self._application = application
            await application.initialize()
            self._initialized = True
            await application.start()
            self._started = True
            if application.updater is None:
                raise RuntimeError("Polling updater is unavailable")
            await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            self._polling = True
            self._expiry_task = asyncio.create_task(self._expire_authentication_flows())
            logger.info("Control bot polling started")
            return True
        except Exception as exc:
            logger.error("Control bot startup failed (error=%s)", type(exc).__name__)
            await self.stop()
            return False

    async def stop(self) -> None:
        """Stop polling and discard pending account authentication flows."""

        application = self._application
        if self._polling and application is not None and application.updater is not None:
            try:
                await application.updater.stop()
            except Exception as exc:
                logger.warning("Control bot polling shutdown failed (error=%s)", type(exc).__name__)
            self._polling = False

        if self._expiry_task is not None:
            self._expiry_task.cancel()
            try:
                await self._expiry_task
            except asyncio.CancelledError:
                pass
            self._expiry_task = None

        async with self._flow_lock:
            pending = list(self._auth_flows.values())
            self._auth_flows.clear()
            self._active_flow_key = None
            self._removal_confirmations.clear()
            self._workflows.clear()
            self._action_confirmations.clear()
            self._dashboard_sections.clear()
            for flow in pending:
                if flow.auth_token:
                    try:
                        await self.auth_service.cancel(flow.auth_token)
                    except Exception as exc:
                        logger.warning("Could not clear pending account flow during shutdown (error=%s)", type(exc).__name__)

        if application is None:
            return
        if self._started:
            try:
                await application.stop()
            except Exception as exc:
                logger.warning("Control bot application shutdown failed (error=%s)", type(exc).__name__)
            self._started = False
        if self._initialized:
            try:
                await application.shutdown()
            except Exception as exc:
                logger.warning("Control bot resource cleanup failed (error=%s)", type(exc).__name__)
            self._initialized = False
        self._application = None

    async def _authorize_update(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Authorize every update by numeric Telegram ID, never by username."""

        user = update.effective_user
        if user is not None and user.id == self.settings.admin_id:
            return
        try:
            if update.callback_query is not None:
                await update.callback_query.answer("Unauthorized.", show_alert=True)
            elif update.message is not None:
                await update.message.reply_text("Unauthorized.")
            elif update.inline_query is not None:
                await update.inline_query.answer(results=[], cache_time=0)
        except Exception as exc:
            logger.info("Could not send unauthorized response (error=%s)", type(exc).__name__)
        raise ApplicationHandlerStop

    @staticmethod
    def _flow_key(update: Update) -> tuple[int, int] | None:
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return None
        return user.id, chat.id

    @staticmethod
    def _chat_id(message_or_query) -> int:
        query_message = getattr(message_or_query, "message", None)
        if query_message is not None:
            return query_message.chat_id
        return message_or_query.chat_id

    async def _delete_sensitive_message(self, update: Update) -> None:
        message = update.effective_message
        if message is None:
            return
        try:
            await message.delete()
        except Exception as exc:
            logger.info("Could not delete an authentication input message (error=%s)", type(exc).__name__)

    async def _send_chat_message(self, message, text: str) -> None:
        """Send a fresh chat message instead of replying to a deleted input."""

        if self._application is not None:
            await self._application.bot.send_message(
                chat_id=message.chat_id,
                text=text,
                reply_markup=self._flow_navigation(message.chat_id),
            )

    async def _start_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        message = update.effective_message
        if message is not None:
            await message.reply_text(
                "PromoPilotHQ dashboard",
                reply_markup=self._dashboard_home_keyboard(),
            )

    async def notify_restriction(self, restriction: dict) -> None:
        """Send one persisted new restriction alert to the configured owner."""

        if self._application is None or self.settings.admin_id is None:
            raise RuntimeError("Control bot is unavailable for owner notifications")
        account_id = str(restriction.get("account_id", ""))
        try:
            account = self.account_repository.get(account_id) or {}
        except Exception as exc:
            logger.warning("Could not read account label for owner restriction notification (error=%s)", type(exc).__name__)
            account = {}
        account_label = str(account.get("name") or account.get("phone") or account_id)
        text = (
            "Account-target restriction detected\n"
            f"Account: {account_label}\n"
            f"Target bot: @{restriction.get('target_username', 'unknown')} "
            f"(ID: {restriction.get('target_bot_id', 'unavailable')})\n"
            f"Reason: {restriction.get('detection_reason', 'Access denied')}\n"
            f"Matched phrase: {restriction.get('matched_phrase', 'unavailable')}\n"
            f"Detected at: {restriction.get('detected_at', 'unknown')}"
        )
        await self._application.bot.send_message(chat_id=self.settings.admin_id, text=text)

    async def notify_normal_campaign_suspended(self, detail: dict) -> None:
        """Notify the owner after scheduled priority safely suspends a normal account worker."""
        if self._application is None or self.settings.admin_id is None:
            return
        account_id = str(detail.get("account_id", ""))
        campaign = detail.get("campaign") or {}
        account = self.account_repository.get(account_id) or {}
        label = str(account.get("name") or account.get("phone") or "Unknown account")
        progress = detail.get("progress") or {}
        cycle_progress = ", ".join(
            f"target {target_id}: next cycle {cycle}"
            for target_id, cycle in (progress.get("next_cycles") or {}).items()
        ) or "No cycle checkpoint was started"
        text = (
            "Normal campaign suspended for scheduled priority\n"
            f"Account: {label} (ID: {account_id})\n"
            f"Campaign: {campaign.get('name', 'Unknown campaign')} (ID: {campaign.get('id', 'unknown')})\n"
            "Reason: Scheduled campaign requires this account.\n"
            f"Saved progress: {progress.get('successful', 0)} successful, "
            f"{progress.get('failed', 0)} failed, {progress.get('restricted', 0)} restricted.\n"
            f"Checkpoint: {cycle_progress}\n"
            "This account is needed for scheduled promotion. Please use another account for your normal campaign."
        )
        await self._application.bot.send_message(chat_id=self.settings.admin_id, text=text)

    async def notify_scheduler_recovery(self, status: dict) -> bool:
        """Ask the configured owner to confirm recovery of interrupted scheduled work."""
        if self._application is None or self.settings.admin_id is None:
            return False
        recovery = status.get("recovery") or {}
        assignments = recovery.get("assignments") or []
        lines = [
            "Scheduled promotion was interrupted by an unexpected application restart.",
            f"Last scheduler status: {recovery.get('last_known_scheduler_status', 'unknown')}",
            f"Last operating period: {recovery.get('last_operating_period', 'unknown')}",
            f"Interrupted assignments: {len(assignments)}",
        ]
        for item in assignments:
            progress = item.get("progress") or {}
            saved_cycles = list((progress.get("next_cycles") or {}).items())
            cycles = ", ".join(
                f"target {target}: cycle {cycle}"
                for target, cycle in saved_cycles[:12]
            ) or "no cycle checkpoint"
            if len(saved_cycles) > 12:
                cycles += f", and {len(saved_cycles) - 12} more targets"
            lines.append(
                f"- {item.get('campaign_name', 'Unknown campaign')} (ID {item.get('campaign_id', 'unknown')})\n"
                f"  Assignment: {item.get('assignment_id') or 'unknown'}\n"
                f"  Account: {item.get('account_name', item.get('account_id'))} (ID {item.get('account_id')})\n"
                f"  Saved: {progress.get('successful', 0)} successful, {progress.get('failed', 0)} failed, "
                f"{progress.get('restricted', 0)} restricted; {cycles}"
            )
        lines.append("Choose whether to resume eligible scheduled work or keep the scheduler stopped.")
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Resume Scheduled Campaigns", callback_data="scheduler_recovery:resume"),
            InlineKeyboardButton("Keep Scheduler Stopped", callback_data="scheduler_recovery:stop"),
        ]])
        text = "\n".join(lines)
        chunks = (self.reporting_service.split_text(text) if self.reporting_service is not None
                  else [text[:3900]])
        for index, chunk in enumerate(chunks):
            await self._application.bot.send_message(
                chat_id=self.settings.admin_id, text=chunk,
                reply_markup=keyboard if index == len(chunks) - 1 else None,
            )
        return True

    async def deliver_scheduled_report(self, report: dict) -> bool:
        """Deliver a scheduled-only report to the configured owner."""
        if self._application is None or self.settings.admin_id is None:
            return False
        for chunk in self.reporting_service.split_report(report):
            await self._application.bot.send_message(chat_id=self.settings.admin_id, text=chunk)
        return True

    @staticmethod
    def _dashboard_home_keyboard() -> InlineKeyboardMarkup:
        sections = [
            ("Accounts", "accounts"), ("Targets", "targets"),
            ("Stickers", "stickers"), ("Messages", "messages"),
            ("Campaigns", "campaigns"), ("Account Restrictions", "restrictions"),
            ("Reports", "reports"), ("Settings", "settings"),
        ]
        rows = []
        for index in range(0, len(sections), 2):
            rows.append([
                InlineKeyboardButton(label, callback_data=f"dash:section:{section}")
                for label, section in sections[index:index + 2]
            ])
        rows.insert(0, [InlineKeyboardButton("\U0001F680 Promotion Dashboard", callback_data="promo:dashboard")])
        return InlineKeyboardMarkup(rows)

    @staticmethod
    def _format_duration(seconds: int | None) -> str:
        if seconds is None:
            return "—"
        seconds = max(0, int(seconds))
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"

    async def _show_promotion_dashboard(self, query, page: int = 0, *, refresh: bool = False) -> None:
        if self.scheduler_service is None or query.message is None:
            await query.edit_message_text("Promotion scheduling is unavailable.")
            return
        data = (await self.scheduler_service.refresh_promotion_dashboard_data(page)
                if refresh else await self.scheduler_service.promotion_dashboard_data(page))
        if not data.get("configured"):
            buttons = [[InlineKeyboardButton("Configure Promotion", callback_data="promo:configure")]]
            buttons.append([InlineKeyboardButton("\U0001F504 Refresh Dashboard", callback_data="promo:refresh")])
            await query.edit_message_text(
                "Promotion not configured\n\nConfigure a promotion name and an opted-in target bot to begin.",
                reply_markup=self._dashboard_navigation(InlineKeyboardMarkup(buttons), query.message.chat_id,
                                                        back_data="dash:home"),
            )
            return
        status = data["status"]
        settings = status["settings"]
        if status.get("recovery_required"):
            state = "Awaiting Recovery Confirmation"
        elif status.get("manually_paused"):
            state = "Paused"
        elif data.get("running_accounts"):
            state = "Running"
        elif status.get("operating_period") == "inactive" and status.get("enabled"):
            state = "Outside Operating Hours"
        else:
            state = "Ready"
        period = str(status.get("operating_period", "inactive")).replace("_", " ").title()
        running_ids = status.get("running_scheduled_accounts", [])
        running_lines = []
        for account_id in running_ids:
            row = next((item for item in data["accounts"] if item["account_id"] == str(account_id)), None)
            running_lines.append(row["account_name"] + f" ({account_id})" if row else str(account_id))
        account_lines = []
        for row in data["accounts"]:
            account_lines.append(
                f"{row['account_name']} (ID {row['account_id']}) — {row['status']} — @{data.get('target_username', 'unknown')}\n"
                f"  Assignment: {row.get('campaign_name') or '—'} (ID {row.get('campaign_id') or '—'})\n"
                f"  Cycles {row['completed_cycles']}; current cycle {row.get('current_cycle') or '—'}; "
                f"step {row.get('execution_step') or '—'}\n"
                f"  Current run {self._format_duration(row.get('run_seconds'))}; "
                f"window work {self._format_duration(row.get('window_seconds'))}; "
                f"operations {row.get('operations', 0)}; last activity {row.get('last_activity') or '—'}"
                + (f"; reason {row['last_error']}" if row.get("last_error") else "")
            )
        lines = [
            f"\U0001F680 Promotion Dashboard — {data['promotion'].get('name', 'Promotion')}",
            f"Target bot: @{data.get('target_username') or 'unavailable'} (ID {data['promotion'].get('target_id')})",
            f"State: {state}",
            f"Operating period: {period}",
            f"Operating hours: {settings['operating_start']}–{settings['operating_end']} IST; "
            f"peak: {settings['peak_start']}–{settings['peak_end']} IST",
            f"Eligible accounts: {data['eligible_total']} / {data['account_count']}; "
            f"maximum concurrency: {status.get('maximum_concurrency', 0)}",
            f"Running accounts: {data['running_accounts']}; idle/waiting: {data['idle_accounts']}",
            f"Completed cycles: {data['completed_cycles']}; promotion operations this window: {data['total_operations']}",
            f"Restricted accounts: {data['restricted_accounts']}; unavailable accounts: {data['unavailable_accounts']}",
        ]
        if running_lines:
            lines.append("Running: " + ", ".join(running_lines))
        lines.append(f"\nAccount activity ({data['account_page'] + 1}/{data['account_pages']}):")
        lines.extend(account_lines or ["No accounts are associated with an enabled campaign for this target."])
        if not data.get("promotion_content_available"):
            lines.append("\nUpload a sticker in the existing Sticker Library to use the scheduled promotion sequence.")
        if data.get("restricted_pairs"):
            lines.append("\nRestricted account-target pairs:")
            lines.extend(f"- Account {item['account_id']} → target {item['target_id']} ({item['status']})"
                         for item in data["restricted_pairs"][:10])
        if data.get("suspended_campaigns"):
            lines.append("\nNormal campaigns suspended by scheduled priority:")
            for campaign in data["suspended_campaigns"][:8]:
                lines.append(f"- {campaign['name']} (ID {campaign['id']}), accounts: "
                             + (", ".join(campaign["account_ids"]) or "unavailable"))
        if data.get("recent_activity"):
            lines.append("\nRecent scheduler activity:")
            for item in data["recent_activity"][:6]:
                detail = f" — {item['reason']}" if item.get("reason") else ""
                lines.append(f"- {item.get('occurred_at', 'unknown')}: {item.get('event_type')} "
                             f"account {item.get('account_id', '—')}{detail}")
        buttons = []
        if state != "Awaiting Recovery Confirmation":
            if state == "Running":
                buttons.append([InlineKeyboardButton("\u23F8 Pause Promotion", callback_data="promo:pause")])
            elif state == "Paused":
                buttons.append([InlineKeyboardButton("\u25B6 Resume Promotion", callback_data="promo:resume")])
            else:
                buttons.append([InlineKeyboardButton("\u25B6 Start Promotion", callback_data="promo:start")])
        else:
            buttons.append([InlineKeyboardButton("Recovery Status", callback_data="dash:action:settings:scheduler_recovery")])
        buttons.append([InlineKeyboardButton("Configure Promotion", callback_data="promo:configure"),
                        InlineKeyboardButton("\U0001F504 Refresh Dashboard", callback_data="promo:refresh")])
        page_count = data["account_pages"]
        if page_count > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"promo:page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{page_count}", callback_data="noop"))
            if page + 1 < page_count:
                nav.append(InlineKeyboardButton("Next", callback_data=f"promo:page:{page + 1}"))
            buttons.append(nav)
        text = "\n".join(lines)
        chunks = self.reporting_service.split_text(text) if self.reporting_service is not None else [text[:3900]]
        await query.edit_message_text(
            chunks[0], reply_markup=self._dashboard_navigation(
                InlineKeyboardMarkup(buttons), query.message.chat_id, back_data="dash:home",
            ),
        )
        for chunk in chunks[1:]:
            await self._application.bot.send_message(chat_id=self.settings.admin_id, text=chunk)

    async def _show_promotion_targets(self, message_or_query, page: int = 0, edit: bool = False) -> None:
        targets = self._targets(opted_in_only=True)
        pages = max(1, (len(targets) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = min(max(0, int(page)), pages - 1)
        rows = [[InlineKeyboardButton(
            f"@{item['username']}", callback_data=f"promo:target:{item['id']}"
        )] for item in targets[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"promo:targets:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"promo:targets:{page + 1}"))
            rows.append(nav)
        rows.append([InlineKeyboardButton("Cancel", callback_data="promo:cancel")])
        markup = self._with_flow_navigation(InlineKeyboardMarkup(rows), self._chat_id(message_or_query))
        text = "Select an existing target bot with confirmed opt-in:"
        if edit:
            await message_or_query.edit_message_text(text if targets else "No opted-in target bots are available.", reply_markup=markup)
        else:
            await message_or_query.reply_text(text if targets else "No opted-in target bots are available.", reply_markup=markup)

    @staticmethod
    def _dashboard_section_keyboard(section: str) -> InlineKeyboardMarkup:
        actions = {
            "accounts": [("View accounts", "list"), ("Add account", "add"),
                         ("Connection status", "status"), ("Disconnect account", "disconnect"),
                         ("Reconnect account", "reconnect"), ("Remove account", "remove")],
            "targets": [("List targets", "list"), ("Add target", "add"), ("Remove target", "remove")],
            "stickers": [("List stickers", "list"), ("Upload sticker", "upload"), ("Remove sticker", "remove")],
            "messages": [("List messages", "list"), ("Create message", "create"), ("Remove message", "remove")],
            "campaigns": [("List campaigns", "list"), ("Create campaign", "create"),
                          ("Run campaign", "run"), ("Pause / resume", "pause"),
                          ("Stop campaign", "stop"), ("Suspended campaigns", "suspended"),
                          ("Delete campaign", "delete")],
            "restrictions": [("View restrictions", "list")],
            "reports": [("Latest 9 PM progress report", "progress"),
                        ("Latest 3:15 AM final report", "final"),
                        ("Current scheduled statistics", "current")],
            "settings": [("Scheduler status", "scheduler_status"), ("Enable / disable scheduler", "scheduler_toggle"),
                         ("Recovery status", "scheduler_recovery")],
        }
        rows = []
        section_actions = actions.get(section, [])
        for index in range(0, len(section_actions), 2):
            rows.append([
                InlineKeyboardButton(label, callback_data=f"dash:action:{section}:{action}")
                for label, action in section_actions[index:index + 2]
            ])
        rows.append([
            InlineKeyboardButton("Back", callback_data="dash:home"),
            InlineKeyboardButton("Home", callback_data="dash:home"),
        ])
        return InlineKeyboardMarkup(rows)

    def _dashboard_navigation(
        self,
        markup: InlineKeyboardMarkup | None,
        chat_id: int,
        back_data: str | None = None,
    ) -> InlineKeyboardMarkup:
        rows = [list(row) for row in markup.inline_keyboard] if markup is not None else []
        section = self._dashboard_sections.get(chat_id)
        back_data = back_data or (f"dash:section:{section}" if section else "dash:home")
        rows.append([
            InlineKeyboardButton("Back", callback_data=back_data),
            InlineKeyboardButton("Home", callback_data="dash:home"),
        ])
        return InlineKeyboardMarkup(rows)

    def _flow_navigation(self, chat_id: int) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("Back", callback_data="dash:cancel:back"),
            InlineKeyboardButton("Home", callback_data="dash:cancel:home"),
        ]])

    def _with_flow_navigation(self, markup: InlineKeyboardMarkup | None, chat_id: int) -> InlineKeyboardMarkup:
        rows = [list(row) for row in markup.inline_keyboard] if markup is not None else []
        rows.extend(self._flow_navigation(chat_id).inline_keyboard)
        return InlineKeyboardMarkup(rows)

    async def _edit_or_reply(self, update: Update, message, text: str, reply_markup=None) -> None:
        query = update.callback_query
        if query is not None:
            await query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await message.reply_text(text, reply_markup=reply_markup)

    async def _dashboard_cancel_flow(self, query, destination: str) -> None:
        if query.message is None:
            return
        chat_id = query.message.chat_id
        key = (self.settings.admin_id, chat_id)
        async with self._flow_lock:
            auth_flow = self._auth_flows.pop(key, None)
            workflow = self._workflows.pop(key, None)
            if auth_flow is not None and auth_flow.auth_token:
                try:
                    await self.auth_service.cancel(auth_flow.auth_token)
                except Exception as exc:
                    logger.warning("Could not cancel account authentication from dashboard (error=%s)", type(exc).__name__)
            if self._active_flow_key == key:
                self._active_flow_key = None
            for nonce, confirmation in list(self._removal_confirmations.items()):
                if confirmation.chat_id == chat_id:
                    self._removal_confirmations.pop(nonce, None)
            for nonce, confirmation in list(self._action_confirmations.items()):
                if confirmation.chat_id == chat_id:
                    self._action_confirmations.pop(nonce, None)
        if destination == "back" and chat_id in self._dashboard_sections:
            self._discard_pending_sticker_upload(workflow)
            await self._dashboard_show_section(query, self._dashboard_sections[chat_id])
        else:
            self._discard_pending_sticker_upload(workflow)
            await self._dashboard_show_home(query)

    async def _dashboard_show_home(self, query) -> None:
        if query.message is None:
            return
        self._dashboard_sections.pop(query.message.chat_id, None)
        await query.edit_message_text("PromoPilotHQ dashboard", reply_markup=self._dashboard_home_keyboard())

    async def _dashboard_show_section(self, query, section: str) -> None:
        if section not in {"accounts", "targets", "stickers", "messages", "campaigns", "restrictions", "reports", "settings"}:
            await query.edit_message_text("This dashboard section is unavailable. Return Home and try again.")
            return
        if query.message is not None:
            self._dashboard_sections[query.message.chat_id] = section
        titles = {
            "accounts": "Telegram accounts", "targets": "Promotion targets",
            "stickers": "Sticker library", "messages": "Promotion messages",
            "campaigns": "Campaigns", "reports": "Reports",
            "restrictions": "Account Restrictions",
            "settings": "Settings",
        }
        descriptions = {
            "reports": "Reports are reserved for future campaign execution and progress reporting.",
            "settings": "Application settings controls are not available yet.",
        }
        text = titles[section]
        if section in descriptions:
            text += "\n\n" + descriptions[section]
        await query.edit_message_text(text, reply_markup=self._dashboard_section_keyboard(section))

    async def _show_restrictions(
        self,
        query,
        *,
        account_id: str | None = None,
        target_bot_id: str | None = None,
        page: int = 0,
    ) -> None:
        chat_id = query.message.chat_id if query.message is not None else 0
        self._dashboard_sections[chat_id] = "restrictions"
        try:
            records = self.restriction_repository.list()
        except Exception as exc:
            logger.warning("Could not read account restrictions (error=%s)", type(exc).__name__)
            await query.edit_message_text(
                "Could not load account restrictions.",
                reply_markup=self._dashboard_navigation(None, chat_id, back_data="dash:section:restrictions"),
            )
            return
        all_records = records

        if account_id is not None:
            records = [item for item in records if str(item.get("account_id")) == account_id]
        if target_bot_id is not None:
            records = [item for item in records if str(item.get("target_bot_id")) == target_bot_id]
        records.sort(key=lambda item: str(item.get("detected_at", "")), reverse=True)
        rows: list[list[InlineKeyboardButton]] = []

        if account_id is None:
            accounts = {}
            for item in all_records:
                account_key = str(item.get("account_id", ""))
                accounts[account_key] = self._restriction_account_label(account_key)
            for key, label in sorted(accounts.items(), key=lambda pair: pair[1].casefold()):
                rows.append([InlineKeyboardButton(f"Account: {label}"[:64], callback_data=f"r:acct:{key}:0")])
        elif target_bot_id is None:
            bot_ids: dict[str, str] = {}
            for item in records:
                bot_id = str(item.get("target_bot_id", ""))
                if bot_id:
                    bot_ids[bot_id] = str(item.get("target_username", "unknown"))
            for bot_id, username in sorted(bot_ids.items(), key=lambda pair: pair[1].casefold()):
                rows.append([InlineKeyboardButton(f"Target: @{username}"[:64], callback_data=f"r:target:{account_id}:{bot_id}:0")])

        page_size = 6
        page = max(0, min(page, max(0, (len(records) - 1) // page_size)))
        page_records = records[page * page_size:(page + 1) * page_size]
        for item in page_records:
            account_label = self._restriction_account_label(str(item.get("account_id", "")))
            label = f"{account_label} / @{item.get('target_username', 'unknown')} ({item.get('status', 'active')})"
            rows.append([InlineKeyboardButton(label[:64], callback_data=f"r:view:{item['id']}")])

        if not records:
            text = "No account restrictions are recorded for this filter."
        else:
            scope = "all accounts" if account_id is None else f"account {account_id[:8]}"
            if target_bot_id is not None:
                scope += f", bot ID {target_bot_id}"
            text = f"Account Restrictions ({scope})\nSelect a restriction to view its details."
        if len(records) > page_size:
            previous = max(0, page - 1)
            following = min((len(records) - 1) // page_size, page + 1)
            if account_id is None:
                prev_data, next_data = f"r:list:{previous}", f"r:list:{following}"
            elif target_bot_id is None:
                prev_data, next_data = f"r:acct:{account_id}:{previous}", f"r:acct:{account_id}:{following}"
            else:
                prev_data = f"r:target:{account_id}:{target_bot_id}:{previous}"
                next_data = f"r:target:{account_id}:{target_bot_id}:{following}"
            rows.append([
                InlineKeyboardButton("Previous", callback_data=prev_data),
                InlineKeyboardButton(f"{page + 1}/{(len(records) + page_size - 1) // page_size}", callback_data="noop"),
                InlineKeyboardButton("Next", callback_data=next_data),
            ])
        back = "dash:section:restrictions"
        rows.append([
            InlineKeyboardButton("Refresh", callback_data=(
                f"r:list:{page}" if account_id is None else
                f"r:acct:{account_id}:{page}" if target_bot_id is None else
                f"r:target:{account_id}:{target_bot_id}:{page}"
            )),
            InlineKeyboardButton("Back", callback_data=back),
            InlineKeyboardButton("Home", callback_data="dash:home"),
        ])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))

    def _restriction_account_label(self, account_id: str) -> str:
        try:
            account = self.account_repository.get(account_id) or {}
        except Exception as exc:
            logger.warning("Could not read account label for restriction dashboard (error=%s)", type(exc).__name__)
            account = {}
        return str(account.get("name") or account.get("phone") or account_id[:8] or "Unavailable")

    async def _show_restriction(self, query, record_id: str) -> None:
        chat_id = query.message.chat_id if query.message is not None else 0
        try:
            record = self.restriction_repository.get(record_id)
        except Exception as exc:
            logger.warning("Could not read account restriction details (error=%s)", type(exc).__name__)
            record = None
        if record is None:
            await query.edit_message_text("This restriction is unavailable or has been removed.")
            return
        account_label = self._restriction_account_label(str(record.get("account_id", "")))
        lines = [
            "Account restriction details",
            f"Status: {record.get('status', 'active')}",
            f"Account: {account_label}",
            f"Target bot: @{record.get('target_username', 'unknown')}",
            f"Target Telegram user ID: {record.get('target_bot_id', 'unavailable')}",
            f"Reason: {record.get('detection_reason', 'unavailable')}",
            f"Matched phrase: {record.get('matched_phrase', 'unavailable')}",
            f"Detected at: {record.get('detected_at', 'unknown')}",
            f"Campaign ID: {record.get('campaign_id') or 'unavailable'}",
            f"Message excerpt: {record.get('message_excerpt', '')[:500] or 'unavailable'}",
        ]
        rows = []
        if record.get("status", "active") == "active":
            rows.append([InlineKeyboardButton("Clear restriction…", callback_data=f"r:clear:{record_id}")])
        rows.append([
            InlineKeyboardButton("Refresh", callback_data=f"r:view:{record_id}"),
            InlineKeyboardButton("Restrictions", callback_data="r:list:0"),
            InlineKeyboardButton("Home", callback_data="dash:home"),
        ])
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))

    async def _ask_clear_restriction(self, query, record_id: str) -> None:
        chat_id = query.message.chat_id if query.message is not None else 0
        try:
            record = self.restriction_repository.get(record_id)
        except Exception as exc:
            logger.warning("Could not read restriction before clear confirmation (error=%s)", type(exc).__name__)
            await query.edit_message_text("Could not load this restriction. Refresh the list and try again.")
            return
        if record is None or record.get("status", "active") != "active":
            await query.edit_message_text("This restriction is no longer active.", reply_markup=self._dashboard_navigation(None, chat_id, back_data="r:list:0"))
            return
        nonce = secrets.token_urlsafe(6)
        self._action_confirmations[nonce] = ActionConfirmation(
            kind="restriction_clear", record_id=record_id, chat_id=chat_id,
            expires_at=datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
        )
        username = str(record.get("target_username", "unknown"))
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Confirm clear", callback_data=f"action_yes:{nonce}"),
            InlineKeyboardButton("Cancel", callback_data=f"action_no:{nonce}"),
        ]])
        await query.edit_message_text(
            f"Clear the restriction for @{username}? The audit record will be retained.",
            reply_markup=self._with_flow_navigation(keyboard, chat_id),
        )

    async def _dashboard_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE, section: str, action: str) -> None:
        query = update.callback_query
        if query is None or query.message is None:
            return
        self._dashboard_sections[query.message.chat_id] = section
        message = query.message
        if section == "restrictions" and action == "list":
            await self._show_restrictions(query)
            return
        if section == "reports" and action in {"progress", "final", "current"}:
            if self.reporting_service is None:
                await query.edit_message_text("Scheduled reporting service is unavailable.")
                return
            if action == "current":
                report = self.reporting_service.build_current_report(
                    scheduler_enabled=bool(self.scheduler_service and self.scheduler_service.enabled),
                )
            else:
                report = self.reporting_service.latest(action)
            if report is None:
                await query.edit_message_text(
                    "No report is available yet.",
                    reply_markup=self._dashboard_navigation(None, message.chat_id,
                                                             back_data="dash:section:reports"),
                )
                return
            text = self.reporting_service.format_report(report)
            if report.get("delivery_status") == "failed" or report.get("delivery_error"):
                text += "\nAutomatic Telegram delivery failed; this saved report remains available here."
            chunks = self.reporting_service.split_text(text)
            await query.edit_message_text(
                chunks[0],
                reply_markup=self._dashboard_navigation(None, message.chat_id,
                                                         back_data="dash:section:reports"),
            )
            for chunk in chunks[1:]:
                await self._application.bot.send_message(chat_id=self.settings.admin_id, text=chunk)
            return
        if section == "settings" and action in {"scheduler_status", "scheduler_toggle", "scheduler_recovery"}:
            if self.scheduler_service is None:
                await query.edit_message_text("Scheduler service is unavailable.")
                return
            if action == "scheduler_toggle":
                self.scheduler_service.set_enabled(not self.scheduler_service.enabled)
            status = self.scheduler_service.status()
            settings = status["settings"]
            running_accounts = status.get("running_scheduled_accounts", [])
            idle_accounts = status.get("idle_eligible_accounts", [])
            scheduled_campaigns = status.get("active_scheduled_campaigns", [])
            restricted_pairs = status.get("restricted_or_excluded_pairs", [])
            running_text = ", ".join(running_accounts) or "none"
            idle_text = ", ".join(idle_accounts) or "none"
            campaign_text = ", ".join(scheduled_campaigns) or "none"
            restricted_text = ", ".join(
                f"{item.get('account_id')}→{item.get('target_id')} ({item.get('status')})"
                for item in restricted_pairs[:20]
            ) or "none"
            text = (
                f"Scheduler: {status['state']}\nEnabled: {'Yes' if status['enabled'] else 'No'}\n"
                f"Current period: {status.get('operating_period', 'inactive')}\n"
                f"Eligible accounts: {status.get('eligible_account_count', 0)}\n"
                f"Maximum concurrency: {status.get('maximum_concurrency', 0)}\n"
                f"Running scheduled accounts: {running_text}\nIdle eligible accounts: {idle_text}\n"
                f"Active scheduled campaigns: {campaign_text}\n"
                f"Restricted/excluded pairs ({len(restricted_pairs)}): {restricted_text}\n"
                f"Next window transition: {status.get('next_transition', 'unavailable')}\n"
                f"Timezone: {settings['timezone']}\n"
                f"Operating window: {settings['operating_start']}–{settings['operating_end']}\n"
                f"Peak window: {settings['peak_start']}–{settings['peak_end']}\n"
                f"Concurrency: outside peak {settings['outside_peak_concurrency_1_20']}/"
                f"{settings['outside_peak_concurrency_21_50']}/{settings['outside_peak_concurrency_51_plus']} "
                f"by eligible account tier; peak max {settings['peak_concurrency']}"
            )
            recovery = status.get("recovery") or {}
            assignments = recovery.get("assignments") or []
            recovery_lines = [
                "Recovery status: " + status.get("recovery_confirmation_status", "not_required"),
                f"Recovery required: {'Yes' if status.get('recovery_required') else 'No'}",
                f"Last known operating period: {recovery.get('last_operating_period', 'unknown')}",
                f"Last known scheduler status: {recovery.get('last_known_scheduler_status', 'unknown')}",
                f"Interrupted assignments: {len(assignments)}",
            ]
            for item in assignments:
                progress = item.get("progress") or {}
                saved_cycles = list((progress.get("next_cycles") or {}).items())
                cycles = ", ".join(
                    f"{target}:{cycle}" for target, cycle in saved_cycles[:12]
                ) or "no cycle checkpoint"
                if len(saved_cycles) > 12:
                    cycles += f", and {len(saved_cycles) - 12} more targets"
                recovery_lines.append(
                    f"- {item.get('campaign_name', item.get('campaign_id'))} (ID {item.get('campaign_id')}) / "
                    f"{item.get('account_name', item.get('account_id'))} (ID {item.get('account_id')}): "
                    f"{progress.get('successful', 0)} successful, {progress.get('failed', 0)} failed; {cycles}"
                )
            text += "\n" + "\n".join(recovery_lines)
            buttons = [[InlineKeyboardButton(
                "Disable scheduler" if status["enabled"] else "Enable scheduler",
                callback_data="scheduler:toggle",
            )]]
            if status.get("recovery_required"):
                buttons.append([
                    InlineKeyboardButton("Resume Scheduled Campaigns", callback_data="scheduler_recovery:resume"),
                    InlineKeyboardButton("Keep Scheduler Stopped", callback_data="scheduler_recovery:stop"),
                ])
            chunks = (self.reporting_service.split_text(text) if self.reporting_service is not None
                      else [text[:3900]])
            await query.edit_message_text(chunks[0], reply_markup=self._dashboard_navigation(
                InlineKeyboardMarkup(buttons), message.chat_id, back_data="dash:section:settings"))
            for chunk in chunks[1:]:
                await self._application.bot.send_message(chat_id=self.settings.admin_id, text=chunk)
            return
        if action == "placeholder":
            await query.edit_message_text(
                "This section is reserved for a later phase.",
                reply_markup=self._dashboard_navigation(None, message.chat_id),
            )
            return
        if section == "accounts":
            if action == "list":
                await self._edit_account_picker(query, "account", 0)
            elif action in {"disconnect", "reconnect", "remove"}:
                await self._edit_account_picker(query, action, 0)
            elif action == "status":
                text, keyboard = await self._status_page(self._authenticated_accounts(), 0)
                await query.edit_message_text(text, reply_markup=self._dashboard_navigation(keyboard, message.chat_id))
            elif action == "add":
                await self._add_account_command(update, context)
            return
        if section == "campaigns" and action in {"run", "pause", "stop"}:
            await self._campaigns_page(query, 0, edit=True, purpose=action)
            return
        if section == "campaigns" and action == "suspended":
            await self._suspended_campaigns_page(query)
            return
        handlers = {
            ("targets", "list"): self._targets_command,
            ("targets", "remove"): self._remove_target_command,
            ("targets", "add"): self._add_target_command,
            ("stickers", "list"): self._stickers_command,
            ("stickers", "remove"): self._remove_sticker_command,
            ("stickers", "upload"): self._upload_sticker_command,
            ("messages", "list"): self._messages_command,
            ("messages", "remove"): self._remove_message_command,
            ("messages", "create"): self._create_message_command,
            ("campaigns", "list"): self._campaigns_command,
            ("campaigns", "delete"): self._delete_campaign_command,
            ("campaigns", "create"): self._create_campaign_command,
        }
        handler = handlers.get((section, action))
        if handler is None:
            await self._dashboard_show_section(query, section)
            return
        if section == "targets" and action in {"list", "remove"}:
            await self._targets_page(query, 0, edit=True)
        elif section == "stickers" and action in {"list", "remove"}:
            await self._stickers_page(query, 0, edit=True, purpose="view" if action == "list" else "remove")
        elif section == "messages" and action in {"list", "remove"}:
            await self._messages_page(query, 0, edit=True)
        elif section == "campaigns" and action in {"list", "delete"}:
            await self._campaigns_page(query, 0, edit=True)
        else:
            # Existing command handlers start the same guided flow used by slash commands.
            await handler(update, context)

    async def _help_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        if update.effective_message is not None:
            await update.effective_message.reply_text(
                "/start - open the interactive control dashboard\n"
                "/help - show available commands\n"
                "/status - show application and account connection statuses\n"
                "/accounts - list authenticated accounts\n"
                "/account - view an account\n"
                "/disconnect - disconnect an account and keep its session\n"
                "/reconnect - reconnect an account from its saved session\n"
                "/removeaccount - remove an account after confirmation\n"
                "/addaccount - authenticate and add a Telegram account\n"
                "/cancel - cancel the active workflow\n"
                "/addtarget, /targets, /target, /removetarget - manage opted-in targets\n"
                "/uploadsticker, /stickers, /removesticker - manage sticker files\n"
                "/createmessage, /messages, /message, /removemessage - manage sticker-then-text content\n"
                "/createcampaign, /campaigns, /campaign, /deletecampaign - manage campaign drafts\n"
                "/teststicker @botusername - send one selected sticker to an opted-in bot target"
            )

    async def _status_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        message = update.effective_message
        if message is None:
            return
        try:
            accounts = self._authenticated_accounts()
        except Exception as exc:
            logger.warning("Could not read account count for control bot (error=%s)", type(exc).__name__)
            await message.reply_text("Application status: running\nSaved Telegram accounts: unavailable")
            return
        text, keyboard = await self._status_page(accounts, 0)
        await message.reply_text(text, reply_markup=self._dashboard_navigation(keyboard, message.chat_id))

    @staticmethod
    def _valid_account_id(account_id: str) -> bool:
        return bool(_ACCOUNT_ID_PATTERN.fullmatch(account_id))

    def _get_account(self, account_id: str) -> dict | None:
        if not self._valid_account_id(account_id):
            return None
        record = self.account_repository.get(account_id)
        if record is None or record.get("authenticated") is not True:
            return None
        return record

    def _authenticated_accounts(self) -> list[dict]:
        return [
            record
            for record in self.account_repository.list()
            if self._valid_account_id(str(record.get("id", "")))
            and record.get("authenticated") is True
        ]

    async def _status_page(
        self,
        accounts: list[dict],
        page: int,
    ) -> tuple[str, InlineKeyboardMarkup | None]:
        total_pages = max(1, (len(accounts) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        start = page * _PAGE_SIZE
        lines = ["Application status: running", f"Authenticated Telegram accounts: {len(accounts)}"]
        for account in accounts[start : start + _PAGE_SIZE]:
            account_id = str(account["id"])
            state = "connected" if await self.client_manager.is_connected(account_id) else "disconnected"
            lines.append(f"{account.get('name') or account.get('phone', 'Phone unavailable')} - {state}")
        if not accounts:
            lines.append("No authenticated accounts.")
        keyboard = self._page_keyboard("status", page, total_pages)
        return "\n".join(lines), keyboard

    @staticmethod
    def _page_keyboard(action: str, page: int, total_pages: int) -> InlineKeyboardMarkup | None:
        if total_pages <= 1:
            return None
        buttons = []
        if page > 0:
            buttons.append(InlineKeyboardButton("Previous", callback_data=f"pick:{action}:{page - 1}"))
        buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page + 1 < total_pages:
            buttons.append(InlineKeyboardButton("Next", callback_data=f"pick:{action}:{page + 1}"))
        return InlineKeyboardMarkup([buttons])

    async def _account_picker(
        self,
        message,
        action: str,
        page: int = 0,
    ) -> None:
        accounts = self._authenticated_accounts()
        if not accounts:
            await message.reply_text("No authenticated Telegram accounts are saved.", reply_markup=self._dashboard_navigation(None, message.chat_id))
            return
        total_pages = (len(accounts) + _PAGE_SIZE - 1) // _PAGE_SIZE
        page = max(0, min(page, total_pages - 1))
        rows = []
        for account in accounts[page * _PAGE_SIZE : (page + 1) * _PAGE_SIZE]:
            account_id = str(account["id"])
            connected = await self.client_manager.is_connected(account_id)
            phone = str(account.get("name") or account.get("phone", "Phone unavailable"))
            label = f"{phone} - {'Connected' if connected else 'Disconnected'}"
            callback_data = (
                f"account:{account_id}:{page}" if action == "account" else f"{action}:{account_id}"
            )
            rows.append([InlineKeyboardButton(label[:60], callback_data=callback_data)])
        pager = self._page_keyboard(action, page, total_pages)
        if pager is not None:
            rows.extend(pager.inline_keyboard)
        markup = self._dashboard_navigation(InlineKeyboardMarkup(rows), message.chat_id)
        await self._edit_or_reply(update, message, f"Select an account ({page + 1}/{total_pages}):", markup)

    async def _account_details(
        self,
        message_or_query,
        account_id: str,
        page: int = 0,
        *,
        edit: bool = False,
    ) -> None:
        account = self._get_account(account_id)
        if account is None:
            text = "This account is no longer available. Open /accounts to refresh the list."
            if edit:
                await message_or_query.edit_message_text(text)
            else:
                await message_or_query.reply_text(text)
            return
        connected = await self.client_manager.is_connected(account_id)
        session_exists = self.client_manager.session_file_exists(account_id)
        username = account.get("username")
        lines = [
            f"Name: {account.get('name', 'Unnamed')}",
            f"Phone: {account.get('phone', 'Unavailable')}",
            f"Status: {'Connected' if connected else 'Disconnected'}",
            f"Session file: {'Present' if session_exists else 'Missing'}",
            f"Authenticated: {'Yes' if account.get('authenticated') is True else 'No'}",
        ]
        if username:
            lines.append(f"Username: @{username}")
        if account.get("telegram_user_id") is not None:
            lines.append(f"Telegram user ID: {account['telegram_user_id']}")
        buttons = [
            [InlineKeyboardButton("Disconnect", callback_data=f"disconnect:{account_id}")],
            [InlineKeyboardButton("Reconnect", callback_data=f"reconnect:{account_id}")],
            [InlineKeyboardButton("Remove account", callback_data=f"remove:{account_id}")],
            [InlineKeyboardButton("Back to accounts", callback_data=f"pick:account:{page}")],
            [InlineKeyboardButton("Home", callback_data="dash:home")],
        ]
        markup = InlineKeyboardMarkup(buttons)
        if edit:
            await message_or_query.edit_message_text("\n".join(lines), reply_markup=markup)
        else:
            await message_or_query.reply_text("\n".join(lines), reply_markup=markup)

    async def _accounts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._account_picker(update.effective_message, "account")

    async def _account_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        if context.args:
            account_id = context.args[0]
            if not self._valid_account_id(account_id):
                await message.reply_text("Invalid account ID.")
                return
            await self._account_details(message, account_id)
            return
        await self._account_picker(message, "account")

    async def _disconnect_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._account_picker(update.effective_message, "disconnect")

    async def _reconnect_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._account_picker(update.effective_message, "reconnect")

    async def _remove_account_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._account_picker(update.effective_message, "remove")

    async def _callback_router(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        data = query.data or ""
        parts = data.split(":")
        if data == "noop":
            return
        if parts[0] == "scheduler_recovery":
            if (self.scheduler_service is None or query.message is None
                    or query.message.chat.type != "private"
                    or update.effective_user is None
                    or update.effective_user.id != self.settings.admin_id):
                await query.edit_message_text("Recovery controls are available only to the configured owner in a private chat.")
                return
            if len(parts) != 2 or parts[1] not in {"resume", "stop"}:
                await query.edit_message_text("This recovery action is stale. Open Scheduler Status to refresh.")
                return
            result = await self.scheduler_service.confirm_recovery(parts[1] == "resume")
            if not result.get("accepted"):
                await query.edit_message_text(
                    "Recovery confirmation is no longer pending or revalidation failed. "
                    "The scheduler remains stopped; review Scheduler Status.",
                )
                return
            if parts[1] == "stop":
                text = "Scheduler kept stopped. Interrupted checkpoints are preserved; enable the scheduler manually when ready."
            elif result.get("inside_operating_window"):
                text = (
                    "Recovery confirmed. Eligible scheduled work is resuming for the remaining operating window.\n"
                    f"Eligible accounts: {result.get('eligible_account_count', 0)}; "
                    f"campaigns: {result.get('eligible_campaign_count', 0)}."
                )
            else:
                text = (
                    "Recovery confirmed outside operating hours. The scheduler remains enabled and will wait "
                    "until the next 3:00 PM IST window.\n"
                    f"Eligible accounts: {result.get('eligible_account_count', 0)}; "
                    f"campaigns: {result.get('eligible_campaign_count', 0)}."
                )
            await query.edit_message_text(
                text, reply_markup=self._dashboard_navigation(None, query.message.chat_id,
                                                               back_data="dash:section:settings"),
            )
            return
        if parts[0] == "promo":
            if self.scheduler_service is None or query.message is None:
                await query.edit_message_text("Promotion scheduling is unavailable.")
                return
            if len(parts) == 2 and parts[1] in {"dashboard", "refresh"}:
                await self._show_promotion_dashboard(query, 0, refresh=parts[1] == "refresh")
                return
            if len(parts) == 3 and parts[1] == "page" and parts[2].isdigit():
                await self._show_promotion_dashboard(query, int(parts[2]))
                return
            if parts == ["promo", "configure"]:
                if query.message.chat.type != "private":
                    await query.edit_message_text("Configure the promotion in a private chat with this bot.")
                    return
                key = (self.settings.admin_id, query.message.chat_id)
                if not self._begin_workflow(key, "promotion_config", "name", {}):
                    await query.edit_message_text("Finish or cancel the active dashboard workflow first.")
                    return
                await query.edit_message_text(
                    "Send a name for this promotion (1-80 characters).",
                    reply_markup=self._flow_navigation(query.message.chat_id),
                )
                return
            if parts == ["promo", "cancel"]:
                key = (self.settings.admin_id, query.message.chat_id)
                self._workflows.pop(key, None)
                await self._show_promotion_dashboard(query)
                return
            if len(parts) == 3 and parts[1] == "targets" and parts[2].isdigit():
                await self._show_promotion_targets(query, int(parts[2]), edit=True)
                return
            if len(parts) == 3 and parts[1] == "target" and _RECORD_ID_PATTERN.fullmatch(parts[2]):
                if query.message.chat.type != "private":
                    await query.edit_message_text("Configure the promotion in a private chat with this bot.")
                    return
                key = (self.settings.admin_id, query.message.chat_id)
                workflow = self._workflows.get(key)
                target = self._target_by_id(parts[2])
                if (workflow is None or workflow.kind != "promotion_config" or workflow.step != "target"
                        or target is None or target.get("opted_in") is not True):
                    await query.edit_message_text("This target selection is stale. Open Configure Promotion again.")
                    return
                try:
                    await self.scheduler_service.save_promotion_config(
                        str(workflow.data.get("name", "")), str(target["id"]),
                    )
                except ValueError as exc:
                    await query.edit_message_text(str(exc), reply_markup=self._dashboard_navigation(
                        None, query.message.chat_id, back_data="promo:dashboard"))
                    return
                self._workflows.pop(key, None)
                await self._show_promotion_dashboard(query)
                return
            if parts in (["promo", "start"], ["promo", "pause"], ["promo", "resume"]):
                try:
                    if parts[1] == "start":
                        await self.scheduler_service.start_promotion()
                    elif parts[1] == "pause":
                        await self.scheduler_service.pause_promotion()
                    else:
                        await self.scheduler_service.resume_promotion()
                except ValueError as exc:
                    await query.edit_message_text(
                        str(exc), reply_markup=self._dashboard_navigation(InlineKeyboardMarkup([[
                            InlineKeyboardButton("Sticker Library", callback_data="dash:section:stickers"),
                            InlineKeyboardButton("Refresh Dashboard", callback_data="promo:refresh"),
                        ]]), query.message.chat_id, back_data="promo:dashboard"),
                    )
                    return
                await self._show_promotion_dashboard(query)
                return
            await query.edit_message_text("This Promotion Dashboard action is stale. Refresh the dashboard.")
            return
        if parts[0] == "dash":
            if data == "dash:home":
                await self._dashboard_show_home(query)
            elif len(parts) == 3 and parts[1] == "cancel" and parts[2] in {"back", "home"}:
                await self._dashboard_cancel_flow(query, parts[2])
            elif len(parts) == 3 and parts[1] == "section":
                await self._dashboard_show_section(query, parts[2])
            elif len(parts) == 4 and parts[1] == "action":
                await self._dashboard_action(update, context, parts[2], parts[3])
            else:
                await query.edit_message_text(
                    "This dashboard button is stale. Open /start to refresh.",
                    reply_markup=self._dashboard_navigation(None, query.message.chat_id) if query.message else None,
                )
            return
        if parts[0] == "r":
            if query.message is None or query.message.chat.type != "private":
                await query.edit_message_text("Account restriction controls are available in a private chat with this bot.")
                return
            if len(parts) == 3 and parts[1] == "list" and parts[2].isdigit():
                await self._show_restrictions(query, page=int(parts[2]))
            elif len(parts) == 4 and parts[1] == "acct" and _ACCOUNT_ID_PATTERN.fullmatch(parts[2]) and parts[3].isdigit():
                await self._show_restrictions(query, account_id=parts[2], page=int(parts[3]))
            elif (len(parts) == 5 and parts[1] == "target"
                  and _ACCOUNT_ID_PATTERN.fullmatch(parts[2]) and parts[3].isdigit() and parts[4].isdigit()):
                await self._show_restrictions(
                    query, account_id=parts[2], target_bot_id=parts[3], page=int(parts[4])
                )
            elif len(parts) == 3 and parts[1] == "view" and re.fullmatch(r"[a-f0-9]{32}", parts[2]):
                await self._show_restriction(query, parts[2])
            elif len(parts) == 3 and parts[1] == "clear" and re.fullmatch(r"[a-f0-9]{32}", parts[2]):
                await self._ask_clear_restriction(query, parts[2])
            else:
                await query.edit_message_text("This restriction dashboard action is stale. Refresh the list.")
            return
        if len(parts) == 3 and parts[0] == "campaign_control":
            action, campaign_id = parts[1], parts[2]
            campaign = self._get_campaign(campaign_id)
            if campaign is None or self.campaign_execution is None or query.message is None:
                await query.edit_message_text("Campaign is stale or execution is unavailable.", reply_markup=self._dashboard_navigation(None, query.message.chat_id if query.message else 0))
                return
            if action == "start":
                self.campaign_repository.update(campaign_id, {
                    "progress_chat_id": query.message.chat_id,
                    "progress_message_id": query.message.message_id,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                started = await self.campaign_execution.start(campaign_id, self._campaign_progress_callback)
                if not started:
                    await query.edit_message_text("Campaign could not start. Check its settings, records and current status.", reply_markup=self._dashboard_navigation(None, query.message.chat_id, back_data=f"campaign_view:{campaign_id}"))
                else:
                    await self._campaign_progress_callback(self.campaign_repository.get(campaign_id))
                return
            if action == "pause":
                if campaign.get("status") == "running":
                    changed = await self.campaign_execution.pause(campaign_id)
                else:
                    self.campaign_repository.update(campaign_id, {
                        "progress_chat_id": query.message.chat_id,
                        "progress_message_id": query.message.message_id,
                    })
                    changed = await self.campaign_execution.resume(campaign_id, self._campaign_progress_callback)
                if not changed:
                    await query.edit_message_text("Campaign cannot be paused or resumed in its current state.", reply_markup=self._dashboard_navigation(None, query.message.chat_id, back_data=f"campaign_view:{campaign_id}"))
                else:
                    await self._campaign_progress_callback(self.campaign_repository.get(campaign_id))
                return
            if action == "stop":
                changed = await self.campaign_execution.stop(campaign_id)
                if not changed:
                    await query.edit_message_text("Campaign is not currently running or paused.", reply_markup=self._dashboard_navigation(None, query.message.chat_id, back_data=f"campaign_view:{campaign_id}"))
                else:
                    await self._campaign_progress_callback(self.campaign_repository.get(campaign_id))
                return
            await query.edit_message_text("This campaign action is stale.")
            return
        if parts == ["scheduler", "toggle"]:
            if self.scheduler_service is None or query.message is None:
                await query.edit_message_text("Scheduler service is unavailable.")
            else:
                self.scheduler_service.set_enabled(not self.scheduler_service.enabled)
                await self._dashboard_action(update, context, "settings", "scheduler_status")
            return
        if len(parts) == 2 and parts[0] == "campaign_schedule":
            campaign = self._get_campaign(parts[1])
            if campaign is None or self.scheduler_service is None:
                await query.edit_message_text("Campaign or scheduler service is unavailable.")
            elif campaign.get("status") == "running":
                await query.edit_message_text("Pause or stop the campaign before changing scheduling eligibility.")
            else:
                try:
                    updated = self.scheduler_service.set_campaign_enabled(
                        parts[1], not bool(campaign.get("scheduling_enabled", False)))
                except (ValueError, KeyError) as exc:
                    await query.edit_message_text(str(exc))
                else:
                    await self._show_campaign(query, self._get_campaign(parts[1]) or updated, edit=True)
            return
        if len(parts) == 2 and parts[0] == "resume_suspended":
            selection = self._suspended_resume_tokens.get(parts[1])
            if selection is None or self.campaign_execution is None:
                await query.edit_message_text("This suspended campaign action is stale. Refresh the campaign details.")
                return
            campaign_id, account_id = selection
            lock = self.campaign_execution.account_execution_lock(account_id)
            if lock.locked() or self.campaign_execution.scheduled_account_claimed(account_id):
                await query.edit_message_text(
                    "This account is already running a promotion. Please use another account.",
                    reply_markup=self._dashboard_navigation(None, query.message.chat_id,
                                                             back_data=f"campaign_view:{campaign_id}"),
                )
                return
            resumed = await self.campaign_execution.resume_suspended_account(
                campaign_id, account_id, self._campaign_progress_callback,
            )
            if not resumed:
                if lock.locked() or self.campaign_execution.scheduled_account_claimed(account_id):
                    await query.edit_message_text(
                        "This account is already running a promotion. Please use another account."
                    )
                else:
                    await query.edit_message_text("This suspended campaign cannot be resumed in its current state.")
                return
            campaign = self._get_campaign(campaign_id)
            if campaign is not None:
                await self._show_campaign(query, campaign, edit=True)
            else:
                await query.edit_message_text("The suspended campaign is no longer available.")
            return
        if len(parts) == 3 and parts[0] == "campaign_progress":
            campaign = self._get_campaign(parts[1])
            if campaign is None or not parts[2].isdigit() or int(parts[2]) > 100000:
                await query.edit_message_text("This campaign progress view is stale.")
            else:
                await self._show_campaign_progress(query, campaign, int(parts[2]))
            return
        if len(parts) == 2 and parts[0] == "suspended_page" and parts[1].isdigit():
            await self._suspended_campaigns_page(query, int(parts[1]))
            return
        if len(parts) == 3 and parts[0] == "campaign_config":
            field, campaign_id = parts[1], parts[2]
            campaign = self._get_campaign(campaign_id)
            key = (self.settings.admin_id, query.message.chat_id if query.message else 0)
            if field != "cycles" or campaign is None or query.message is None or query.message.chat.type != "private":
                await query.edit_message_text("Campaign settings action is stale or unavailable.")
            elif campaign.get("status") == "running" or not self._begin_workflow(key, "campaign_setting", field, {"campaign_id": campaign_id}):
                await query.edit_message_text("Pause or stop the campaign before changing its cycle count.")
            else:
                await query.edit_message_text("Send the new total cycles per target (1-100000).", reply_markup=self._flow_navigation(query.message.chat_id))
            return
        if len(parts) == 2 and parts[0] in {"targets_page", "target_optin", "target_remove", "target_edit", "sticker_remove", "message_view", "message_remove", "message_edit", "campaign_view", "campaign_delete", "campaign_edit"}:
            value = parts[1]
            if parts[0] == "targets_page" and value.isdigit() and int(value) <= 100000:
                await self._targets_page(query, int(value), edit=True)
            elif parts[0] == "target_optin":
                target = self._target_by_id(value)
                if target is None:
                    await query.edit_message_text("This target is stale or invalid.")
                else:
                    self.target_repository.update(value, {
                        "opted_in": True,
                        "opt_in_source": "owner_confirmed_explicit_request_or_consent",
                        "opted_in_at": datetime.now(timezone.utc).isoformat(),
                    })
                    await query.edit_message_text(
                        f"@{target['username']} is marked opted in.",
                        reply_markup=self._dashboard_navigation(None, query.message.chat_id, back_data="dash:section:targets"),
                    )
            elif parts[0] in {"target_remove", "sticker_remove", "message_remove", "campaign_delete"}:
                kind = {"target_remove": "target", "sticker_remove": "sticker", "message_remove": "message", "campaign_delete": "campaign"}[parts[0]]
                record = self._lookup_action_record(kind, value)
                if record is None:
                    await query.edit_message_text("This item is stale or no longer available.")
                else:
                    await self._ask_action_confirmation(query, kind, value, record)
            elif parts[0] == "target_edit":
                target = self._target_by_id(value)
                key = (self.settings.admin_id, query.message.chat_id)
                if target is None or query.message.chat.type != "private":
                    await query.edit_message_text("Target edit is unavailable here. Open the dashboard in a private chat.")
                elif not self._begin_workflow(key, "edit_target", "username", {"target_id": value}):
                    await query.edit_message_text("Finish or cancel account authentication before editing a target.")
                else:
                    await query.edit_message_text(
                        f"Send the new username for @{target['username']}. Changing it clears opt-in until reconfirmed.",
                        reply_markup=self._flow_navigation(query.message.chat_id),
                    )
            elif parts[0] == "message_edit":
                record = self._get_message(value)
                key = (self.settings.admin_id, query.message.chat_id)
                if record is None or query.message.chat.type != "private":
                    await query.edit_message_text("Message edit is unavailable here or the record is stale.")
                elif any(campaign.get("message_id") == value for campaign in self.campaign_repository.list()):
                    await query.edit_message_text("This message is referenced by a campaign draft. Delete that draft before editing its content.")
                elif not self._begin_workflow(key, "edit_message", "text", {"message_id": value, "sticker_id": record["sticker_id"]}):
                    await query.edit_message_text("Finish or cancel account authentication before editing a message.")
                else:
                    await query.edit_message_text("Send the replacement promotion text. The saved sticker will remain first in the sequence.", reply_markup=self._flow_navigation(query.message.chat_id))
            elif parts[0] == "campaign_edit":
                campaign = self._get_campaign(value)
                key = (self.settings.admin_id, query.message.chat_id)
                if campaign is None or query.message.chat.type != "private":
                    await query.edit_message_text("Campaign edit is unavailable here or the record is stale.")
                elif not self._begin_workflow(key, "edit_campaign", "name", {"campaign_id": value}):
                    await query.edit_message_text("Finish or cancel account authentication before editing a campaign.")
                else:
                    await query.edit_message_text("Send the new campaign draft name (up to 80 characters).", reply_markup=self._flow_navigation(query.message.chat_id))
            elif parts[0] == "message_view":
                record = self._get_message(value)
                if record is None:
                    await query.edit_message_text("Promotion message is stale or unavailable.")
                else:
                    await self._show_message_record(query, record, edit=True)
            elif parts[0] == "campaign_view":
                record = self._get_campaign(value)
                if record is None:
                    await query.edit_message_text("Campaign is stale or unavailable.")
                else:
                    await self._show_campaign(query, record, edit=True)
            return
        if len(parts) == 2 and parts[0] in {"campaign_page", "message_page"} and parts[1].isdigit() and int(parts[1]) <= 100000:
            if parts[0] == "campaign_page":
                await self._campaigns_page(query, int(parts[1]), edit=True)
            else:
                await self._messages_page(query, int(parts[1]), edit=True)
            return
        if len(parts) == 3 and parts[0] == "stickers_page":
            purpose, page_text = parts[1], parts[2]
            if purpose not in {"view", "remove", "message", "campaign", "test"} or not page_text.isdigit():
                await query.edit_message_text("Invalid sticker selection.")
            else:
                await self._stickers_page(query, int(page_text), edit=True, purpose=purpose)
            return
        if len(parts) == 4 and parts[0] == "sticker_select":
            purpose, sticker_id, page_text = parts[1], parts[2], parts[3]
            sticker = self._get_sticker(sticker_id)
            if sticker is None or not page_text.isdigit():
                await query.edit_message_text("This sticker is stale or unavailable.")
            elif purpose == "remove":
                await self._ask_action_confirmation(query, "sticker", sticker_id, sticker)
            elif purpose == "view":
                markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Remove sticker", callback_data=f"sticker_remove:{sticker_id}")],
                    [InlineKeyboardButton("Back", callback_data="stickers_page:view:0"), InlineKeyboardButton("Home", callback_data="dash:home")],
                ])
                await query.edit_message_text(f"Sticker: {self._sticker_name(sticker)}\nID: {sticker_id}\nFormat: {sticker.get('format', 'unknown')}\nFile: {sticker.get('file_path')}", reply_markup=markup)
            elif purpose == "message":
                key = (self.settings.admin_id, query.message.chat_id)
                flow = self._workflows.get(key)
                if flow is None or flow.kind != "message" or flow.step != "sticker":
                    await query.edit_message_text("This content workflow expired. Start /createmessage again.")
                else:
                    flow.data["sticker_id"] = sticker_id
                    flow.step = "text"
                    await query.edit_message_text("Sticker selected. Now send the promotion text that follows it.", reply_markup=self._flow_navigation(query.message.chat_id))
            elif purpose == "test":
                key = (self.settings.admin_id, query.message.chat_id)
                flow = self._workflows.get(key)
                if flow is None or flow.kind != "test_sticker" or flow.step != "sticker":
                    await query.edit_message_text("This sticker test expired. Start again with /teststicker @botusername.")
                else:
                    await self._send_test_sticker(query, key, flow, sticker)
            elif purpose == "campaign":
                key = (self.settings.admin_id, query.message.chat_id)
                flow = self._workflows.get(key)
                if flow is None or flow.kind != "campaign" or flow.step != "sticker":
                    await query.edit_message_text("This campaign workflow expired. Start /createcampaign again.")
                else:
                    flow.data["sticker_id"] = sticker_id
                    flow.step = "confirm"
                    try:
                        await self._preview_sticker_and_text(
                            query.message.chat_id,
                            sticker,
                            PROMOTION_MESSAGES[int(flow.data.get("rotation_index", 0)) % len(PROMOTION_MESSAGES)],
                        )
                    except Exception as exc:
                        logger.warning("Could not preview campaign (error=%s)", type(exc).__name__)
                        self._workflows.pop(key, None)
                        await query.edit_message_text("Campaign preview failed. Nothing was saved or started.")
                        return
                    nonce = secrets.token_urlsafe(6)
                    flow.data["confirmation"] = nonce
                    self._action_confirmations[nonce] = ActionConfirmation(
                        "save_campaign", str(self.settings.admin_id), query.message.chat_id,
                        datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
                    )
                    markup = self._with_flow_navigation(InlineKeyboardMarkup([[
                        InlineKeyboardButton("Confirm and start", callback_data=f"content_save:{nonce}"),
                        InlineKeyboardButton("Cancel", callback_data=f"content_cancel:{nonce}"),
                    ]]), query.message.chat_id)
                    await query.edit_message_text("Campaign preview shown above. Confirm to save and start it.", reply_markup=markup)
            return
        if len(parts) == 2 and parts[0] in {"action_yes", "action_no", "content_save", "content_cancel"}:
            await self._handle_action_confirmation(query, parts[0], parts[1])
            return
        if len(parts) == 2 and parts[0] == "test_account":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            account = self._get_account(parts[1])
            if flow is None or flow.kind != "test_sticker" or flow.step != "account" or account is None:
                await query.edit_message_text("This account selection is stale or unavailable.")
                return
            account_id = str(account["id"])
            if not await self.client_manager.is_connected(account_id) or not await self.client_manager.is_authorized(account_id):
                self._workflows.pop(key, None)
                await query.edit_message_text("The selected account is disconnected or unauthorized. Nothing was sent.")
                return
            flow.data["account_id"] = account_id
            flow.step = "sticker"
            await self._stickers_page(query, 0, edit=True, purpose="test")
            return
        if len(parts) == 2 and parts[0] == "campaign_account":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            account = self._get_account(parts[1])
            if flow is None or flow.kind != "campaign" or flow.step != "account" or account is None:
                await query.edit_message_text("This account selection is stale or unavailable.")
            else:
                selected = set(flow.data.get("account_ids", []))
                if account["id"] in selected:
                    selected.remove(account["id"])
                else:
                    selected.add(account["id"])
                flow.data["account_ids"] = sorted(selected)
                await self._show_campaign_accounts(query, flow, int(flow.data.get("account_page", 0)), edit=True)
            return
        if data == "campaign_accounts_done":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            account_ids = flow.data.get("account_ids", []) if flow else []
            if flow is None or flow.kind != "campaign" or flow.step != "account" or not account_ids:
                await query.edit_message_text("Select at least one authenticated account.")
            elif any(self._get_account(str(account_id)) is None for account_id in account_ids):
                await query.edit_message_text("An account selection is no longer available. Refresh the picker.")
            else:
                flow.step = "targets"
                await self._show_campaign_targets(query, flow, edit=True)
            return
        if len(parts) == 2 and parts[0] == "campaign_accounts_page":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            if flow is None or flow.kind != "campaign" or flow.step != "account" or not parts[1].isdigit() or int(parts[1]) > 100000:
                await query.edit_message_text("This campaign account picker is stale.")
            else:
                await self._show_campaign_accounts(query, flow, int(parts[1]), edit=True)
            return
        if len(parts) == 2 and parts[0] == "campaign_messages_page":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            if flow is None or flow.kind != "campaign" or flow.step != "message" or not parts[1].isdigit() or int(parts[1]) > 100000:
                await query.edit_message_text("This campaign message picker is stale.")
            else:
                await self._show_campaign_messages(query, flow, int(parts[1]), edit=True)
            return
        if len(parts) == 2 and parts[0] == "campaign_target":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            target = self._target_by_id(parts[1])
            if flow is None or flow.kind != "campaign" or flow.step != "targets" or target is None or target.get("opted_in") is not True:
                await query.edit_message_text("This target selection is stale, unavailable, or not opted in.")
            else:
                selected = set(flow.data.get("target_ids", []))
                if parts[1] in selected:
                    selected.remove(parts[1])
                else:
                    selected.add(parts[1])
                flow.data["target_ids"] = sorted(selected)
                await self._show_campaign_targets(query, flow, edit=True)
            return
        if len(parts) == 2 and parts[0] == "campaign_target_page":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            if flow is None or flow.kind != "campaign" or flow.step != "targets" or not parts[1].isdigit() or int(parts[1]) > 100000:
                await query.edit_message_text("This campaign target picker is stale.")
            else:
                flow.data["target_page"] = int(parts[1])
                await self._show_campaign_targets(query, flow, edit=True)
            return
        if data.startswith("campaign_targets_done"):
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            if flow is None or flow.kind != "campaign" or flow.step != "targets" or not flow.data.get("target_ids"):
                await query.edit_message_text("Select at least one currently opted-in target.")
            else:
                flow.step = "cycles"
                await query.edit_message_text("Send the number of cycles per selected target (1-100000).", reply_markup=self._flow_navigation(query.message.chat_id))
            return
        if len(parts) == 2 and parts[0] == "campaign_message":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            record = self._get_message(parts[1])
            if flow is None or flow.kind != "campaign" or flow.step != "message" or record is None or record.get("sticker_id") != flow.data.get("sticker_id"):
                await query.edit_message_text("This content selection is stale or its sticker sequence does not match.")
            else:
                flow.data["message_id"] = parts[1]
                flow.step = "confirm"
                try:
                    await self._preview_sticker_and_text(query.message.chat_id, self._get_sticker(flow.data["sticker_id"]), record["text"])
                except Exception as exc:
                    logger.warning("Could not preview campaign content (error=%s)", type(exc).__name__)
                    self._workflows.pop(key, None)
                    await query.edit_message_text("Preview failed. Campaign draft not saved.")
                    return
                nonce = secrets.token_urlsafe(6)
                self._action_confirmations[nonce] = ActionConfirmation("save_campaign", str(self.settings.admin_id), query.message.chat_id, datetime.now(timezone.utc) + _CONFIRM_TIMEOUT)
                flow.data["confirmation"] = nonce
                markup = InlineKeyboardMarkup([[InlineKeyboardButton("Save draft", callback_data=f"content_save:{nonce}"), InlineKeyboardButton("Cancel", callback_data=f"content_cancel:{nonce}")]])
                markup = self._with_flow_navigation(markup, query.message.chat_id)
                await query.edit_message_text("Preview shown privately above. Save this campaign draft?", reply_markup=markup)
            return
        if len(parts) == 3 and parts[0] == "target_view":
            target_id, page_text = parts[1], parts[2]
            target = self._target_by_id(target_id)
            if target is None or not page_text.isdigit() or int(page_text) > 100000:
                await query.edit_message_text("This target is stale or invalid.")
            else:
                await self._show_target(query, target, edit=True)
            return
        if len(parts) == 3 and parts[0] == "pick":
            action, page_text = parts[1], parts[2]
            if not page_text.isdigit() or int(page_text) > 100000:
                await query.edit_message_text("This button is stale or invalid. Use the command again.")
                return
            if action == "status":
                accounts = self._authenticated_accounts()
                text, keyboard = await self._status_page(accounts, int(page_text))
                chat_id = query.message.chat_id if query.message else 0
                await query.edit_message_text(text, reply_markup=self._dashboard_navigation(keyboard, chat_id))
            elif action in {"account", "disconnect", "reconnect", "remove"}:
                await self._edit_account_picker(query, action, int(page_text))
            else:
                await query.edit_message_text("This selection is no longer available.")
            return
        if len(parts) == 3 and parts[0] == "account":
            account_id, page_text = parts[1], parts[2]
            if not self._valid_account_id(account_id) or not page_text.isdigit() or int(page_text) > 100000:
                await query.edit_message_text("Invalid account selection.")
                return
            await self._account_details(query, account_id, int(page_text), edit=True)
            return
        if len(parts) == 2 and parts[0] in {"disconnect", "reconnect", "remove"}:
            action, account_id = parts
            if not self._valid_account_id(account_id):
                await query.edit_message_text("Invalid account selection.")
                return
            if action == "disconnect":
                await self._disconnect_selected(query, account_id)
            elif action == "reconnect":
                await self._reconnect_selected(query, account_id)
            else:
                await self._ask_remove_confirmation(query, account_id)
            return
        if len(parts) == 2 and parts[0] in {"remove_yes", "remove_no"}:
            await self._handle_remove_confirmation(query, parts[0], parts[1])
            return
        await query.edit_message_text("This button is stale or unsupported. Use /accounts and try again.")

    async def _edit_account_picker(self, query, action: str, page: int) -> None:
        accounts = self._authenticated_accounts()
        if not accounts:
            await query.edit_message_text("No authenticated Telegram accounts are saved.", reply_markup=self._dashboard_navigation(None, query.message.chat_id if query.message else 0))
            return
        total_pages = (len(accounts) + _PAGE_SIZE - 1) // _PAGE_SIZE
        page = max(0, min(page, total_pages - 1))
        rows = []
        for account in accounts[page * _PAGE_SIZE : (page + 1) * _PAGE_SIZE]:
            account_id = str(account["id"])
            connected = await self.client_manager.is_connected(account_id)
            label = f"{account.get('name') or account.get('phone', 'Phone unavailable')} - {'Connected' if connected else 'Disconnected'}"
            callback_data = (
                f"account:{account_id}:{page}" if action == "account" else f"{action}:{account_id}"
            )
            rows.append([InlineKeyboardButton(label[:60], callback_data=callback_data)])
        pager = self._page_keyboard(action, page, total_pages)
        if pager is not None:
            rows.extend(pager.inline_keyboard)
        markup = self._dashboard_navigation(InlineKeyboardMarkup(rows), query.message.chat_id if query.message else 0)
        await query.edit_message_text(
            f"Select an account ({page + 1}/{total_pages}):",
            reply_markup=markup,
        )

    async def _disconnect_selected(self, query, account_id: str) -> None:
        account = self._get_account(account_id)
        if account is None:
            await query.edit_message_text("This account is no longer available.")
            return
        try:
            await self.client_manager.disconnect(account_id)
        except Exception as exc:
            logger.warning("Could not disconnect selected Telegram account (error=%s)", type(exc).__name__)
            await query.edit_message_text("Could not disconnect this account. Its saved session was preserved.")
            return
        await self._account_details(query, account_id, edit=True)

    async def _reconnect_selected(self, query, account_id: str) -> None:
        if self._get_account(account_id) is None:
            await query.edit_message_text("This account is no longer available.")
            return
        if not self.client_manager.session_file_exists(account_id):
            await query.edit_message_text("Session file is missing. The account was not changed.")
            return
        try:
            connected = await self.client_manager.connect(account_id)
        except Exception as exc:
            logger.warning("Could not reconnect selected Telegram account (error=%s)", type(exc).__name__)
            await query.edit_message_text("Could not reconnect this account. Check the session and try again.")
            return
        if not connected:
            await query.edit_message_text("Telegram did not authorize the saved session. The account remains saved.")
            return
        await self._account_details(query, account_id, edit=True)

    async def _ask_remove_confirmation(self, query, account_id: str) -> None:
        account = self._get_account(account_id)
        if account is None:
            await query.edit_message_text("This account is no longer available.")
            return
        nonce = secrets.token_urlsafe(6)
        chat_id = query.message.chat_id if query.message is not None else 0
        self._removal_confirmations[nonce] = RemovalConfirmation(
            account_id=account_id,
            chat_id=chat_id,
            expires_at=datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
        )
        markup = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("Yes, remove", callback_data=f"remove_yes:{nonce}"),
                InlineKeyboardButton("Cancel", callback_data=f"remove_no:{nonce}"),
            ], [
                InlineKeyboardButton("Back", callback_data="dash:cancel:back"),
                InlineKeyboardButton("Home", callback_data="dash:cancel:home"),
            ]]
        )
        await query.edit_message_text(
            f"Remove {account.get('name') or account.get('phone', 'this account')}? Its Telethon session and account metadata will be deleted.",
            reply_markup=markup,
        )

    async def _handle_remove_confirmation(self, query, action: str, nonce: str) -> None:
        confirmation = self._removal_confirmations.pop(nonce, None)
        query_chat_id = query.message.chat_id if query.message is not None else 0
        if (
            confirmation is None
            or confirmation.chat_id != query_chat_id
            or confirmation.expires_at <= datetime.now(timezone.utc)
        ):
            await query.edit_message_text("This confirmation expired or is no longer valid. Use /removeaccount again.")
            return
        account_id = confirmation.account_id
        if not self._valid_account_id(account_id) or self._get_account(account_id) is None:
            await query.edit_message_text("This account is no longer available. Use /accounts to refresh.")
            return
        if action == "remove_no":
            await query.edit_message_text("Account removal cancelled.", reply_markup=self._dashboard_navigation(None, query_chat_id))
            return
        try:
            session_path = self.client_manager.session_file_path(account_id)
            await self.client_manager.discard_client(account_id)
            for suffix in ("", "-journal", "-wal", "-shm"):
                path = session_path if not suffix else session_path.with_name(session_path.name + suffix)
                path.unlink(missing_ok=True)
            if not self.account_repository.delete(account_id):
                await query.edit_message_text("Account metadata was already removed. Use /accounts to refresh.")
                return
        except OSError as exc:
            logger.warning("Could not remove Telegram account files (error=%s)", type(exc).__name__)
            await query.edit_message_text("Could not remove the session files. Account metadata was retained.")
            return
        except Exception as exc:
            logger.warning("Could not remove Telegram account (error=%s)", type(exc).__name__)
            await query.edit_message_text("Could not remove this account. Its metadata was retained where possible.")
            return
        await query.edit_message_text("Telegram account, session files, and metadata removed.", reply_markup=self._dashboard_navigation(None, query_chat_id))

    async def _add_account_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        key = self._flow_key(update)
        message = update.effective_message
        if key is None or message is None:
            return
        if message.chat.type != "private":
            await message.reply_text("For account security, use /addaccount in a private chat with this bot.")
            return
        async with self._flow_lock:
            current = self._auth_flows.get(key)
            if current is not None and current.expires_at > datetime.now(timezone.utc):
                await message.reply_text("An account authentication is already in progress. Send /cancel to stop it.")
                return
            if current is not None:
                if current.auth_token:
                    await self.auth_service.cancel(current.auth_token)
                self._auth_flows.pop(key, None)
                self._active_flow_key = None
            if self._active_flow_key is not None and self._active_flow_key != key:
                await message.reply_text("Another account authentication is active. Finish it or send /cancel first.")
                return
            self._workflows.pop(key, None)
            self._auth_flows[key] = BotAuthenticationFlow(
                stage="name",
                expires_at=datetime.now(timezone.utc) + _FLOW_TIMEOUT,
            )
            self._active_flow_key = key
            await self._edit_or_reply(
                update,
                message,
                "Send a name for this Telegram account so it is easy to recognize. Login code and password messages are deleted where Telegram permits.",
                self._flow_navigation(message.chat_id),
            )

    async def _cancel_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        key = self._flow_key(update)
        message = update.effective_message
        if key is None or message is None:
            return
        async with self._flow_lock:
            flow = self._auth_flows.pop(key, None)
            if flow is not None and flow.auth_token:
                await self.auth_service.cancel(flow.auth_token)
            if self._active_flow_key == key:
                self._active_flow_key = None
            workflow = self._workflows.pop(key, None)
        self._discard_pending_sticker_upload(workflow)
        if flow or workflow:
            await message.reply_text("The active workflow was cancelled.")
        else:
            await message.reply_text("No active workflow to cancel.")

    async def _authentication_message(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        key = self._flow_key(update)
        message = update.effective_message
        if key is None or message is None or message.text is None:
            return
        async with self._flow_lock:
            flow = self._auth_flows.get(key)
            if flow is None:
                await self._handle_workflow_text(key, message)
                return
            if flow.expires_at <= datetime.now(timezone.utc):
                self._auth_flows.pop(key, None)
                if flow.auth_token:
                    await self.auth_service.cancel(flow.auth_token)
                self._active_flow_key = None
                await self._send_chat_message(message, "Authentication expired. Start again with /addaccount.")
                return

            await self._delete_sensitive_message(update)
            if flow.stage == "name":
                await self._handle_account_name(message, flow)
            elif flow.stage == "phone":
                await self._handle_phone(message, flow)
            elif flow.stage == "code":
                await self._handle_code(message, key, flow)
            elif flow.stage == "password":
                await self._handle_password(message, key, flow)

    async def _handle_account_name(self, message, flow: BotAuthenticationFlow) -> None:
        name = message.text.strip()
        if not name or len(name) > 64:
            await self._send_chat_message(message, "Enter an account name between 1 and 64 characters.")
            return
        flow.account_name = name
        flow.stage = "phone"
        await self._send_chat_message(
            message,
            "This Telegram chat is not end-to-end encrypted. Send the account phone number in international format, for example +12345678900.",
        )

    async def _handle_phone(self, message, flow: BotAuthenticationFlow) -> None:
        try:
            flow.auth_token = await self.auth_service.start(message.text, flow.account_name or "")
        except AuthenticationFlowError as exc:
            await self._send_chat_message(message, str(exc))
            return
        except Exception as exc:
            logger.warning("Could not start account authentication (error=%s)", type(exc).__name__)
            await self._send_chat_message(message, "Could not start account authentication. Check the phone number and try again.")
            return
        flow.stage = "code"
        flow.expires_at = datetime.now(timezone.utc) + _FLOW_TIMEOUT
        await self._send_chat_message(message, "Enter the login code sent by Telegram.")

    async def _handle_code(
        self,
        message,
        key: tuple[int, int],
        flow: BotAuthenticationFlow,
    ) -> None:
        try:
            requires_password = await self.auth_service.verify_code(flow.auth_token or "", message.text)
        except AuthenticationFlowError as exc:
            if "expired" in str(exc).lower() or "cancelled" in str(exc).lower():
                if flow.auth_token:
                    await self.auth_service.cancel(flow.auth_token)
                self._auth_flows.pop(key, None)
                self._active_flow_key = None
                await self._send_chat_message(message, "The login code expired. Start again with /addaccount.")
            else:
                await self._send_chat_message(message, str(exc))
            return
        except Exception as exc:
            logger.warning("Could not verify Telegram login code (error=%s)", type(exc).__name__)
            await self._send_chat_message(message, "Could not verify the login code. Try again or send /cancel.")
            return

        if requires_password:
            flow.stage = "password"
            flow.expires_at = datetime.now(timezone.utc) + _FLOW_TIMEOUT
            await self._send_chat_message(message, "This account requires 2FA. Send its password as the next message.")
            return
        self._auth_flows.pop(key, None)
        self._active_flow_key = None
        await self._send_chat_message(message, f"Telegram account '{flow.account_name}' authenticated and added successfully.")

    async def _handle_password(
        self,
        message,
        key: tuple[int, int],
        flow: BotAuthenticationFlow,
    ) -> None:
        try:
            await self.auth_service.verify_password(flow.auth_token or "", message.text)
        except AuthenticationFlowError as exc:
            await self._send_chat_message(message, str(exc))
            return
        except Exception as exc:
            logger.warning("Could not verify Telegram 2FA password (error=%s)", type(exc).__name__)
            await self._send_chat_message(message, "Could not verify the 2FA password. Try again or send /cancel.")
            return
        self._auth_flows.pop(key, None)
        self._active_flow_key = None
        await self._send_chat_message(message, f"Telegram account '{flow.account_name}' authenticated and added successfully.")

    # Phase 5.1: target, content, and campaign planning. These handlers only
    # write local JSON/files and preview content to the owner; they never send
    # campaign content to targets.
    def _begin_workflow(self, key: tuple[int, int], kind: str, step: str, data: dict | None = None) -> bool:
        if key in self._auth_flows:
            return False
        self._workflows[key] = Workflow(
            kind=kind,
            step=step,
            data=data or {},
            expires_at=datetime.now(timezone.utc) + _FLOW_TIMEOUT,
        )
        return True

    @staticmethod
    def _private_message(message) -> bool:
        return message is not None and message.chat.type == "private"

    async def _handle_workflow_text(self, key: tuple[int, int], message) -> None:
        workflow = self._workflows.get(key)
        if workflow is None:
            return
        if workflow.expires_at <= datetime.now(timezone.utc):
            self._workflows.pop(key, None)
            self._discard_pending_sticker_upload(workflow)
            await message.reply_text("The active workflow expired. Start it again with a command.")
            return
        workflow.expires_at = datetime.now(timezone.utc) + _FLOW_TIMEOUT
        text = message.text.strip()
        if workflow.kind == "promotion_config" and workflow.step == "name":
            if not text or len(text) > 80:
                await message.reply_text("Enter a promotion name between 1 and 80 characters, or cancel.",
                                         reply_markup=self._flow_navigation(message.chat_id))
                return
            workflow.data["name"] = text
            workflow.step = "target"
            await self._show_promotion_targets(message, 0)
            return
        if workflow.kind == "campaign_setting" and workflow.step == "cycles":
            campaign_id = str(workflow.data.get("campaign_id", ""))
            campaign = self._get_campaign(campaign_id)
            try:
                cycles = int(text)
            except ValueError:
                cycles = 0
            if campaign is None or cycles < 1 or cycles > 100000:
                await message.reply_text("Enter a whole number from 1 to 100000, or use /cancel.", reply_markup=self._flow_navigation(message.chat_id))
                return
            progress = dict(campaign.get("progress") or {})
            total = cycles * len(campaign["account_ids"]) * len(campaign["target_ids"])
            if total < int(progress.get("completed", 0)):
                await message.reply_text("The new cycle count is below completed progress. Enter a larger number or /cancel.", reply_markup=self._flow_navigation(message.chat_id))
                return
            changes = {"total_cycles": cycles, "progress": {**progress, "total": total}}
            if campaign.get("status") == "completed" and total > int(progress.get("completed", 0)):
                changes.update({"status": "paused", "execution_state": "paused"})
            self.campaign_repository.update(campaign_id, changes)
            self._workflows.pop(key, None)
            await message.reply_text("Campaign cycle count saved.", reply_markup=self._dashboard_navigation(None, message.chat_id, back_data=f"campaign_view:{campaign_id}"))
            return
        if workflow.kind == "upload_sticker" and workflow.step == "name":
            sticker_name = text[:80].strip()
            if not sticker_name:
                await message.reply_text("Sticker name must contain 1-80 characters. Enter a name or /cancel.", reply_markup=self._flow_navigation(message.chat_id))
                return
            if any(self._sticker_name(item).casefold() == sticker_name.casefold() for item in self.sticker_repository.list()):
                await message.reply_text("That sticker name is already in use. Choose another name or /cancel.", reply_markup=self._flow_navigation(message.chat_id))
                return
            data = workflow.data
            record = self.sticker_repository.create({
                "id": data["sticker_id"], "name": sticker_name,
                "file_path": data["file_path"], "telegram_file_id": data["telegram_file_id"],
                "telegram_file_unique_id": data.get("telegram_file_unique_id"),
                "emoji": data.get("emoji"), "set_name": data.get("set_name"),
                "format": data.get("format", "webp"),
                "is_animated": data.get("is_animated", False),
                "is_video": data.get("is_video", False),
                "width": data.get("width"), "height": data.get("height"),
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            self._workflows.pop(key, None)
            await message.reply_text(f"Sticker '{record['name']}' saved. ID: {record['id']}", reply_markup=self._dashboard_navigation(None, message.chat_id, back_data="dash:section:stickers"))
            return
        if workflow.kind == "campaign" and workflow.step == "cycles":
            try:
                cycles = int(text)
            except ValueError:
                cycles = 0
            if cycles < 1 or cycles > 100000:
                await message.reply_text("Enter a whole number of cycles from 1 to 100000, or /cancel.", reply_markup=self._flow_navigation(message.chat_id))
                return
            workflow.data["total_cycles"] = cycles
            workflow.step = "sticker"
            await self._stickers_page(message, 0, purpose="campaign")
            return
        if workflow.kind == "add_target" and workflow.step == "username":
            username = text.removeprefix("@").strip()
            if not _USERNAME_PATTERN.fullmatch(username):
                await message.reply_text("Enter a valid Telegram username (5–32 letters, numbers, or underscores).")
                return
            normalized = username.casefold()
            if any(str(item.get("username", "")).casefold() == normalized for item in self.target_repository.list()):
                await message.reply_text("That target is already saved.")
                return
            record = self.target_repository.create({
                "id": uuid.uuid4().hex,
                "username": username,
                "opted_in": False,
                "opt_in_source": None,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            self._workflows.pop(key, None)
            markup = InlineKeyboardMarkup([[InlineKeyboardButton(
                "Confirm recipient opted in", callback_data=f"target_optin:{record['id']}"
            )]])
            markup = self._dashboard_navigation(markup, message.chat_id, back_data="dash:section:targets")
            await message.reply_text(
                f"Saved @{username} as a target with opted_in=false. Campaign drafts only allow targets marked opted in. "
                "If this recipient explicitly requested or consented to receive the promotion, confirm below.",
                reply_markup=markup,
            )
            return
        if workflow.kind == "edit_target" and workflow.step == "username":
            target_id = str(workflow.data.get("target_id", ""))
            target = self._target_by_id(target_id)
            username = text.removeprefix("@").strip()
            if target is None or not _USERNAME_PATTERN.fullmatch(username):
                await message.reply_text("Target or username is invalid. Enter a valid username or cancel.", reply_markup=self._flow_navigation(message.chat_id))
                return
            if any(
                item["id"] != target_id and str(item.get("username", "")).casefold() == username.casefold()
                for item in self.target_repository.list()
            ):
                await message.reply_text("That username is already registered. Enter another username.", reply_markup=self._flow_navigation(message.chat_id))
                return
            changed = target["username"].casefold() != username.casefold()
            self.target_repository.update(target_id, {
                "username": username,
                "telegram_user_id": None if changed else target.get("telegram_user_id"),
                "opted_in": False if changed else target.get("opted_in", False),
                "opt_in_source": None if changed else target.get("opt_in_source"),
                "opted_in_at": None if changed else target.get("opted_in_at"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            self._workflows.pop(key, None)
            await message.reply_text(
                f"Target updated to @{username}. Explicit opt-in must be confirmed again if the username changed.",
                reply_markup=self._dashboard_navigation(None, message.chat_id, back_data="dash:section:targets"),
            )
            return
        if workflow.kind == "message" and workflow.step == "text":
            if not text or len(text) > _TEXT_LIMIT:
                await message.reply_text(f"Enter promotion text between 1 and {_TEXT_LIMIT} characters.")
                return
            sticker = self._get_sticker(workflow.data.get("sticker_id"))
            if sticker is None:
                self._workflows.pop(key, None)
                await message.reply_text("The selected sticker is no longer available. Start /createmessage again.")
                return
            workflow.data["text"] = text
            workflow.step = "preview"
            try:
                await self._preview_sticker_and_text(message.chat_id, sticker, text)
            except Exception as exc:
                logger.warning("Could not preview promotion content (error=%s)", type(exc).__name__)
                await message.reply_text("Preview failed. The promotion was not saved; retry /createmessage.")
                self._workflows.pop(key, None)
                return
            nonce = secrets.token_urlsafe(6)
            self._action_confirmations[nonce] = ActionConfirmation(
                kind="save_message", record_id=key[0] and str(key[0]), chat_id=message.chat_id,
                expires_at=datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
            )
            workflow.data["confirmation"] = nonce
            markup = InlineKeyboardMarkup([[ 
                InlineKeyboardButton("Save this sequence", callback_data=f"content_save:{nonce}"),
                InlineKeyboardButton("Cancel", callback_data=f"content_cancel:{nonce}"),
            ]])
            markup = self._with_flow_navigation(markup, message.chat_id)
            await message.reply_text("Save exactly this sticker followed by this text?", reply_markup=markup)
            return
        if workflow.kind == "edit_message" and workflow.step == "text":
            if not text or len(text) > _TEXT_LIMIT:
                await message.reply_text(f"Enter promotion text between 1 and {_TEXT_LIMIT} characters.", reply_markup=self._flow_navigation(message.chat_id))
                return
            record = self._get_message(str(workflow.data.get("message_id", "")))
            sticker = self._get_sticker(workflow.data.get("sticker_id"))
            if record is None or sticker is None:
                self._workflows.pop(key, None)
                await message.reply_text("The selected promotion message is no longer available.", reply_markup=self._dashboard_navigation(None, message.chat_id))
                return
            workflow.data["text"] = text
            workflow.step = "confirm"
            try:
                await self._preview_sticker_and_text(message.chat_id, sticker, text)
            except Exception as exc:
                logger.warning("Could not preview edited promotion content (error=%s)", type(exc).__name__)
                self._workflows.pop(key, None)
                await message.reply_text("Preview failed. The existing message was not changed.", reply_markup=self._dashboard_navigation(None, message.chat_id))
                return
            nonce = secrets.token_urlsafe(6)
            workflow.data["confirmation"] = nonce
            self._action_confirmations[nonce] = ActionConfirmation(
                kind="update_message",
                record_id=str(workflow.data["message_id"]),
                chat_id=message.chat_id,
                expires_at=datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("Save edited text", callback_data=f"content_save:{nonce}"), InlineKeyboardButton("Cancel", callback_data=f"content_cancel:{nonce}")]])
            await message.reply_text("Save this edited sticker-then-text sequence?", reply_markup=self._with_flow_navigation(markup, message.chat_id))
            return
        if workflow.kind == "edit_campaign" and workflow.step == "name":
            if not _CAMPAIGN_NAME_PATTERN.fullmatch(text):
                await message.reply_text("Campaign name must be between 1 and 80 characters.", reply_markup=self._flow_navigation(message.chat_id))
                return
            campaign_id = str(workflow.data.get("campaign_id", ""))
            campaign = self._get_campaign(campaign_id)
            if campaign is None:
                self._workflows.pop(key, None)
                await message.reply_text("Campaign is no longer available.", reply_markup=self._dashboard_navigation(None, message.chat_id))
                return
            self.campaign_repository.update(campaign_id, {"name": text, "updated_at": datetime.now(timezone.utc).isoformat()})
            self._workflows.pop(key, None)
            await message.reply_text("Campaign draft name updated.", reply_markup=self._dashboard_navigation(None, message.chat_id, back_data="dash:section:campaigns"))
            return
        if workflow.kind == "campaign" and workflow.step == "name":
            if not _CAMPAIGN_NAME_PATTERN.fullmatch(text):
                await message.reply_text("Campaign name must be between 1 and 80 characters.")
                return
            workflow.data["name"] = text
            workflow.step = "account"
            await self._show_campaign_accounts(message, workflow)
            return

    async def _add_target_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        key, message = self._flow_key(update), update.effective_message
        if key is None or not self._private_message(message):
            if message is not None:
                await message.reply_text("Use /addtarget in a private chat with this bot.")
            return
        if not self._begin_workflow(key, "add_target", "username"):
            await message.reply_text("Finish or cancel the active account authentication before starting another workflow.")
            return
        await self._edit_or_reply(update, message, "Send the Telegram username (with or without @). It will be marked not opted in until you confirm consent.", self._flow_navigation(message.chat_id))

    def _targets(self, opted_in_only: bool = False) -> list[dict]:
        records = self.target_repository.list()
        if opted_in_only:
            records = [item for item in records if item.get("opted_in") is True]
        return sorted(records, key=lambda item: str(item.get("username", "")).casefold())

    def _target_by_id(self, target_id: str) -> dict | None:
        if not _RECORD_ID_PATTERN.fullmatch(str(target_id)):
            return None
        record = self.target_repository.get(target_id)
        if record is None or not _USERNAME_PATTERN.fullmatch(str(record.get("username", ""))):
            return None
        return record

    async def _targets_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is not None:
            await self._targets_page(message, 0)

    async def _targets_page(self, message_or_query, page: int, edit: bool = False) -> None:
        targets = self._targets()
        pages = max(1, (len(targets) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        rows = [[InlineKeyboardButton(
            f"@{item['username']} · {'opted in' if item.get('opted_in') is True else 'not opted in'}"[:60],
            callback_data=f"target_view:{item['id']}:{page}",
        )] for item in targets[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"targets_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"targets_page:{page + 1}"))
            rows.append(nav)
        text = f"Targets: {len(targets)}. Campaign drafts can select opted-in targets only."
        markup = self._dashboard_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        if edit:
            await message_or_query.edit_message_text(text if targets else "No targets are saved.", reply_markup=markup)
        else:
            await message_or_query.reply_text(text if targets else "No targets are saved.", reply_markup=markup)

    async def _target_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        if context.args:
            target = self._target_by_id(context.args[0])
            if target is None:
                await message.reply_text("Target ID not found. Use /targets to select a saved target.")
                return
            await self._show_target(message, target)
        else:
            await self._targets_page(message, 0)

    async def _show_target(self, message_or_query, target: dict, edit: bool = False) -> None:
        text = f"Target: @{target['username']}\nOpted in: {'Yes' if target.get('opted_in') is True else 'No'}\nID: {target['id']}"
        buttons = [
            [InlineKeyboardButton("Edit username", callback_data=f"target_edit:{target['id']}")],
            [InlineKeyboardButton("Remove target", callback_data=f"target_remove:{target['id']}")],
        ]
        if target.get("opted_in") is not True:
            buttons.insert(0, [InlineKeyboardButton("Confirm explicit opt-in", callback_data=f"target_optin:{target['id']}")])
        buttons.append([InlineKeyboardButton("Back to targets", callback_data="targets_page:0")])
        buttons.append([InlineKeyboardButton("Home", callback_data="dash:home")])
        markup = InlineKeyboardMarkup(buttons)
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    async def _remove_target_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._targets_page(update.effective_message, 0)

    def _get_sticker(self, sticker_id: str | None) -> dict | None:
        if not isinstance(sticker_id, str) or not _RECORD_ID_PATTERN.fullmatch(sticker_id):
            return None
        sticker = self.sticker_repository.get(sticker_id)
        if sticker is None or not isinstance(sticker.get("file_path"), str):
            return None
        try:
            path = self.sticker_file_manager.path(sticker["file_path"])
        except ValueError:
            return None
        return sticker if path.is_file() else None

    @staticmethod
    def _sticker_name(sticker: dict) -> str:
        return str(sticker.get("name") or f"Sticker {str(sticker.get('id', ''))[:8]}")

    def _discard_pending_sticker_upload(self, workflow: Workflow | None) -> None:
        if workflow is None or workflow.kind != "upload_sticker":
            return
        filename = workflow.data.get("file_path")
        if isinstance(filename, str):
            try:
                self.sticker_file_manager.delete(filename, missing_ok=True)
            except Exception as exc:
                logger.warning("Could not clean up pending sticker upload (error=%s)", type(exc).__name__)

    async def _upload_sticker_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        key, message = self._flow_key(update), update.effective_message
        if key is None or not self._private_message(message):
            if message is not None:
                await message.reply_text("Use /uploadsticker in a private chat with this bot.")
            return
        if not self._begin_workflow(key, "upload_sticker", "file"):
            await message.reply_text("Finish or cancel the active account authentication before starting another workflow.")
            return
        await self._edit_or_reply(update, message, "Send one Telegram sticker as a sticker message. It will be stored under the configured media directory.", self._flow_navigation(message.chat_id))

    async def _sticker_upload_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        key, message = self._flow_key(update), update.effective_message
        if key is None or message is None or message.sticker is None:
            return
        workflow = self._workflows.get(key)
        if workflow is None or workflow.kind != "upload_sticker" or workflow.step != "file":
            return
        sticker_obj = message.sticker
        try:
            telegram_file = await context.bot.get_file(sticker_obj.file_id)
            suffix = Path(telegram_file.file_path or "").suffix.lower()
            if suffix not in {".webp", ".tgs", ".webm"}:
                suffix = ".webp"
            sticker_id = uuid.uuid4().hex
            filename = f"{sticker_id}{suffix}"
            destination = self.sticker_file_manager.path(filename)
            destination.parent.mkdir(parents=True, exist_ok=True)
            await telegram_file.download_to_drive(custom_path=destination)
            workflow.data.update({
                "sticker_id": sticker_id,
                "file_path": filename,
                "telegram_file_id": sticker_obj.file_id,
                "telegram_file_unique_id": sticker_obj.file_unique_id,
                "emoji": sticker_obj.emoji,
                "set_name": sticker_obj.set_name,
                "is_animated": bool(getattr(sticker_obj, "is_animated", False)),
                "is_video": bool(getattr(sticker_obj, "is_video", False)),
                "width": getattr(sticker_obj, "width", None),
                "height": getattr(sticker_obj, "height", None),
                "format": suffix.removeprefix("."),
            })
            workflow.step = "name"
        except Exception as exc:
            logger.warning("Could not save uploaded sticker (error=%s)", type(exc).__name__)
            await message.reply_text("Could not save this sticker. Try /uploadsticker again.")
            return
        await message.reply_text("Sticker file received. Send a unique name for it (1-80 characters).", reply_markup=self._flow_navigation(message.chat_id))

    async def _stickers_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is not None:
            await self._stickers_page(message, 0)

    async def _stickers_page(self, message_or_query, page: int, edit: bool = False, purpose: str = "view") -> None:
        stickers = sorted(self.sticker_repository.list(), key=lambda item: str(item.get("created_at", "")), reverse=True)
        pages = max(1, (len(stickers) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        rows = []
        for item in stickers[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]:
            if self._get_sticker(str(item.get("id", ""))) is None:
                continue
            rows.append([InlineKeyboardButton(
                f"{self._sticker_name(item)} {item.get('emoji') or ''}"[:60],
                callback_data=f"sticker_select:{purpose}:{item['id']}:{page}",
            )])
        if pages > 1:
            row = []
            if page > 0:
                row.append(InlineKeyboardButton("Previous", callback_data=f"stickers_page:{purpose}:{page - 1}"))
            if page + 1 < pages:
                row.append(InlineKeyboardButton("Next", callback_data=f"stickers_page:{purpose}:{page + 1}"))
            rows.append(row)
        if purpose in {"message", "campaign", "test"}:
            markup = self._with_flow_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        else:
            markup = self._dashboard_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        text = "Select a sticker:" if purpose in {"message", "campaign", "test"} else f"Stickers: {len(stickers)}"
        if edit:
            await message_or_query.edit_message_text(text if rows else "No usable stickers are saved.", reply_markup=markup)
        else:
            await message_or_query.reply_text(text if rows else "No usable stickers are saved.", reply_markup=markup)

    async def _remove_sticker_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is not None:
            await self._stickers_page(message, 0, purpose="remove")

    async def _test_sticker_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        key, message = self._flow_key(update), update.effective_message
        if key is None or message is None:
            return
        if not self._private_message(message):
            await message.reply_text("Use /teststicker in a private chat with this bot.")
            return
        if len(context.args) != 1:
            await message.reply_text("Usage: /teststicker @botusername")
            return
        username = context.args[0].removeprefix("@").strip()
        if not _USERNAME_PATTERN.fullmatch(username):
            await message.reply_text("Enter one valid Telegram bot username, for example /teststicker @example_bot.")
            return
        target = next(
            (
                item for item in self._targets(opted_in_only=True)
                if str(item.get("username", "")).casefold() == username.casefold()
            ),
            None,
        )
        if target is None:
            await message.reply_text("That bot username must first be saved as a target and explicitly marked opted in.")
            return
        if not self.sticker_repository.list():
            await message.reply_text("No saved stickers are available. Upload one with /uploadsticker first.")
            return
        if not self._begin_workflow(key, "test_sticker", "account", {"target_id": target["id"]}):
            await message.reply_text("Finish or cancel the active account authentication before starting a sticker test.")
            return
        accounts = []
        for account in self._authenticated_accounts():
            account_id = str(account["id"])
            if await self.client_manager.is_connected(account_id) and await self.client_manager.is_authorized(account_id):
                accounts.append(account)
        if not accounts:
            self._workflows.pop(key, None)
            await message.reply_text("No connected, authenticated Telegram account is available for the test.")
            return
        rows = [[InlineKeyboardButton(
            str(account.get("phone", "Telegram account"))[:50],
            callback_data=f"test_account:{account['id']}",
        )] for account in accounts]
        await message.reply_text(
            f"Choose the account to send one test sticker to @{target['username']}.",
            reply_markup=self._with_flow_navigation(InlineKeyboardMarkup(rows), message.chat_id),
        )

    async def _send_test_sticker(self, query, key: tuple[int, int], flow: Workflow, sticker: dict) -> None:
        target = self._target_by_id(str(flow.data.get("target_id", "")))
        account_id = str(flow.data.get("account_id", ""))
        account = self._get_account(account_id)
        client = self.client_manager.get_client(account_id)
        if target is None or target.get("opted_in") is not True or account is None:
            self._workflows.pop(key, None)
            await query.edit_message_text("The target or account is no longer available. Nothing was sent.")
            return
        if client is None or not await self.client_manager.is_connected(account_id) or not await self.client_manager.is_authorized(account_id):
            self._workflows.pop(key, None)
            await query.edit_message_text("The selected account is disconnected or unauthorized. Nothing was sent.")
            return
        try:
            entity = await client.get_entity(target["username"])
            if getattr(entity, "bot", False) is not True:
                self._workflows.pop(key, None)
                await query.edit_message_text("That username does not resolve to a Telegram bot. Nothing was sent.")
                return
            await client.send_file(entity, self.sticker_file_manager.path(sticker["file_path"]))
        except Exception as exc:
            logger.warning("Telegram sticker test failed (error=%s)", type(exc).__name__)
            self._workflows.pop(key, None)
            await query.edit_message_text("Could not send the test sticker. Check the bot username and account access, then try again.")
            return
        self._workflows.pop(key, None)
        await query.edit_message_text(f"One test sticker was sent to @{target['username']}.")

    async def _create_message_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        key, message = self._flow_key(update), update.effective_message
        if key is None or message is None:
            return
        if not self._private_message(message):
            await message.reply_text("Use /createmessage in a private chat with this bot; preview content is sent only to that chat.")
            return
        if not self._begin_workflow(key, "message", "sticker"):
            await message.reply_text("Finish or cancel the active account authentication before starting another workflow.")
            return
        await self._stickers_page(
            update.callback_query or message,
            0,
            edit=update.callback_query is not None,
            purpose="message",
        )

    async def _messages_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        await self._messages_page(message, 0)

    async def _messages_page(self, message_or_query, page: int, edit: bool = False) -> None:
        records = sorted(self.message_repository.list(), key=lambda item: str(item.get("created_at", "")), reverse=True)
        pages = max(1, (len(records) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        rows = [[InlineKeyboardButton(
            f"Message {record['id'][:8]} · {str(record.get('text', ''))[:25]}",
            callback_data=f"message_view:{record['id']}",
        )] for record in records[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE] if self._get_message(str(record.get("id", ""))) is not None]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"message_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"message_page:{page + 1}"))
            rows.append(nav)
        text = f"Saved promotion messages ({len(records)}):" if rows else "No promotion messages are saved."
        markup = self._dashboard_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    async def _message_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        if not context.args:
            await self._messages_command(update, context)
            return
        record = self._get_message(context.args[0])
        if record is None:
            await message.reply_text("Promotion message not found.")
            return
        await self._show_message_record(message, record)

    def _get_message(self, message_id: str) -> dict | None:
        if not _RECORD_ID_PATTERN.fullmatch(message_id):
            return None
        record = self.message_repository.get(message_id)
        if record is None or not isinstance(record.get("text"), str) or self._get_sticker(record.get("sticker_id")) is None:
            return None
        sequence = record.get("sequence")
        if (
            not isinstance(sequence, list)
            or len(sequence) != 2
            or sequence[0] != {"type": "sticker", "sticker_id": record["sticker_id"]}
            or sequence[1] != {"type": "text", "text": record["text"]}
        ):
            return None
        return record

    async def _show_message_record(self, message_or_query, record: dict, edit: bool = False) -> None:
        sticker = self._get_sticker(record["sticker_id"])
        text = f"Promotion content sequence: Sticker → Text\nID: {record['id']}\nText: {record['text']}\nSticker: {record['sticker_id']}"
        markup = self._dashboard_navigation(
            InlineKeyboardMarkup([
                [InlineKeyboardButton("Edit text", callback_data=f"message_edit:{record['id']}")],
                [InlineKeyboardButton("Remove message", callback_data=f"message_remove:{record['id']}")],
            ]),
            self._chat_id(message_or_query),
            back_data="message_page:0",
        )
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    async def _preview_sticker_and_text(self, chat_id: int, sticker: dict, text: str) -> None:
        path = self.sticker_file_manager.path(sticker["file_path"])
        with path.open("rb") as handle:
            await self._application.bot.send_sticker(chat_id=chat_id, sticker=handle)
        await self._application.bot.send_message(chat_id=chat_id, text=text)

    async def _remove_message_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._messages_command(update, context)

    async def _create_campaign_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        key, message = self._flow_key(update), update.effective_message
        if key is None or message is None:
            return
        if not self._private_message(message):
            await message.reply_text("Use /createcampaign in a private chat with this bot.")
            return
        if not self._authenticated_accounts():
            await message.reply_text("Add an authenticated account before creating a campaign draft.")
            return
        if not self._targets(opted_in_only=True):
            await message.reply_text("No explicitly opted-in targets are saved. Campaign drafts cannot select other targets.")
            return
        if not self.sticker_repository.list():
            await message.reply_text("Save at least one named sticker first.")
            return
        if not self._begin_workflow(key, "campaign", "name", {"target_ids": []}):
            await message.reply_text("Finish or cancel the active account authentication before starting another workflow.")
            return
        await self._edit_or_reply(update, message, "Send a name for this campaign (up to 80 characters).", self._flow_navigation(message.chat_id))

    async def _show_campaign_accounts(self, message, workflow: Workflow, page: int = 0, edit: bool = False) -> None:
        accounts = self._authenticated_accounts()
        pages = max(1, (len(accounts) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        workflow.data["account_page"] = page
        selected = set(workflow.data.get("account_ids", []))
        rows = [[InlineKeyboardButton(
            f"{'[x]' if item['id'] in selected else '[ ]'} {str(item.get('name') or item.get('phone', 'Account'))}"[:60],
            callback_data=f"campaign_account:{item['id']}",
        )] for item in accounts[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"campaign_accounts_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"campaign_accounts_page:{page + 1}"))
            rows.append(nav)
        if selected:
            rows.append([InlineKeyboardButton("Continue", callback_data="campaign_accounts_done")])
        markup = self._with_flow_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message))
        text = "Select one or more authenticated accounts, then Continue:"
        if edit:
            await message.edit_message_text(text if accounts else "No authenticated accounts remain.", reply_markup=markup)
        else:
            await message.reply_text(text if accounts else "No authenticated accounts are saved.", reply_markup=markup)
    async def _show_campaign_messages(self, message, workflow: Workflow, page: int = 0, edit: bool = False) -> None:
        records = [item for item in self.message_repository.list() if self._get_message(str(item.get("id", ""))) is not None]
        records.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        pages = max(1, (len(records) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        workflow.data["message_page"] = page
        rows = [[InlineKeyboardButton(
            f"Message {item['id'][:8]} · {str(item.get('text', ''))[:25]}",
            callback_data=f"campaign_message:{item['id']}",
        )] for item in records[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"campaign_messages_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"campaign_messages_page:{page + 1}"))
            rows.append(nav)
        markup = self._with_flow_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message))
        text = "Select the saved sticker-then-text promotion message:"
        if edit:
            await message.edit_message_text(text if records else "No saved promotion messages are available.", reply_markup=markup)
        else:
            await message.reply_text(text if records else "No saved promotion messages are available.", reply_markup=markup)

    async def _campaigns_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        await self._campaigns_page(message, 0)

    async def _campaigns_page(self, message_or_query, page: int, edit: bool = False, purpose: str = "view") -> None:
        control_action = "start" if purpose == "run" else purpose
        records = sorted(self.campaign_repository.list(), key=lambda item: str(item.get("created_at", "")), reverse=True)
        pages = max(1, (len(records) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        rows = [[InlineKeyboardButton(
            f"{record.get('name', 'Campaign')} · {record.get('status', 'draft')}"[:60],
            callback_data=(f"campaign_control:{control_action}:{record['id']}" if purpose in {"run", "pause", "stop"} else f"campaign_view:{record['id']}"),
        )] for record in records[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE] if self._get_campaign(str(record.get("id", ""))) is not None]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"campaign_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"campaign_page:{page + 1}"))
            rows.append(nav)
        text = f"Campaigns ({len(records)}):" if rows else "No campaigns are saved."
        markup = self._dashboard_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    async def _suspended_campaigns_page(self, query, page: int = 0) -> None:
        records = [record for record in self.campaign_repository.list()
                   if record.get("suspended_accounts")]
        records.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        pages = max(1, (len(records) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        rows = [[InlineKeyboardButton(
            f"{record.get('name', 'Campaign')} ({len(record.get('suspended_accounts', []))} accounts)"[:60],
            callback_data=f"campaign_view:{record['id']}",
        )] for record in records[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"suspended_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"suspended_page:{page + 1}"))
            rows.append(nav)
        rows.append([InlineKeyboardButton("Campaigns", callback_data="dash:section:campaigns"),
                     InlineKeyboardButton("Home", callback_data="dash:home")])
        text = ("Normal campaigns suspended by scheduled priority:" if records
                else "No normal campaigns are suspended by scheduled priority.")
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))

    def _get_campaign(self, campaign_id: str) -> dict | None:
        if not _RECORD_ID_PATTERN.fullmatch(campaign_id):
            return None
        campaign = self.campaign_repository.get(campaign_id)
        if campaign is None:
            return None
        account_ids = campaign.get("account_ids")
        if not account_ids and campaign.get("account_id"):
            account_ids = [campaign["account_id"]]
        if not isinstance(account_ids, list) or not account_ids or any(self._get_account(str(item)) is None for item in account_ids):
            return None
        target_ids = campaign.get("target_ids")
        if not isinstance(target_ids, list) or not target_ids:
            return None
        targets = [self._target_by_id(str(target_id)) for target_id in target_ids]
        if any(target is None or target.get("opted_in") is not True for target in targets):
            return None
        sticker = self._get_sticker(str(campaign.get("sticker_id", "")))
        if sticker is None:
            return None
        try:
            cycles = max(1, min(100000, int(campaign.get("total_cycles", 1))))
        except (ValueError, TypeError):
            return None
        progress = dict(campaign.get("progress") or {})
        progress.setdefault("completed", 0)
        progress.setdefault("successful", 0)
        progress.setdefault("failed", 0)
        progress.setdefault("restricted", 0)
        progress["total"] = cycles * len(account_ids) * len(target_ids)
        changes = {
            "account_ids": account_ids, "total_cycles": cycles,
            "rotation_index": int(campaign.get("rotation_index", 0)),
            "scheduling_enabled": campaign.get("scheduling_enabled") is True,
            "progress": progress,
        }
        if any(campaign.get(key) != value for key, value in changes.items()):
            campaign = self.campaign_repository.update(campaign_id, changes)
        return campaign
    async def _campaign_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if message is None:
            return
        if not context.args:
            await self._campaigns_command(update, context)
            return
        campaign = self._get_campaign(context.args[0])
        if campaign is None:
            await message.reply_text("Campaign not found or one of its selected records is no longer available.")
            return
        await self._show_campaign(message, campaign)

    async def _show_campaign(self, message_or_query, campaign: dict, edit: bool = False) -> None:
        accounts = [self._get_account(str(account_id)) for account_id in campaign["account_ids"]]
        account_names = [str(account.get("name") or account.get("phone", "unavailable")) for account in accounts if account]
        targets = [self._target_by_id(str(target_id)) for target_id in campaign["target_ids"]]
        target_names = [f"@{target['username']}" for target in targets if target]
        progress = campaign.get("progress") or {}
        total = max(0, int(progress.get("total", 0)))
        completed = max(0, int(progress.get("completed", 0)))
        restricted = int(progress.get("restricted", 0))
        if self.campaign_execution is not None:
            restricted = sum(
                int(task.get("restricted", 0))
                for task in self.campaign_execution.task_records(str(campaign["id"]))
            )
        percent = min(100, int(completed * 100 / total)) if total else 0
        bar = "#" * (percent // 5) + "-" * (20 - percent // 5)
        rotation = int(campaign.get("rotation_index", 0)) % len(PROMOTION_MESSAGES)
        suspended_lines = []
        suspended_buttons = []
        if self.campaign_execution is not None:
            task_records = self.campaign_execution.task_records(str(campaign["id"]))
            saved_checkpoints = campaign.get("normal_suspension_checkpoints") or {}
            for account_id in campaign.get("suspended_accounts", []):
                account_id = str(account_id)
                account = self._get_account(account_id) or {}
                account_label = str(account.get("name") or account.get("phone") or "Unknown account")
                account_tasks = saved_checkpoints.get(account_id) or [
                    task for task in task_records if str(task.get("account_id")) == account_id
                ]
                successful = sum(int(task.get("successful", 0)) for task in account_tasks)
                failed = sum(int(task.get("failed", 0)) for task in account_tasks)
                next_cycle = min((int(task.get("next_cycle", 1)) for task in account_tasks
                                  if task.get("status") != "restricted"), default=0)
                suspended_lines.append(
                    f"{account_label} ({account_id}): {successful} successful, {failed} failed; "
                    f"next cycle {next_cycle}"
                )
                token = secrets.token_hex(4)
                self._suspended_resume_tokens[token] = (str(campaign["id"]), account_id)
                suspended_buttons.append([InlineKeyboardButton(
                    f"Resume {account_label}"[:60], callback_data=f"resume_suspended:{token}",
                )])
        text = (
            f"Campaign: {campaign['name']}\nStatus: {campaign.get('status', 'draft')}\n"
            f"Accounts ({len(account_names)}): {', '.join(account_names)}\n"
            f"Targets ({len(target_names)}): {', '.join(target_names)}\n"
            f"Cycles per target: {campaign.get('total_cycles', 1)}\n"
            f"Sticker: {self._sticker_name(self._get_sticker(campaign.get('sticker_id')) or {})}\n"
            f"Next built-in text: {PROMOTION_MESSAGES[rotation]}\n"
            f"Progress: {completed}/{total} | {percent}%\n{bar}\n"
            f"Successful cycles: {int(progress.get('successful', 0))} | Failed cycles: {int(progress.get('failed', 0))} | Restricted cycles: {restricted}\n"
            f"Current operation: {campaign.get('current_operation', 'idle')}"
            + ("\n\nSuspended by scheduled priority:\n" + "\n".join(suspended_lines)
               if suspended_lines else "")
        )
        campaign_buttons = [
            [InlineKeyboardButton("Start / resume", callback_data=f"campaign_control:start:{campaign['id']}")],
            [InlineKeyboardButton("Pause / resume", callback_data=f"campaign_control:pause:{campaign['id']}")],
            [InlineKeyboardButton("Stop", callback_data=f"campaign_control:stop:{campaign['id']}")],
            [InlineKeyboardButton("Per-account / target progress", callback_data=f"campaign_progress:{campaign['id']}:0")],
            [InlineKeyboardButton("Set cycles per target", callback_data=f"campaign_config:cycles:{campaign['id']}")],
        ] + suspended_buttons + [
            [InlineKeyboardButton(
                "Disable scheduling" if campaign.get("scheduling_enabled") else "Enable scheduling",
                callback_data=f"campaign_schedule:{campaign['id']}",
            )],
            [InlineKeyboardButton("Edit name", callback_data=f"campaign_edit:{campaign['id']}")],
            [InlineKeyboardButton("Delete campaign", callback_data=f"campaign_delete:{campaign['id']}")],
        ]
        markup = self._dashboard_navigation(
            InlineKeyboardMarkup(campaign_buttons), self._chat_id(message_or_query), back_data="campaign_page:0",
        )
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    async def _show_campaign_progress(self, query, campaign: dict, page: int) -> None:
        if self.campaign_execution is None:
            await query.edit_message_text("Campaign progress is unavailable.")
            return
        tasks = self.campaign_execution.task_records(campaign["id"])
        if not tasks:
            tasks = [
                {"account_id": account_id, "target_id": target_id, "next_cycle": 1,
                 "successful": 0, "failed": 0, "phase": "idle"}
                for account_id in campaign["account_ids"] for target_id in campaign["target_ids"]
            ]
        tasks.sort(key=lambda item: (str(item.get("account_id", "")), str(item.get("target_id", ""))))
        pages = max(1, (len(tasks) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = max(0, min(page, pages - 1))
        lines = [f"Per-account and target progress ({len(tasks)} entries):"]
        for task in tasks[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]:
            account = self._get_account(str(task.get("account_id", ""))) or {}
            target = self._target_by_id(str(task.get("target_id", ""))) or {}
            phase = str(task.get("phase", "idle"))
            state = str(task.get("status") or ("in progress" if phase not in {"idle", "None"} else "pending"))
            reason = f"; reason: {task.get('restriction_reason')}" if state == "restricted" and task.get("restriction_reason") else ""
            lines.append(
                f"{account.get('name') or account.get('phone', 'Account unavailable')} -> @{target.get('username', 'target unavailable')}: "
                f"cycle {task.get('next_cycle', 1)}/{campaign.get('total_cycles', 1)}, "
                f"ok {task.get('successful', 0)}, failed {task.get('failed', 0)}, "
                f"restricted {task.get('restricted', 0)}, {state} ({phase}){reason}"
            )
        rows = []
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"campaign_progress:{campaign['id']}:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"campaign_progress:{campaign['id']}:{page + 1}"))
            rows.append(nav)
        rows.append([InlineKeyboardButton("Campaign", callback_data=f"campaign_view:{campaign['id']}"), InlineKeyboardButton("Home", callback_data="dash:home")])
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))

    async def _campaign_progress_callback(self, campaign: dict | None) -> None:
        if campaign is None or self._application is None:
            return
        chat_id = campaign.get("progress_chat_id")
        message_id = campaign.get("progress_message_id")
        if not isinstance(chat_id, int) or not isinstance(message_id, int):
            return
        status = str(campaign.get("status", "unknown"))
        now = asyncio.get_running_loop().time()
        last = self._campaign_last_edit.get(campaign["id"], 0.0)
        if status == "running" and now - last < 5.0:
            return
        progress = campaign.get("progress") or {}
        total = max(0, int(progress.get("total", 0)))
        completed = max(0, int(progress.get("completed", 0)))
        restricted = int(progress.get("restricted", 0))
        if self.campaign_execution is not None:
            restricted = sum(
                int(task.get("restricted", 0))
                for task in self.campaign_execution.task_records(str(campaign["id"]))
            )
        percent = min(100, int(completed * 100 / total)) if total else 0
        bar = "#" * (percent // 5) + "-" * (20 - percent // 5)
        account = self._get_account(str(campaign.get("current_account_id", ""))) or {}
        target = self._target_by_id(str(campaign.get("current_target_id", ""))) or {}
        text = (
            f"Campaign: {campaign.get('name', 'Campaign')}\nStatus: {status}\n"
            f"Progress: {completed}/{total} | {percent}%\n{bar}\n"
            f"Successful cycles: {int(progress.get('successful', 0))} | Failed cycles: {int(progress.get('failed', 0))} | Restricted cycles: {restricted}\n"
            f"Account: {account.get('name') or account.get('phone', 'waiting')} | Target: @{target.get('username', 'waiting')}\n"
            f"Current operation: {campaign.get('current_operation', 'idle')}"
        )
        control = "pause" if status == "running" else "start"
        rows = [
            [InlineKeyboardButton("Pause / resume", callback_data=f"campaign_control:{control}:{campaign['id']}")],
            [InlineKeyboardButton("Stop", callback_data=f"campaign_control:stop:{campaign['id']}")],
            [InlineKeyboardButton("Per-target progress", callback_data=f"campaign_progress:{campaign['id']}:0")],
            [InlineKeyboardButton("Campaign details", callback_data=f"campaign_view:{campaign['id']}")],
        ]
        try:
            await self._application.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text,
                reply_markup=InlineKeyboardMarkup(rows),
            )
            self._campaign_last_edit[campaign["id"]] = now
        except Exception as exc:
            logger.debug("Campaign progress message edit skipped (error=%s)", type(exc).__name__)
    async def _delete_campaign_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.effective_message is not None:
            await self._campaigns_command(update, context)

    async def _workflow_notice(self, query, text: str) -> None:
        await query.edit_message_text(text)

    async def _show_campaign_targets(self, message_or_query, workflow: Workflow, edit: bool = False) -> None:
        eligible = self._targets(opted_in_only=True)
        selected = set(workflow.data.get("target_ids", []))
        page_size = 8
        pages = max(1, (len(eligible) + page_size - 1) // page_size)
        page = max(0, min(int(workflow.data.get("target_page", 0)), pages - 1))
        workflow.data["target_page"] = page
        rows = [[InlineKeyboardButton(
            f"{'☑' if item['id'] in selected else '☐'} @{item['username']}"[:60],
            callback_data=f"campaign_target:{item['id']}",
        )] for item in eligible[page * page_size:(page + 1) * page_size]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"campaign_target_page:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"campaign_target_page:{page + 1}"))
            rows.append(nav)
        if selected:
            rows.append([InlineKeyboardButton("Continue", callback_data="campaign_targets_done")])
        text = "Select opted-in targets for this campaign. No messages are sent during campaign setup."
        markup = self._with_flow_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        if edit:
            await message_or_query.edit_message_text(text if eligible else "No opted-in targets remain. Cancel and add an opted-in target first.", reply_markup=markup)
        else:
            await message_or_query.reply_text(text if eligible else "No opted-in targets remain.", reply_markup=markup)

    def _lookup_action_record(self, kind: str, record_id: str) -> dict | None:
        if not _RECORD_ID_PATTERN.fullmatch(record_id):
            return None
        if kind == "target":
            item = self._target_by_id(record_id)
            if item is not None and any(record_id in campaign.get("target_ids", []) for campaign in self.campaign_repository.list()):
                return None
            return item
        if kind == "sticker":
            item = self._get_sticker(record_id)
            if item is not None and (
                any(record.get("sticker_id") == record_id for record in self.message_repository.list())
                or any(record.get("sticker_id") == record_id for record in self.campaign_repository.list())
            ):
                return None
            return item
        if kind == "message":
            item = self._get_message(record_id)
            if item is not None and any(record.get("message_id") == record_id for record in self.campaign_repository.list()):
                return None
            return item
        if kind == "campaign":
            campaign = self._get_campaign(record_id)
            return None if campaign is not None and campaign.get("status") == "running" else campaign
        return None

    async def _ask_action_confirmation(self, query, kind: str, record_id: str, record: dict) -> None:
        nonce = secrets.token_urlsafe(6)
        chat_id = query.message.chat_id if query.message is not None else 0
        self._action_confirmations[nonce] = ActionConfirmation(
            kind=kind,
            record_id=record_id,
            chat_id=chat_id,
            expires_at=datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
        )
        label = record.get("username") or record.get("name") or record_id[:8]
        markup = InlineKeyboardMarkup([[InlineKeyboardButton("Confirm removal", callback_data=f"action_yes:{nonce}"), InlineKeyboardButton("Cancel", callback_data=f"action_no:{nonce}")]])
        markup = self._with_flow_navigation(markup, chat_id)
        await query.edit_message_text(f"Remove {label}? This action cannot be undone.", reply_markup=markup)

    async def _handle_action_confirmation(self, query, action: str, nonce: str) -> None:
        confirmation = self._action_confirmations.pop(nonce, None)
        chat_id = query.message.chat_id if query.message is not None else 0
        if confirmation is None or confirmation.chat_id != chat_id or confirmation.expires_at <= datetime.now(timezone.utc):
            await query.edit_message_text("This confirmation expired or is no longer valid. Start the command again.")
            return
        if action == "action_no" or action == "content_cancel":
            key = (self.settings.admin_id, chat_id)
            workflow = self._workflows.get(key)
            if workflow is not None and workflow.data.get("confirmation") == nonce:
                self._workflows.pop(key, None)
            await query.edit_message_text("Cancelled. No record was saved or removed.", reply_markup=self._dashboard_navigation(None, chat_id))
            return
        if confirmation.kind == "restriction_clear":
            try:
                current = self.restriction_repository.get(confirmation.record_id)
                if current is None or current.get("status", "active") != "active":
                    await query.edit_message_text("This restriction is no longer active.")
                    return
                cleared = self.restriction_repository.clear(
                    confirmation.record_id,
                    cleared_at=datetime.now(timezone.utc).isoformat(),
                    cleared_by=self.settings.admin_id,
                )
                if cleared is None:
                    await query.edit_message_text("This restriction is no longer available.")
                    return
                if self.campaign_execution is not None:
                    self.campaign_execution.clear_detected_restriction(
                        str(cleared.get("account_id", "")), str(cleared.get("target_id", ""))
                    )
            except Exception as exc:
                logger.warning("Could not clear account restriction (error=%s)", type(exc).__name__)
                await query.edit_message_text("Could not clear this restriction. Its record was retained.")
                return
            await self._show_restrictions(query)
            return
        if action == "content_save":
            key = (self.settings.admin_id, chat_id)
            workflow = self._workflows.get(key)
            if workflow is None or workflow.data.get("confirmation") != nonce:
                await query.edit_message_text("This save confirmation is stale. Start the workflow again.")
                return
            now = datetime.now(timezone.utc).isoformat()
            if confirmation.kind == "save_message" and workflow.kind == "message":
                sticker = self._get_sticker(workflow.data.get("sticker_id"))
                text = workflow.data.get("text")
                if sticker is None or not isinstance(text, str) or not text:
                    await query.edit_message_text("Selected content is no longer available; nothing was saved.")
                else:
                    record = self.message_repository.create({
                        "id": uuid.uuid4().hex,
                        "sticker_id": sticker["id"],
                        "text": text,
                        "sequence": [{"type": "sticker", "sticker_id": sticker["id"]}, {"type": "text", "text": text}],
                        "created_at": now,
                        "updated_at": now,
                    })
                    await query.edit_message_text(f"Promotion message saved with exact Sticker → Text sequence. ID: {record['id']}", reply_markup=self._dashboard_navigation(None, chat_id, back_data="dash:section:messages"))
                self._workflows.pop(key, None)
                return
            if confirmation.kind == "update_message" and workflow.kind == "edit_message":
                record = self._get_message(confirmation.record_id)
                sticker = self._get_sticker(workflow.data.get("sticker_id"))
                text = workflow.data.get("text")
                referenced = any(
                    campaign.get("message_id") == confirmation.record_id
                    for campaign in self.campaign_repository.list()
                )
                if record is None or sticker is None or not isinstance(text, str) or not text or referenced:
                    await query.edit_message_text("The message changed, is unavailable, or is now used by a campaign. Existing content was kept.", reply_markup=self._dashboard_navigation(None, chat_id, back_data="dash:section:messages"))
                else:
                    self.message_repository.update(confirmation.record_id, {
                        "sticker_id": sticker["id"],
                        "text": text,
                        "sequence": [{"type": "sticker", "sticker_id": sticker["id"]}, {"type": "text", "text": text}],
                        "updated_at": now,
                    })
                    await query.edit_message_text("Promotion message updated after preview.", reply_markup=self._dashboard_navigation(None, chat_id, back_data="dash:section:messages"))
                self._workflows.pop(key, None)
                return
            if confirmation.kind == "save_campaign" and workflow.kind == "campaign":
                account_ids = list(dict.fromkeys(str(item) for item in workflow.data.get("account_ids", [])))
                accounts = [self._get_account(account_id) for account_id in account_ids]
                targets = [self._target_by_id(str(target_id)) for target_id in workflow.data.get("target_ids", [])]
                sticker = self._get_sticker(workflow.data.get("sticker_id"))
                cycles = int(workflow.data.get("total_cycles", 0))
                if (not accounts or any(item is None for item in accounts) or not targets
                        or any(item is None or item.get("opted_in") is not True for item in targets)
                        or sticker is None or cycles < 1 or cycles > 100000):
                    await query.edit_message_text("Campaign selections changed or are unavailable. Nothing was saved or started.")
                else:
                    record_id = uuid.uuid4().hex
                    total = cycles * len(account_ids) * len(targets)
                    record = self.campaign_repository.create({
                        "id": record_id,
                        "name": workflow.data["name"],
                        "account_ids": account_ids,
                        "target_ids": [item["id"] for item in targets],
                        "sticker_id": sticker["id"],
                        "total_cycles": cycles,
                        "rotation_index": 0,
                        "scheduling_enabled": False,
                        "status": "draft",
                        "progress": {"completed": 0, "successful": 0, "failed": 0, "total": total},
                        "current_operation": "idle",
                        "execution_state": "not_started",
                        "pause_requested": False,
                        "stop_requested": False,
                        "progress_chat_id": chat_id,
                        "progress_message_id": query.message.message_id,
                        "created_at": now,
                        "updated_at": now,
                    })
                    self._workflows.pop(key, None)
                    if self.campaign_execution is None or not await self.campaign_execution.start(record["id"], self._campaign_progress_callback):
                        self.campaign_repository.update(record["id"], {"status": "failed", "execution_state": "failed", "current_operation": "start failed"})
                        await query.edit_message_text(f"Campaign saved but could not start. ID: {record['id']}", reply_markup=self._dashboard_navigation(None, chat_id, back_data="dash:section:campaigns"))
                    else:
                        await self._campaign_progress_callback(self.campaign_repository.get(record["id"]))
                self._workflows.pop(key, None)
                return
            self._workflows.pop(key, None)
            await query.edit_message_text("This save confirmation is stale. Nothing was saved.")
            return

        record = self._lookup_action_record(confirmation.kind, confirmation.record_id)
        if record is None:
            await query.edit_message_text("This item is unavailable, referenced by another record, or already removed.")
            return
        try:
            if confirmation.kind == "target":
                self.target_repository.delete(confirmation.record_id)
            elif confirmation.kind == "message":
                self.message_repository.delete(confirmation.record_id)
            elif confirmation.kind == "campaign":
                self.campaign_repository.delete(confirmation.record_id)
            elif confirmation.kind == "sticker":
                self.sticker_file_manager.delete(record["file_path"], missing_ok=True)
                self.sticker_repository.delete(confirmation.record_id)
        except Exception as exc:
            logger.warning("Could not remove %s record (error=%s)", confirmation.kind, type(exc).__name__)
            await query.edit_message_text("Removal failed; the record was retained where possible.")
            return
        await query.edit_message_text(f"{confirmation.kind.capitalize()} removed.", reply_markup=self._dashboard_navigation(None, chat_id))

    async def _expire_authentication_flows(self) -> None:
        """Periodically clean up abandoned flows and their temporary sessions."""

        try:
            while True:
                await asyncio.sleep(30)
                async with self._flow_lock:
                    now = datetime.now(timezone.utc)
                    expired = [
                        (key, flow)
                        for key, flow in self._auth_flows.items()
                        if flow.expires_at <= now
                    ]
                    for key, flow in expired:
                        self._auth_flows.pop(key, None)
                        if flow.auth_token:
                            await self.auth_service.cancel(flow.auth_token)
                        if self._active_flow_key == key:
                            self._active_flow_key = None
                        if self._application is not None:
                            try:
                                await self._application.bot.send_message(
                                    chat_id=key[1],
                                    text="Account authentication expired. Start again with /addaccount.",
                                )
                            except Exception as exc:
                                logger.info("Could not send authentication expiry notice (error=%s)", type(exc).__name__)
                    expired_confirmations = [
                        nonce
                        for nonce, confirmation in self._removal_confirmations.items()
                        if confirmation.expires_at <= now
                    ]
                    for nonce in expired_confirmations:
                        self._removal_confirmations.pop(nonce, None)
                    for key, workflow in list(self._workflows.items()):
                        if workflow.expires_at <= now:
                            self._workflows.pop(key, None)
                            self._discard_pending_sticker_upload(workflow)
                            if self._application is not None:
                                try:
                                    await self._application.bot.send_message(
                                        chat_id=key[1],
                                        text="The active workflow expired. Start it again with a command.",
                                    )
                                except Exception as exc:
                                    logger.info("Could not send workflow expiry notice (error=%s)", type(exc).__name__)
                    for nonce, confirmation in list(self._action_confirmations.items()):
                        if confirmation.expires_at <= now:
                            self._action_confirmations.pop(nonce, None)
        except asyncio.CancelledError:
            raise
