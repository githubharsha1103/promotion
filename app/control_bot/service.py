"""Owner-only Telegram Bot API control and account enrollment interface."""
import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import re
import secrets
import time
import uuid
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Conflict
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
from app.storage.json_store import JsonStoreError
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
_MAX_DEFERRED_CONTROL_OPERATIONS = 4
_POLL_HEALTH_INTERVAL_SECONDS = 10.0
_TELEGRAM_PROBE_INTERVAL_SECONDS = 60.0
_TELEGRAM_PROBE_TIMEOUT_SECONDS = 10.0


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
        self._shutdown_requested = False
        self._startup_task: asyncio.Task[None] | None = None
        self._expiry_task: asyncio.Task[None] | None = None
        self._background_control_tasks: set[asyncio.Task] = set()
        self._background_control_keys: set[str] = set()
        self._auth_flows: dict[tuple[int, int], BotAuthenticationFlow] = {}
        self._active_flow_key: tuple[int, int] | None = None
        self._removal_confirmations: dict[str, RemovalConfirmation] = {}
        self._workflows: dict[tuple[int, int], Workflow] = {}
        self._action_confirmations: dict[str, ActionConfirmation] = {}
        self._dashboard_sections: dict[int, str] = {}
        self._flow_lock = asyncio.Lock()
        self._campaign_last_edit: dict[str, float] = {}
        self._suspended_resume_tokens: dict[str, tuple[str, str]] = {}
        self._last_successful_update_at: str | None = None
        self._last_polling_error: str | None = None
        self._polling_restart_count = 0
        self._background_failure_count = 0
        self._last_background_failure_at: str | None = None
        self._failed_update_ids: set[int] = set()
        self._last_event_loop_lag_seconds = 0.0
        self._process_cpu_percent = 0.0
        self._process_memory_bytes: int | None = None
        self._last_cpu_time = time.process_time()
        self._last_cpu_sample = time.monotonic()
        self._telegram_connected: bool | None = None
        self._consecutive_probe_failures = 0
        self._polling_failure_event = asyncio.Event()
        self._control_loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> bool:
        """Start one polling supervisor; temporary API failures are retried in the background."""

        token = self.settings.control_bot_token.get_secret_value()
        if not token or self.settings.admin_id is None:
            logger.info("Control bot disabled: CONTROL_BOT_TOKEN or ADMIN_ID is not configured")
            return False

        if self._polling or (
            self._startup_task is not None and not self._startup_task.done()
        ):
            return True
        self._shutdown_requested = False
        self._control_loop = asyncio.get_running_loop()
        self._startup_task = asyncio.create_task(
            self._start_with_retry(token), name="control-bot-startup",
        )
        return True

    def _build_application(self, token: str) -> Application:
        application = Application.builder().token(token).concurrent_updates(4).build()
        application.bot_data["control_bot_service"] = self
        # This handler runs first and stops processing for unauthorized updates.
        application.add_handler(TypeHandler(Update, self._authorize_update), group=-100)
        application.add_handler(TypeHandler(Update, self._record_processed_update), group=100)
        application.add_error_handler(self._record_handler_error)
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
        return application

    async def _start_with_retry(self, token: str) -> None:
        attempt = 0
        delay = 1.0
        while not self._shutdown_requested:
            started_at = time.monotonic()
            try:
                self._polling_failure_event.clear()
                application = self._build_application(token)
                self._application = application
                await application.initialize()
                self._initialized = True
                await application.start()
                self._started = True
                if application.updater is None:
                    raise RuntimeError("Polling updater is unavailable")
                await application.updater.start_polling(
                    allowed_updates=Update.ALL_TYPES,
                    error_callback=self._polling_error_callback,
                )
                self._polling = True
                self._expiry_task = asyncio.create_task(self._expire_authentication_flows())
                logger.info("Control bot polling started")
                await self._monitor_polling(application)
                if self._shutdown_requested:
                    return
                raise RuntimeError("Control bot polling stopped unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                self._polling_restart_count += 1
                self._last_polling_error = type(exc).__name__
                logger.warning(
                    "Control bot polling attempt %d failed; retrying (error=%s)",
                    attempt, type(exc).__name__,
                )
                await self._notify_polling_recovery(exc)
                if not await self._close_application():
                    logger.critical(
                        "Control bot polling shutdown was not confirmed; refusing to create another poller",
                    )
                    self._shutdown_requested = True
                    return
                if self._shutdown_requested:
                    return
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    raise
                if time.monotonic() - started_at >= 300:
                    attempt = 0
                    delay = 1.0
                delay = min(60.0, delay * 2)

    async def _monitor_polling(self, application: Application) -> None:
        loop = asyncio.get_running_loop()
        next_probe = loop.time()
        while not self._shutdown_requested:
            expected = loop.time() + _POLL_HEALTH_INTERVAL_SECONDS
            await asyncio.sleep(_POLL_HEALTH_INTERVAL_SECONDS)
            now = loop.time()
            self._last_event_loop_lag_seconds = max(0.0, now - expected)
            self._sample_process_metrics()
            updater = application.updater
            if updater is None or not updater.running:
                raise RuntimeError("Telegram polling updater is not running")
            if self._polling_failure_event.is_set():
                raise RuntimeError("Telegram polling conflict detected")
            if now < next_probe:
                continue
            try:
                await asyncio.wait_for(
                    application.bot.get_me(),
                    timeout=_TELEGRAM_PROBE_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._consecutive_probe_failures += 1
                self._telegram_connected = False
                logger.warning(
                    "Control bot connectivity probe failed (%d consecutive; error=%s)",
                    self._consecutive_probe_failures, type(exc).__name__,
                )
                if self._consecutive_probe_failures >= 3:
                    raise RuntimeError("Telegram Bot API health probes failed") from exc
            else:
                self._telegram_connected = True
                self._consecutive_probe_failures = 0
            next_probe = now + _TELEGRAM_PROBE_INTERVAL_SECONDS

    def _polling_error_callback(self, error: Exception) -> None:
        self._last_polling_error = type(error).__name__
        if isinstance(error, Conflict) and self._control_loop is not None:
            self._control_loop.call_soon_threadsafe(self._polling_failure_event.set)

    def _sample_process_metrics(self) -> None:
        now = time.monotonic()
        cpu_time = time.process_time()
        elapsed = now - self._last_cpu_sample
        if elapsed > 0:
            self._process_cpu_percent = max(
                0.0, (cpu_time - self._last_cpu_time) / elapsed * 100.0,
            )
        self._last_cpu_time = cpu_time
        self._last_cpu_sample = now
        try:
            statm = Path("/proc/self/statm").read_text(encoding="ascii").split()
            self._process_memory_bytes = int(statm[1]) * int(os.sysconf("SC_PAGE_SIZE"))
        except (OSError, ValueError, IndexError, AttributeError):
            self._process_memory_bytes = None

    def health_snapshot(self) -> dict:
        application = self._application
        updater = application.updater if application is not None else None
        polling = bool(self._polling and updater is not None and updater.running)
        state = "running" if polling else (
            "restarting" if self._startup_task is not None and not self._startup_task.done()
            else "stopped"
        )
        return {
            "state": state,
            "polling": polling,
            "telegram_connected": self._telegram_connected,
            "last_successful_update_at": self._last_successful_update_at,
            "last_polling_error": self._last_polling_error,
            "polling_restart_count": self._polling_restart_count,
            "event_loop_lag_seconds": round(self._last_event_loop_lag_seconds, 3),
            "process_cpu_percent": round(self._process_cpu_percent, 2),
            "process_memory_bytes": self._process_memory_bytes,
            "background_control_tasks": len(self._background_control_tasks),
            "background_failure_count": self._background_failure_count,
            "last_background_failure_at": self._last_background_failure_at,
        }

    async def _notify_polling_recovery(self, error: Exception) -> None:
        application = self._application
        if application is None or self.settings.admin_id is None:
            return
        try:
            await asyncio.wait_for(
                application.bot.send_message(
                    chat_id=self.settings.admin_id,
                    text=(
                        "Control-bot polling is recovering. Existing campaign state is preserved. "
                        f"Failure: {type(error).__name__}; retry is backoff-limited."
                    ),
                ),
                timeout=5.0,
            )
        except Exception as notify_error:
            logger.info(
                "Could not notify owner about control-bot recovery (error=%s)",
                type(notify_error).__name__,
            )

    async def _record_processed_update(
        self, update: Update, _context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        if update.update_id not in self._failed_update_ids:
            self._last_successful_update_at = datetime.now(timezone.utc).isoformat()
        self._failed_update_ids.discard(update.update_id)

    async def _record_handler_error(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        error = context.error
        self._background_failure_count += 1
        self._last_background_failure_at = datetime.now(timezone.utc).isoformat()
        self._failed_update_ids.add(update.update_id)
        if len(self._failed_update_ids) > 128:
            self._failed_update_ids.pop()
        logger.error(
            "Control bot update handler failed (error=%s)",
            type(error).__name__ if error is not None else "unknown",
        )

    async def _close_application(self) -> bool:
        application = self._application
        expiry_task = self._expiry_task
        if expiry_task is not None:
            self._expiry_task = None
            if not expiry_task.done():
                expiry_task.cancel()
            await asyncio.gather(expiry_task, return_exceptions=True)
        if application is None:
            return True
        updater = application.updater
        if updater is not None and (self._polling or updater.running):
            try:
                await asyncio.wait_for(updater.stop(), timeout=20.0)
            except Exception as exc:
                logger.warning("Control bot polling shutdown failed (error=%s)", type(exc).__name__)
        polling_stopped = updater is None or not updater.running
        self._polling = not polling_stopped

        if self._started or application.running:
            try:
                await asyncio.wait_for(application.stop(), timeout=20.0)
            except Exception as exc:
                logger.warning("Control bot application shutdown failed (error=%s)", type(exc).__name__)
        self._started = False
        try:
            await asyncio.wait_for(application.shutdown(), timeout=20.0)
        except Exception as exc:
            logger.warning("Control bot resource cleanup failed (error=%s)", type(exc).__name__)
        self._initialized = False
        if polling_stopped:
            self._application = None
        return polling_stopped

    async def stop(self) -> None:
        """Stop startup/polling and discard pending account authentication flows."""

        self._shutdown_requested = True
        startup_task = self._startup_task
        if startup_task is not None and not startup_task.done():
            startup_task.cancel()
            try:
                await startup_task
            except asyncio.CancelledError:
                pass
        self._startup_task = None
        background_tasks = list(self._background_control_tasks)
        if background_tasks:
            _, pending = await asyncio.wait(background_tasks, timeout=5.0)
            for task in pending:
                task.cancel()
            await asyncio.gather(*background_tasks, return_exceptions=True)
        await self._close_application()

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
        account_label = str(account.get("name") or account.get("phone") or "Unnamed account")
        detected_at = self._format_dashboard_datetime(
            restriction.get("detected_at") or restriction.get("created_at")
        )
        text = (
            "🚫 Promotion restriction detected\n"
            f"Account: {account_label}\n"
            f"Target: @{str(restriction.get('target_username') or 'unknown').lstrip('@')}\n"
            f"Reason: {restriction.get('detection_reason', 'Access denied')}\n"
            f"Status: {str(restriction.get('status') or 'active').title()}"
            + (f"\nDetected: {detected_at}" if detected_at else "")
        )
        await self._application.bot.send_message(chat_id=self.settings.admin_id, text=text)

    async def notify_normal_campaign_suspended(self, detail: dict) -> None:
        """Notify the owner after scheduled priority safely suspends a normal account worker."""
        if self._application is None or self.settings.admin_id is None:
            return
        account_id = str(detail.get("account_id", ""))
        campaign = detail.get("campaign") or {}
        account = self.account_repository.get(account_id) or {}
        label = str(account.get("name") or account.get("phone") or "Unnamed account")
        progress = detail.get("progress") or {}
        text = (
            "⏸️ Normal campaign paused for scheduled promotion\n"
            f"Account: {label}\n"
            f"Campaign: {campaign.get('name', 'Campaign')}\n"
            "Reason: Scheduled campaign requires this account.\n"
            f"Completed cycles saved: {progress.get('successful', 0)}.\n"
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
            account_id = str(item.get("account_id") or "")
            account = self.account_repository.get(account_id) or {}
            account_name = str(account.get("name") or account.get("phone") or "Unnamed account")
            lines.append(
                f"• {item.get('campaign_name', 'Promotion')} · {account_name}\n"
                f"  Completed cycles saved: {progress.get('successful', 0)}"
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

    @staticmethod
    def _format_dashboard_time(value: str) -> str:
        hour, minute = (int(part) for part in str(value).split(":"))
        return datetime(2000, 1, 1, hour, minute).strftime("%I:%M %p").lstrip("0")

    @staticmethod
    def _format_dashboard_datetime(value: str | None) -> str | None:
        if not value:
            return None
        try:
            stamp = datetime.fromisoformat(str(value))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            local = stamp.astimezone(ZoneInfo("Asia/Kolkata"))
        except (TypeError, ValueError):
            return None
        return f"{local.strftime('%d %b %Y')} · {local.strftime('%I:%M %p').lstrip('0')} IST"

    @staticmethod
    async def _edit_dashboard_message(query, text: str, reply_markup=None) -> None:
        try:
            await query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as exc:
            if "message is not modified" not in str(exc).casefold():
                raise

    async def _show_promotion_dashboard(self, query, page: int = 0, *, refresh: bool = False) -> None:
        if self.scheduler_service is None or query.message is None:
            await self._edit_dashboard_message(query, "Promotion scheduling is unavailable.")
            return
        data = (await self.scheduler_service.refresh_promotion_dashboard_data(page)
                if refresh else await self.scheduler_service.promotion_dashboard_data(page))
        if not data.get("configured"):
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 Configure Promotion", callback_data="promo:configure")],
                [InlineKeyboardButton("⚙️ Settings", callback_data="promo:settings")],
                [InlineKeyboardButton("🔄 Refresh Dashboard", callback_data="promo:refresh")],
                [InlineKeyboardButton("⬅️ Back", callback_data="dash:home"),
                 InlineKeyboardButton("🏠 Home", callback_data="dash:home")],
            ])
            await self._edit_dashboard_message(
                query,
                "🚀 Promotion Dashboard\n\nNo promotion is configured yet.",
                reply_markup=markup,
            )
            return
        status = data["status"]
        settings = status["settings"]
        target_usernames = data.get("target_usernames") or [data.get("target_username") or "unavailable"]
        target_username = ", @".join(str(name).lstrip("@") for name in target_usernames)
        if status.get("recovery_required"):
            state = "Awaiting recovery confirmation"
        elif status.get("manually_paused"):
            state = "Paused"
        elif not status.get("enabled"):
            state = "Stopped"
        elif data.get("running_accounts"):
            state = "Running"
        elif status.get("operating_period") == "inactive" and status.get("enabled"):
            state = "Waiting for operating hours"
        else:
            state = "Waiting"
        period_names = {
            "peak": "Peak",
            "outside_peak": "Off-Peak",
            "inactive": "Outside Operating Hours",
        }
        period = period_names.get(str(status.get("operating_period")), "Outside Operating Hours")
        lines = [
            f"🚀 {data['promotion'].get('name', 'Promotion')}",
            f"🤖 Targets ({len(target_usernames)}): @{target_username}",
            f"🔀 Target order: {data['promotion'].get('execution_mode', 'sequential')}",
            f"⚙️ Status: {state}",
            f"🕒 Period: {period}",
            f"🔁 Cycles: {data['window_completed_cycles']} this window · "
            f"{data['completed_cycles']} lifetime",
            f"👥 Accounts: {data['account_count']} total · {data['eligible_total']} eligible",
            f"🟢 Running {data['running_accounts']} · 🟡 Waiting {data['idle_accounts']}",
            f"🚫 Restricted {data['restricted_accounts']} · ⏳ Rate limited {data['rate_limited_accounts']}",
            f"🔴 Unavailable {data['unavailable_accounts']}",
            f"🕰 Hours: {self._format_dashboard_time(settings['operating_start'])}–"
            f"{self._format_dashboard_time(settings['operating_end'])} IST",
            f"🌟 Peak: {self._format_dashboard_time(settings['peak_start'])}–"
            f"{self._format_dashboard_time(settings['peak_end'])} IST",
            "⚙️ Limits: peak "
            f"{settings['peak_concurrency']} · off-peak "
            f"{settings['outside_peak_concurrency_1_20']}/"
            f"{settings['outside_peak_concurrency_21_50']}/"
            f"{settings['outside_peak_concurrency_51_plus']} (by account tier) · "
            f"parallel targets {settings['parallel_target_concurrency']}",
        ]
        buttons = []
        if status.get("recovery_required"):
            buttons.append([InlineKeyboardButton("Recovery status", callback_data="dash:action:settings:scheduler_recovery")])
        elif status.get("manually_paused"):
            buttons.append([InlineKeyboardButton("▶️ Resume Promotion", callback_data="promo:resume")])
        elif status.get("enabled"):
            buttons.append([InlineKeyboardButton("⏸️ Pause Promotion", callback_data="promo:pause")])
        else:
            buttons.append([InlineKeyboardButton("▶️ Start Promotion", callback_data="promo:start")])
        buttons.extend([
            [InlineKeyboardButton("👥 Account Activity", callback_data="promo:activity:0"),
             InlineKeyboardButton("🚫 Restrictions", callback_data="promo:restrictions:0")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="promo:settings"),
             InlineKeyboardButton("📊 Reports", callback_data="promo:reports")],
            [InlineKeyboardButton("📝 Configure Promotion", callback_data="promo:configure")],
            [InlineKeyboardButton("🔄 Refresh Dashboard", callback_data="promo:refresh")],
            [InlineKeyboardButton("⬅️ Back", callback_data="dash:home"),
             InlineKeyboardButton("🏠 Home", callback_data="dash:home")],
        ])
        if not data.get("promotion_content_available"):
            lines.append("\nPromotional content is unavailable; configure text and/or a sticker for every target.")
        await self._edit_dashboard_message(
            query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons),
        )

    def _promotion_settings_keyboard(self) -> InlineKeyboardMarkup:
        settings = asdict(self.scheduler_service.settings)
        button_labels = {
            "operating_start": "Operating start",
            "operating_end": "Operating end",
            "peak_start": "Peak start",
            "peak_end": "Peak end",
            "peak_concurrency": "Peak limit",
            "outside_peak_concurrency_1_20": "Off-peak · 1–20",
            "outside_peak_concurrency_21_50": "Off-peak · 21–50",
            "outside_peak_concurrency_51_plus": "Off-peak · 51+",
            "parallel_target_concurrency": "Parallel target limit",
            "progress_report_time": "Progress report",
            "final_report_time": "Final report",
        }
        rows = []
        setting_buttons = []
        for key in button_labels:
            value = settings[key]
            label_value = (
                self._format_dashboard_time(value) if key.endswith("_start")
                or key.endswith("_end") or key.endswith("_time") else str(value)
            )
            setting_buttons.append(InlineKeyboardButton(
                f"{button_labels[key]} · {label_value}",
                callback_data=f"promo:setting:{key}",
            ))
        for index in range(0, len(setting_buttons), 2):
            rows.append(setting_buttons[index:index + 2])
        rows.extend([
            [InlineKeyboardButton("♻️ Restore default settings", callback_data="promo:settings:restore")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="promo:settings"),
             InlineKeyboardButton("⬅️ Back", callback_data="promo:dashboard"),
             InlineKeyboardButton("🏠 Home", callback_data="dash:home")],
        ])
        return InlineKeyboardMarkup(rows)

    async def _show_promotion_settings(self, query) -> None:
        if self.scheduler_service is None:
            await self._edit_dashboard_message(query, "Scheduler settings are unavailable.")
            return
        settings = self.scheduler_service.settings
        text = (
            "⚙️ Promotion Settings\n"
            f"🕰 Operating: {self._format_dashboard_time(settings.operating_start)}–"
            f"{self._format_dashboard_time(settings.operating_end)} IST\n"
            f"🌟 Peak: {self._format_dashboard_time(settings.peak_start)}–"
            f"{self._format_dashboard_time(settings.peak_end)} IST\n"
            f"🔢 Peak account limit: {settings.peak_concurrency}\n"
            "🔢 Off-peak limits by account tier: "
            f"{settings.outside_peak_concurrency_1_20} / "
            f"{settings.outside_peak_concurrency_21_50} / "
            f"{settings.outside_peak_concurrency_51_plus}\n"
            f"🔀 Parallel target limit: {settings.parallel_target_concurrency}\n"
            f"📨 Daily reports: {self._format_dashboard_time(settings.progress_report_time)} "
            "progress · "
            f"{self._format_dashboard_time(settings.final_report_time)} final (IST)\n\n"
            "Select a value to update it. Telegram-imposed waits and account restrictions remain enforced."
        )
        await self._edit_dashboard_message(
            query, text, reply_markup=self._promotion_settings_keyboard(),
        )

    async def _show_promotion_activity(self, query, page: int = 0, *, refresh: bool = False) -> None:
        data = (await self.scheduler_service.refresh_promotion_dashboard_data(page)
                if refresh else await self.scheduler_service.promotion_dashboard_data(page))
        accounts = data.get("accounts", [])
        page_count = max(1, data.get("account_pages", 1))
        page = min(max(0, page), page_count - 1)
        indicators = {
            "Running": "🟢 Running",
            "Waiting": "🟡 Idle / Waiting",
            "Paused": "⏸️ Paused",
            "Restricted": "🚫 Restricted",
            "Rate Limited": "⏳ Rate Limited",
            "Unavailable": "🔴 Offline",
        }
        lines = ["👥 Account Activity"]
        for row in accounts:
            lines.append(f"\n{row['account_name']} · {indicators.get(row['status'], row['status'])}")
            lines.append(
                f"Cycles: {row['window_completed_cycles']} this window · {row['completed_cycles']} lifetime"
            )
            last_activity = self._format_dashboard_datetime(row.get("last_activity"))
            if last_activity:
                lines.append(f"Last activity: {last_activity}")
            reason = None
            if row["status"] == "Rate Limited" and row.get("rate_limit_seconds") is not None:
                reason = f"Telegram wait remaining: {self._format_duration(row['rate_limit_seconds'])}"
            elif row["status"] == "Paused":
                reason = "Promotion paused by owner"
            elif row["status"] == "Waiting":
                current_status = data.get("status", {})
                if current_status.get("recovery_required"):
                    reason = "Awaiting recovery confirmation"
                elif not current_status.get("enabled"):
                    reason = "Promotion is stopped"
                elif current_status.get("operating_period") == "inactive":
                    reason = "Outside operating hours"
                else:
                    reason = row.get("last_error")
            elif row["status"] in {"Unavailable", "Restricted"}:
                reason = row.get("last_error")
            if reason:
                lines.append(f"Reason: {reason}")
        if not accounts:
            lines.append("\nNo accounts are available for this promotion.")
        rows = []
        if page_count > 1:
            nav = []
            if page:
                nav.append(InlineKeyboardButton("◀️ Previous", callback_data=f"promo:activity:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{page_count}", callback_data="noop"))
            if page + 1 < page_count:
                nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"promo:activity:{page + 1}"))
            rows.append(nav)
        rows.append([
            InlineKeyboardButton("🔄 Refresh", callback_data=f"promo:activity_refresh:{page}"),
            InlineKeyboardButton("⬅️ Back", callback_data="promo:dashboard"),
            InlineKeyboardButton("🏠 Home", callback_data="dash:home"),
        ])
        await self._edit_dashboard_message(
            query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _show_promotion_restrictions(self, query, page: int = 0) -> None:
        try:
            records = sorted(
                self.restriction_repository.list(),
                key=lambda item: str(item.get("detected_at", "")),
                reverse=True,
            )
        except Exception as exc:
            logger.warning("Could not load promotion restriction records (error=%s)", type(exc).__name__)
            await self._edit_dashboard_message(
                query,
                "Could not load restrictions.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Refresh", callback_data="promo:restrictions:0"),
                    InlineKeyboardButton("⬅️ Back", callback_data="promo:dashboard"),
                    InlineKeyboardButton("🏠 Home", callback_data="dash:home"),
                ]]),
            )
            return
        page_size = 5
        page_count = max(1, (len(records) + page_size - 1) // page_size)
        page = min(max(0, int(page)), page_count - 1)
        selected = records[page * page_size:(page + 1) * page_size]
        lines = ["🚫 Promotion Restrictions"]
        for record in selected:
            account_name = self._restriction_account_label(str(record.get("account_id", "")))
            target_name = str(record.get("target_username") or "unknown").lstrip("@")
            lines.append(f"\n{account_name} → @{target_name}")
            reason = record.get("detection_reason")
            if reason:
                lines.append(f"Reason: {reason}")
            lines.append(f"Status: {str(record.get('status') or 'active').title()}")
            detected = self._format_dashboard_datetime(
                record.get("detected_at") or record.get("created_at")
            )
            if detected:
                lines.append(f"Detected: {detected}")
        if not selected:
            lines.append("\nNo promotion restrictions are recorded.")
        rows = []
        if page_count > 1:
            nav = []
            if page:
                nav.append(InlineKeyboardButton("◀️ Previous", callback_data=f"promo:restrictions:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{page_count}", callback_data="noop"))
            if page + 1 < page_count:
                nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"promo:restrictions:{page + 1}"))
            rows.append(nav)
        rows.append([
            InlineKeyboardButton("🔄 Refresh", callback_data=f"promo:restrictions:{page}"),
            InlineKeyboardButton("⬅️ Back", callback_data="promo:dashboard"),
            InlineKeyboardButton("🏠 Home", callback_data="dash:home"),
        ])
        await self._edit_dashboard_message(
            query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _show_promotion_reports(self, query) -> None:
        buttons = [
            [InlineKeyboardButton("📊 Current operating window", callback_data="promo:report:current")],
            [InlineKeyboardButton("🕘 Latest progress report", callback_data="promo:report:progress")],
            [InlineKeyboardButton("🌙 Latest final report", callback_data="promo:report:final")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="promo:reports")],
            [InlineKeyboardButton("⬅️ Back", callback_data="promo:dashboard"),
             InlineKeyboardButton("🏠 Home", callback_data="dash:home")],
        ]
        await self._edit_dashboard_message(
            query, "📊 Promotion Reports\nChoose a report to view.",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    async def _show_promotion_report(self, query, kind: str) -> None:
        if self.reporting_service is None or self.scheduler_service is None:
            await self._edit_dashboard_message(query, "Scheduled reporting is unavailable.")
            return
        if kind == "current":
            settings = self.scheduler_service.settings
            report = await asyncio.to_thread(
                self.reporting_service.build_current_report,
                scheduler_enabled=self.scheduler_service.enabled,
                operating_start=settings.operating_start,
                operating_end=settings.operating_end,
                scheduler_status=self.scheduler_service.state.value,
            )
        else:
            report = await asyncio.to_thread(self.reporting_service.latest, kind)
        if report is None:
            await self._edit_dashboard_message(
                query, "No saved report is available yet.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ Back", callback_data="promo:reports"),
                    InlineKeyboardButton("🏠 Home", callback_data="dash:home"),
                ]]),
            )
            return
        chunks = self.reporting_service.split_text(self.reporting_service.format_report(report))
        await self._edit_dashboard_message(
            query, chunks[0], reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⬅️ Back", callback_data="promo:reports"),
                InlineKeyboardButton("🏠 Home", callback_data="dash:home"),
            ]]),
        )
        for chunk in chunks[1:]:
            await self._application.bot.send_message(
                chat_id=query.message.chat_id, text=chunk,
            )

    async def _show_promotion_targets(self, message_or_query, page: int = 0, edit: bool = False) -> None:
        targets = self._targets(opted_in_only=True)
        key = (self.settings.admin_id, self._chat_id(message_or_query))
        workflow = self._workflows.get(key)
        selected = set(workflow.data.get("target_ids", [])) if workflow and workflow.kind == "promotion_config" else set()
        pages = max(1, (len(targets) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = min(max(0, int(page)), pages - 1)
        rows = [[InlineKeyboardButton(
            f"{'☑' if item['id'] in selected else '☐'} @{item['username']}"[:60],
            callback_data=f"promo:target:{item['id']}"
        )] for item in targets[page * _PAGE_SIZE:(page + 1) * _PAGE_SIZE]]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("Previous", callback_data=f"promo:targets:{page - 1}"))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton("Next", callback_data=f"promo:targets:{page + 1}"))
            rows.append(nav)
        if selected:
            rows.append([InlineKeyboardButton("Continue", callback_data="promo:targets_done")])
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
            "reports": [("Latest progress report", "progress"),
                        ("Latest final report", "final"),
                        ("Current promotion report", "current")],
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
        if destination == "back" and workflow is not None and workflow.kind == "promotion_setting":
            await self._show_promotion_settings(query)
        elif destination == "back" and chat_id in self._dashboard_sections:
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
        return str(account.get("name") or account.get("phone") or "Unnamed account")

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
                scheduler_settings = (
                    self.scheduler_service.settings if self.scheduler_service is not None else None
                )
                report = self.reporting_service.build_current_report(
                    scheduler_enabled=bool(self.scheduler_service and self.scheduler_service.enabled),
                    operating_start=(
                        scheduler_settings.operating_start if scheduler_settings else "15:00"
                    ),
                    operating_end=(
                        scheduler_settings.operating_end if scheduler_settings else "03:00"
                    ),
                    scheduler_status=(
                        self.scheduler_service.state.value if self.scheduler_service else None
                    ),
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
            account_names = lambda ids: ", ".join(
                self._restriction_account_label(str(account_id)) for account_id in ids
            ) or "none"
            period = {
                "peak": "Peak", "outside_peak": "Off-Peak", "inactive": "Outside Operating Hours",
            }.get(str(status.get("operating_period")), "Outside Operating Hours")
            next_transition = self._format_dashboard_datetime(status.get("next_transition"))
            text = (
                f"Scheduler: {status['state'].replace('_', ' ').title()} · "
                f"{'enabled' if status['enabled'] else 'disabled'}\n"
                f"Current period: {period}\n"
                f"Eligible accounts: {status.get('eligible_account_count', 0)} · "
                f"maximum concurrency: {status.get('maximum_concurrency', 0)}\n"
                f"Running: {account_names(status.get('running_scheduled_accounts', []))}\n"
                f"Waiting: {account_names(status.get('idle_eligible_accounts', []))}\n"
                f"Next transition: {next_transition or 'unavailable'}\n"
                f"Operating hours: {self._format_dashboard_time(settings['operating_start'])}–"
                f"{self._format_dashboard_time(settings['operating_end'])} IST\n"
                f"Peak hours: {self._format_dashboard_time(settings['peak_start'])}–"
                f"{self._format_dashboard_time(settings['peak_end'])} IST\n"
                f"Concurrency: off-peak {settings['outside_peak_concurrency_1_20']}/"
                f"{settings['outside_peak_concurrency_21_50']}/{settings['outside_peak_concurrency_51_plus']} "
                f"by account tier · peak {settings['peak_concurrency']}"
            )
            recovery = status.get("recovery") or {}
            assignments = recovery.get("assignments") or []
            recovery_lines = [
                "\nRecovery status: " + status.get("recovery_confirmation_status", "not_required").replace("_", " "),
                f"Interrupted accounts: {len(assignments)}",
            ]
            for item in assignments:
                progress = item.get("progress") or {}
                account_id = str(item.get("account_id") or "")
                recovery_lines.append(
                    f"• {item.get('campaign_name', 'Promotion')} · "
                    f"{self._restriction_account_label(account_id)} · "
                    f"{progress.get('successful', 0)} completed cycles saved"
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
            accounts = await asyncio.to_thread(self._authenticated_accounts)
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
        accounts = await asyncio.to_thread(self._authenticated_accounts)
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

    @staticmethod
    def _deferred_callback_key(data: str) -> str | None:
        parts = data.split(":")
        if len(parts) == 3 and parts[0] == "campaign_control":
            return f"campaign:{parts[2]}"
        if len(parts) == 2 and parts[0] in {"scheduler_recovery", "content_save"}:
            return "scheduler" if parts[0] == "scheduler_recovery" else "confirmation"
        if len(parts) == 2 and parts[0] in {"reconnect", "disconnect", "test_account"}:
            return f"account:{parts[1]}"
        if len(parts) == 4 and parts[0] == "sticker_select" and parts[1] == "test":
            return f"account:test:{parts[2]}"
        if parts[0] == "promo" and (
            data in {
                "promo:save", "promo:start", "promo:pause", "promo:resume",
                "promo:refresh",
            }
            or (len(parts) == 3 and parts[1] == "activity_refresh")
            or (len(parts) == 3 and parts[1] == "report")
        ):
            return "scheduler" if parts[1] != "report" else "report"
        if data == "scheduler:toggle":
            return "scheduler"
        if len(parts) == 4 and parts[:3] == ["dash", "action", "settings"] and parts[3] in {
            "scheduler_toggle", "scheduler_recovery",
        }:
            return "scheduler"
        if len(parts) == 2 and parts[0] in {"action_yes", "remove_yes"}:
            return "confirmation"
        return None

    async def _callback_router(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        if query is None:
            return
        data = query.data or ""
        operation_key = self._deferred_callback_key(data)
        if operation_key is None:
            await query.answer()
            await self._route_callback(update, context)
            return
        if operation_key in self._background_control_keys:
            await query.answer("That control operation is already in progress.", show_alert=True)
            return
        if len(self._background_control_tasks) >= _MAX_DEFERRED_CONTROL_OPERATIONS:
            await query.answer("Control service is busy. Please retry shortly.", show_alert=True)
            return
        await query.answer("Request accepted; processing in the background.")
        self._background_control_keys.add(operation_key)
        task = asyncio.create_task(
            self._run_deferred_callback(update, context, operation_key),
            name=f"control-operation-{operation_key[:40]}",
        )
        self._background_control_tasks.add(task)
        task.add_done_callback(self._background_control_tasks.discard)

    async def _run_deferred_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, operation_key: str,
    ) -> None:
        try:
            await self._route_callback(update, context)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._background_failure_count += 1
            self._last_background_failure_at = datetime.now(timezone.utc).isoformat()
            logger.exception(
                "Deferred control operation failed (operation=%s, error=%s)",
                operation_key, type(exc).__name__,
            )
            query = update.callback_query
            if query is not None:
                try:
                    await query.edit_message_text(
                        "The operation failed. Existing campaign and account records were retained where possible.",
                    )
                except Exception as notify_error:
                    logger.info(
                        "Could not report deferred control failure (error=%s)",
                        type(notify_error).__name__,
                    )
        finally:
            self._background_control_keys.discard(operation_key)

    async def _route_callback(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        query = update.callback_query
        if query is None:
            return
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
                    f"until the next "
                    f"{self._format_dashboard_time(self.scheduler_service.settings.operating_start)} IST window.\n"
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
            admin_id = self.settings.admin_id
            if admin_id is None:
                await query.edit_message_text("Owner configuration is unavailable.")
                return
            if len(parts) == 2 and parts[1] in {"dashboard", "refresh"}:
                await self._show_promotion_dashboard(query, 0, refresh=parts[1] == "refresh")
                return
            if parts == ["promo", "settings"]:
                await self._show_promotion_settings(query)
                return
            if parts == ["promo", "settings", "restore"]:
                if query.message.chat.type != "private":
                    await self._edit_dashboard_message(
                        query, "Promotion settings can only be changed in a private chat.",
                    )
                    return
                try:
                    self.scheduler_service.restore_default_settings()
                except JsonStoreError as exc:
                    logger.error("Could not restore scheduler settings (error=%s)", type(exc).__name__)
                    await self._edit_dashboard_message(
                        query, "Default settings could not be saved. Existing settings remain active.",
                    )
                    return
                await self._show_promotion_settings(query)
                return
            if len(parts) == 3 and parts[1] == "activity" and parts[2].isdigit():
                await self._show_promotion_activity(query, int(parts[2]))
                return
            if len(parts) == 3 and parts[1] == "activity_refresh" and parts[2].isdigit():
                await self._show_promotion_activity(query, int(parts[2]), refresh=True)
                return
            if len(parts) == 3 and parts[1] == "restrictions" and parts[2].isdigit():
                await self._show_promotion_restrictions(query, int(parts[2]))
                return
            if parts == ["promo", "reports"]:
                await self._show_promotion_reports(query)
                return
            if len(parts) == 3 and parts[1] == "report" and parts[2] in {
                "current", "progress", "final",
            }:
                await self._show_promotion_report(query, parts[2])
                return
            if len(parts) == 3 and parts[1] == "setting":
                setting_fields = set(asdict(self.scheduler_service.settings)) - {"timezone"}
                setting_name = parts[2]
                if setting_name not in setting_fields:
                    await self._edit_dashboard_message(query, "This setting is unavailable.")
                    return
                if query.message.chat.type != "private":
                    await self._edit_dashboard_message(
                        query, "Promotion settings can only be changed in a private chat.",
                    )
                    return
                key = (admin_id, query.message.chat_id)
                if not self._begin_workflow(
                    key, "promotion_setting", "value", {"setting": setting_name},
                ):
                    await self._edit_dashboard_message(
                        query, "Finish or cancel the active dashboard workflow first.",
                    )
                    return
                if setting_name.endswith("_time") or setting_name.endswith("_start") or setting_name.endswith("_end"):
                    prompt = "Send the time as 9:00 PM (or 21:00)."
                else:
                    prompt = "Send the whole-number account limit."
                await self._edit_dashboard_message(
                    query, prompt, reply_markup=self._flow_navigation(query.message.chat_id),
                )
                return
            if len(parts) == 3 and parts[1] == "page" and parts[2].isdigit():
                await self._show_promotion_dashboard(query, int(parts[2]))
                return
            if parts == ["promo", "configure"]:
                if query.message.chat.type != "private":
                    await query.edit_message_text("Configure the promotion in a private chat with this bot.")
                    return
                key = (admin_id, query.message.chat_id)
                if not self._begin_workflow(key, "promotion_config", "name", {}):
                    await query.edit_message_text("Finish or cancel the active dashboard workflow first.")
                    return
                await query.edit_message_text(
                    "Send a name for this promotion (1-80 characters).",
                    reply_markup=self._flow_navigation(query.message.chat_id),
                )
                return
            if parts == ["promo", "cancel"]:
                key = (admin_id, query.message.chat_id)
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
                key = (admin_id, query.message.chat_id)
                workflow = self._workflows.get(key)
                target = self._target_by_id(parts[2])
                if (workflow is None or workflow.kind != "promotion_config" or workflow.step != "target"
                        or target is None or target.get("opted_in") is not True):
                    await query.edit_message_text("This target selection is stale. Open Configure Promotion again.")
                    return
                selected = set(workflow.data.get("target_ids", []))
                if parts[2] in selected:
                    selected.remove(parts[2])
                else:
                    selected.add(parts[2])
                workflow.data["target_ids"] = sorted(selected)
                await self._show_promotion_targets(query, edit=True)
                return
            if parts == ["promo", "targets_done"]:
                key = (admin_id, query.message.chat_id)
                workflow = self._workflows.get(key)
                target_ids = workflow.data.get("target_ids", []) if workflow else []
                if (workflow is None or workflow.kind != "promotion_config"
                        or workflow.step != "target" or not target_ids):
                    await query.edit_message_text("Select at least one opted-in target.")
                    return
                missing = [
                    str(target_id) for target_id in target_ids
                    if self._campaign_target_content_config(str(target_id)) is None
                ]
                if missing:
                    await query.edit_message_text(
                        "Configure valid text and/or sticker content for every selected target first. "
                        "Open Targets, choose each target, then Configure content.",
                        reply_markup=self._dashboard_navigation(None, query.message.chat_id, back_data="dash:section:targets"),
                    )
                    return
                workflow.step = "execution_mode"
                await query.edit_message_text(
                    "Choose target execution order:",
                    reply_markup=InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Sequential", callback_data="promo:mode:sequential"),
                            InlineKeyboardButton("Parallel", callback_data="promo:mode:parallel"),
                        ],
                        [InlineKeyboardButton("Cancel", callback_data="promo:cancel")],
                    ]),
                )
                return
            if len(parts) == 3 and parts[1] == "mode" and parts[2] in {"sequential", "parallel"}:
                key = (admin_id, query.message.chat_id)
                workflow = self._workflows.get(key)
                if (workflow is None or workflow.kind != "promotion_config"
                        or workflow.step != "execution_mode"):
                    await query.edit_message_text("This execution mode selection is stale.")
                    return
                workflow.data["execution_mode"] = parts[2]
                workflow.step = "confirm"
                target_lines = []
                for target_id in workflow.data.get("target_ids", []):
                    config = self._campaign_target_content_config(str(target_id)) or {}
                    target_lines.append(
                        self._format_target_content_summary(str(target_id), config)
                    )
                extra_targets = max(0, len(target_lines) - 12)
                summary = (
                    f"Scheduled promotion summary\nName: {workflow.data.get('name', 'Promotion')}\n"
                    f"Execution: {parts[2]}\nTargets:\n" + "\n".join(target_lines[:12])
                    + (f"\n+ {extra_targets} additional targets" if extra_targets else "")
                    + "\n\nSave this configuration? Only explicitly opted-in targets are eligible."
                )
                await query.edit_message_text(summary, reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Save configuration", callback_data="promo:save"),
                    InlineKeyboardButton("Cancel", callback_data="promo:cancel"),
                ]]))
                return
            if parts == ["promo", "save"]:
                key = (admin_id, query.message.chat_id)
                workflow = self._workflows.get(key)
                if (workflow is None or workflow.kind != "promotion_config"
                        or workflow.step != "confirm"):
                    await query.edit_message_text("This promotion save confirmation is stale.")
                    return
                try:
                    await self.scheduler_service.save_promotion_config(
                        str(workflow.data.get("name", "")),
                        list(workflow.data.get("target_ids", [])),
                        str(workflow.data.get("execution_mode", "sequential")),
                    )
                except ValueError as exc:
                    await query.edit_message_text(str(exc), reply_markup=self._dashboard_navigation(
                        None, query.message.chat_id, back_data="promo:dashboard"))
                    return
                except JsonStoreError as exc:
                    logger.error("Could not save scheduled promotion configuration (error=%s)", type(exc).__name__)
                    await query.edit_message_text(
                        "Scheduled promotion settings could not be saved. Existing settings remain active.",
                        reply_markup=self._dashboard_navigation(
                            None, query.message.chat_id, back_data="promo:dashboard",
                        ),
                    )
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
        if len(parts) == 2 and parts[0] == "campaign_content":
            campaign = self._get_campaign(parts[1])
            if (
                campaign is None or query.message is None
                or query.message.chat.type != "private"
                or self.settings.admin_id is None
            ):
                await query.edit_message_text("Campaign content editing is unavailable or stale.")
            elif campaign.get("status") == "running":
                await query.edit_message_text("Pause or stop the campaign before editing target content.")
            else:
                key = (self.settings.admin_id, query.message.chat_id)
                if not self._begin_workflow(
                    key, "campaign_content", "target", {"campaign_id": parts[1]},
                ):
                    await query.edit_message_text("Finish or cancel the active workflow first.")
                else:
                    await self._show_campaign_content_targets(
                        query, campaign, edit=True,
                    )
            return
        if len(parts) == 3 and parts[0] == "campaign_content_target":
            campaign_id, index_text = parts[1], parts[2]
            campaign = self._get_campaign(campaign_id)
            admin_id = self.settings.admin_id
            if admin_id is None or query.message is None:
                await query.edit_message_text("This campaign target selection is stale.")
                return
            key = (admin_id, query.message.chat_id)
            workflow = self._workflows.get(key)
            if (
                workflow is None or workflow.kind != "campaign_content"
                or workflow.step != "target"
                or workflow.data.get("campaign_id") != campaign_id
                or campaign is None or not index_text.isdigit()
                or int(index_text) >= len(campaign.get("target_ids", []))
            ):
                await query.edit_message_text("This campaign target selection is stale.")
                return
            target_id = str(campaign["target_ids"][int(index_text)])
            content_configs = self._campaign_content_configs(campaign) or {}
            config = content_configs.get(target_id)
            if config is None:
                await query.edit_message_text("This target's content is unavailable.")
                return
            self._workflows[key] = Workflow(
                kind="target_content", step="settings",
                data={
                    "target_id": target_id,
                    "text_enabled": config["text_enabled"],
                    "promotional_text": config.get("promotional_text") or "",
                    "sticker_enabled": config["sticker_enabled"],
                    "sticker_id": config.get("sticker_id") or "",
                    "scope": "campaign",
                    "campaign_id": campaign_id,
                },
                expires_at=datetime.now(timezone.utc) + _FLOW_TIMEOUT,
            )
            await self._show_target_content_config(query, self._workflows[key], edit=True)
            return
        if len(parts) == 3 and parts[0] == "campaign_content_page":
            campaign_id, page_text = parts[1], parts[2]
            campaign = self._get_campaign(campaign_id)
            admin_id = self.settings.admin_id
            if admin_id is None or query.message is None:
                await query.edit_message_text("This campaign target page is stale.")
                return
            key = (admin_id, query.message.chat_id)
            workflow = self._workflows.get(key)
            if (
                workflow is None or workflow.kind != "campaign_content"
                or workflow.step != "target" or workflow.data.get("campaign_id") != campaign_id
                or campaign is None or not page_text.isdigit() or int(page_text) > 100000
            ):
                await query.edit_message_text("This campaign target page is stale.")
            else:
                await self._show_campaign_content_targets(
                    query, campaign, int(page_text), edit=True,
                )
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
        if len(parts) == 2 and parts[0] in {"targets_page", "target_optin", "target_optout", "target_config", "target_remove", "target_edit", "sticker_remove", "message_view", "message_remove", "message_edit", "campaign_view", "campaign_delete", "campaign_edit"}:
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
            elif parts[0] == "target_optout":
                target = self._target_by_id(value)
                if target is None:
                    await query.edit_message_text("This target is stale or invalid.")
                else:
                    self.target_repository.update(value, {
                        "opted_in": False,
                        "opted_out_at": datetime.now(timezone.utc).isoformat(),
                    })
                    await query.edit_message_text(
                        f"@{target['username']} is opted out and will be excluded from future sends.",
                        reply_markup=self._dashboard_navigation(None, query.message.chat_id, back_data="dash:section:targets"),
                    )
            elif parts[0] == "target_config":
                target = self._target_by_id(value)
                admin_id = self.settings.admin_id
                if (
                    target is None or query.message.chat.type != "private"
                    or admin_id is None
                ):
                    await query.edit_message_text("Target configuration is unavailable here or the target is stale.")
                else:
                    key = (admin_id, query.message.chat_id)
                    if not self._begin_workflow(
                        key, "target_content", "settings",
                        {"target_id": value, **self._target_content_defaults(target)},
                    ):
                        await query.edit_message_text("Finish or cancel the active workflow before configuring a target.")
                    else:
                        await self._show_target_content_config(query, self._workflows[key], edit=True)
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
            if purpose not in {"view", "remove", "message", "campaign", "target_content", "test"} or not page_text.isdigit():
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
            elif purpose == "target_content":
                admin_id = self.settings.admin_id
                if admin_id is None:
                    await query.edit_message_text("This target configuration expired. Open the target again.")
                    return
                key = (admin_id, query.message.chat_id)
                flow = self._workflows.get(key)
                if flow is None or flow.kind != "target_content" or flow.step != "settings":
                    await query.edit_message_text("This target configuration expired. Open the target again.")
                else:
                    flow.data["sticker_id"] = sticker_id
                    flow.data["sticker_enabled"] = True
                    await self._show_target_content_config(query, flow, edit=True)
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
            elif any(
                self._campaign_target_content_config(str(target_id)) is None
                for target_id in flow.data.get("target_ids", [])
            ):
                await query.edit_message_text(
                    "Configure valid text and/or sticker content for every selected target first. "
                    "Open Targets, choose each target, then Configure content.",
                    reply_markup=self._flow_navigation(query.message.chat_id),
                )
            else:
                flow.step = "cycles"
                await query.edit_message_text("Send the number of cycles per selected target (1-100000).", reply_markup=self._flow_navigation(query.message.chat_id))
            return
        if len(parts) == 2 and parts[0] == "campaign_mode":
            key = (self.settings.admin_id, query.message.chat_id)
            flow = self._workflows.get(key)
            mode = parts[1]
            if (flow is None or flow.kind != "campaign" or flow.step != "execution_mode"
                    or mode not in {"sequential", "parallel"}):
                await query.edit_message_text("This campaign execution selection is stale.")
                return
            target_configs = {}
            for target_id in flow.data.get("target_ids", []):
                config = self._campaign_target_content_config(str(target_id))
                if config is not None:
                    target_configs[str(target_id)] = config
            if len(target_configs) != len(flow.data.get("target_ids", [])):
                await query.edit_message_text(
                    "A selected target's content is no longer available. Nothing was saved.",
                    reply_markup=self._flow_navigation(query.message.chat_id),
                )
                return
            flow.data["target_configs"] = target_configs
            flow.data["execution_mode"] = mode
            flow.step = "confirm"
            nonce = secrets.token_urlsafe(6)
            flow.data["confirmation"] = nonce
            self._action_confirmations[nonce] = ActionConfirmation(
                "save_campaign", str(self.settings.admin_id), query.message.chat_id,
                datetime.now(timezone.utc) + _CONFIRM_TIMEOUT,
            )
            target_lines = []
            for target_id in flow.data["target_ids"]:
                config = target_configs[str(target_id)]
                target_lines.append(
                    self._format_target_content_summary(str(target_id), config)
                )
            extra_targets = max(0, len(target_lines) - 12)
            summary = (
                f"Campaign summary\nName: {flow.data.get('name', 'Campaign')}\n"
                f"Execution: {mode}\nAccounts: {len(flow.data.get('account_ids', []))}\n"
                f"Cycles per target: {flow.data.get('total_cycles', 1)}\nTargets:\n"
                + "\n".join(target_lines[:12])
                + (f"\n+ {extra_targets} additional targets" if extra_targets else "")
                + "\n\nConfirm to save and start this opt-in campaign."
            )
            markup = self._with_flow_navigation(InlineKeyboardMarkup([[
                InlineKeyboardButton("Confirm and start", callback_data=f"content_save:{nonce}"),
                InlineKeyboardButton("Cancel", callback_data=f"content_cancel:{nonce}"),
            ]]), query.message.chat_id)
            await query.edit_message_text(summary, reply_markup=markup)
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
        if len(parts) == 3 and parts[0] == "tconf":
            action, target_id = parts[1], parts[2]
            admin_id = self.settings.admin_id
            if admin_id is None:
                await query.edit_message_text("This target configuration is stale. Open the target again.")
                return
            key = (admin_id, query.message.chat_id)
            workflow = self._workflows.get(key)
            target = self._target_by_id(target_id)
            if (workflow is None or workflow.kind != "target_content"
                    or str(workflow.data.get("target_id")) != target_id or target is None):
                await query.edit_message_text("This target configuration is stale. Open the target again.")
                return
            if action in {"text", "sticker"}:
                enabled_key = f"{action}_enabled"
                if workflow.data.get(enabled_key) is True:
                    if sum(bool(workflow.data.get(f"{kind}_enabled")) for kind in ("text", "sticker")) <= 1:
                        await query.edit_message_text("At least one content type must remain enabled.")
                        return
                    workflow.data[enabled_key] = False
                else:
                    workflow.data[enabled_key] = True
                    if action == "text" and not workflow.data.get("promotional_text"):
                        workflow.step = "text"
                        await query.edit_message_text(
                            "Send the promotional text (1-4096 characters).",
                            reply_markup=self._flow_navigation(query.message.chat_id),
                        )
                        return
                    if action == "sticker" and not self._get_sticker(workflow.data.get("sticker_id")):
                        await self._stickers_page(query, 0, edit=True, purpose="target_content")
                        return
                await self._show_target_content_config(query, workflow, edit=True)
            elif action == "settext" and workflow.data.get("text_enabled") is True:
                workflow.step = "text"
                await query.edit_message_text(
                    "Send the promotional text (1-4096 characters).",
                    reply_markup=self._flow_navigation(query.message.chat_id),
                )
            elif action == "setsticker" and workflow.data.get("sticker_enabled") is True:
                await self._stickers_page(query, 0, edit=True, purpose="target_content")
            elif action == "save":
                config = self._validated_target_content_config(target, workflow.data)
                if config is None:
                    await query.edit_message_text(
                        "Choose at least one enabled content type and provide its text or sticker.",
                        reply_markup=self._flow_navigation(query.message.chat_id),
                    )
                    return
                try:
                    if workflow.data.get("scope") == "campaign":
                        campaign_id = str(workflow.data.get("campaign_id", ""))
                        campaign = self._get_campaign(campaign_id)
                        configs = self._campaign_content_configs(campaign) if campaign else None
                        if campaign is None or campaign.get("status") == "running" or configs is None:
                            await query.edit_message_text("Campaign changed or is running; target settings were not saved.")
                            return
                        configs[target_id] = config
                        self.campaign_repository.update(campaign_id, {
                            "target_configs": configs,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        })
                    else:
                        self.target_repository.update(target_id, {
                            "promotion_config": config,
                            "promotion_config_updated_at": datetime.now(timezone.utc).isoformat(),
                        })
                except JsonStoreError as exc:
                    logger.error("Could not save target content configuration (error=%s)", type(exc).__name__)
                    await query.edit_message_text(
                        "Target settings could not be saved. Existing settings remain active.",
                        reply_markup=self._flow_navigation(query.message.chat_id),
                    )
                    return
                self._workflows.pop(key, None)
                if workflow.data.get("scope") == "campaign":
                    updated_campaign = self._get_campaign(str(workflow.data["campaign_id"]))
                    if updated_campaign is None:
                        await query.edit_message_text("Target settings were saved, but the campaign is unavailable.")
                    else:
                        await self._show_campaign(query, updated_campaign, edit=True)
                else:
                    await self._show_target(query, self._target_by_id(target_id) or target, edit=True)
            else:
                await query.edit_message_text("This target configuration action is unavailable.")
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
        if workflow.kind == "promotion_setting" and workflow.step == "value":
            setting_name = str(workflow.data.get("setting", ""))
            setting_fields = set(asdict(self.scheduler_service.settings)) - {"timezone"} \
                if self.scheduler_service is not None else set()
            if self.scheduler_service is None or setting_name not in setting_fields:
                self._workflows.pop(key, None)
                await message.reply_text("This promotion setting is no longer available.")
                return
            try:
                if setting_name.endswith("_time") or setting_name.endswith("_start") or setting_name.endswith("_end"):
                    parsed_time = None
                    for time_format in ("%I:%M %p", "%H:%M"):
                        try:
                            parsed_time = datetime.strptime(text.upper(), time_format).time()
                            break
                        except ValueError:
                            continue
                    if parsed_time is None:
                        raise ValueError("Enter a valid time such as 9:00 PM or 21:00.")
                    value = parsed_time.strftime("%H:%M")
                else:
                    try:
                        value = int(text)
                    except ValueError as exc:
                        raise ValueError("Enter a whole number within the allowed limit.") from exc
                self.scheduler_service.update_settings(**{setting_name: value})
            except ValueError as exc:
                await message.reply_text(
                    f"{exc}\nTry again or use /cancel.",
                    reply_markup=self._flow_navigation(message.chat_id),
                )
                return
            except JsonStoreError as exc:
                logger.error("Could not persist scheduler setting (error=%s)", type(exc).__name__)
                await message.reply_text(
                    "The setting could not be saved. Existing settings remain active; try again or /cancel.",
                    reply_markup=self._flow_navigation(message.chat_id),
                )
                return
            self._workflows.pop(key, None)
            await message.reply_text(
                "Promotion setting saved.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⬅️ Back", callback_data="promo:dashboard"),
                    InlineKeyboardButton("⚙️ Settings", callback_data="promo:settings"),
                    InlineKeyboardButton("🏠 Home", callback_data="dash:home"),
                ]]),
            )
            return
        if workflow.kind == "target_content" and workflow.step == "text":
            promotional_text = text.strip()
            if not promotional_text or len(promotional_text) > _TEXT_LIMIT:
                await message.reply_text(
                    "Promotional text must contain 1-4096 characters.",
                    reply_markup=self._flow_navigation(message.chat_id),
                )
                return
            workflow.data["promotional_text"] = promotional_text
            workflow.data["text_enabled"] = True
            workflow.step = "settings"
            await self._show_target_content_config(message, workflow)
            return
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
            workflow.step = "execution_mode"
            await message.reply_text(
                "Choose how selected targets should be processed:",
                reply_markup=self._with_flow_navigation(InlineKeyboardMarkup([[
                    InlineKeyboardButton("Sequential", callback_data="campaign_mode:sequential"),
                    InlineKeyboardButton("Parallel", callback_data="campaign_mode:parallel"),
                ]]), message.chat_id),
            )
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
        config = target.get("promotion_config") or {}
        content_status = (
            f"\nContent: text {'on' if config.get('text_enabled') else 'off'}, "
            f"sticker {'on' if config.get('sticker_enabled') else 'off'}"
            if isinstance(config, dict) and config else "\nContent: not configured"
        )
        text = (
            f"Target: @{target['username']}\n"
            f"Opted in: {'Yes' if target.get('opted_in') is True else 'No'}"
            f"{content_status}\nID: {target['id']}"
        )
        buttons = [
            [InlineKeyboardButton("Configure content", callback_data=f"target_config:{target['id']}")],
            [InlineKeyboardButton("Edit username", callback_data=f"target_edit:{target['id']}")],
            [InlineKeyboardButton("Remove target", callback_data=f"target_remove:{target['id']}")],
        ]
        if target.get("opted_in") is not True:
            buttons.insert(0, [InlineKeyboardButton("Confirm explicit opt-in", callback_data=f"target_optin:{target['id']}")])
        else:
            buttons.insert(0, [InlineKeyboardButton("Record opt-out", callback_data=f"target_optout:{target['id']}")])
        buttons.append([InlineKeyboardButton("Back to targets", callback_data="targets_page:0")])
        buttons.append([InlineKeyboardButton("Home", callback_data="dash:home")])
        markup = InlineKeyboardMarkup(buttons)
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    @staticmethod
    def _target_content_defaults(target: dict) -> dict:
        saved = target.get("promotion_config")
        if not isinstance(saved, dict):
            saved = {}
        return {
            "text_enabled": saved.get("text_enabled") is True,
            "promotional_text": str(saved.get("promotional_text") or ""),
            "sticker_enabled": saved.get("sticker_enabled") is True,
            "sticker_id": str(saved.get("sticker_id") or ""),
        }

    def _validated_target_content_config(self, target: dict, values: dict) -> dict | None:
        text_enabled = values.get("text_enabled") is True
        sticker_enabled = values.get("sticker_enabled") is True
        if not text_enabled and not sticker_enabled:
            return None
        promotional_text = str(values.get("promotional_text") or "").strip()
        if text_enabled and (not promotional_text or len(promotional_text) > _TEXT_LIMIT):
            return None
        sticker_id = str(values.get("sticker_id") or "")
        if sticker_enabled and self._get_sticker(sticker_id) is None:
            return None
        return {
            "target_id": str(target["id"]),
            "target_username": str(target["username"]),
            "target_display_name": f"@{target['username']}",
            "text_enabled": text_enabled,
            "promotional_text": promotional_text if text_enabled else None,
            "sticker_enabled": sticker_enabled,
            "sticker_id": sticker_id if sticker_enabled else None,
        }

    async def _show_target_content_config(self, message_or_query, workflow: Workflow, edit: bool = False) -> None:
        target = self._target_by_id(str(workflow.data.get("target_id", "")))
        if target is None:
            await message_or_query.edit_message_text("This target is no longer available.")
            return
        text_enabled = workflow.data.get("text_enabled") is True
        sticker_enabled = workflow.data.get("sticker_enabled") is True
        text_value = str(workflow.data.get("promotional_text") or "")
        sticker = self._get_sticker(str(workflow.data.get("sticker_id") or ""))
        text_summary = (
            text_value[:150] + ("..." if len(text_value) > 150 else "")
            if text_enabled and text_value else "not set"
        )
        sticker_summary = self._sticker_name(sticker) if sticker_enabled and sticker else "not set"
        rows = [
            [InlineKeyboardButton(
                f"Text: {'Enabled' if text_enabled else 'Disabled'}",
                callback_data=f"tconf:text:{target['id']}",
            )],
            [InlineKeyboardButton(
                f"Sticker: {'Enabled' if sticker_enabled else 'Disabled'}",
                callback_data=f"tconf:sticker:{target['id']}",
            )],
        ]
        if text_enabled:
            rows.append([InlineKeyboardButton("Set promotional text", callback_data=f"tconf:settext:{target['id']}")])
        if sticker_enabled:
            rows.append([InlineKeyboardButton("Select sticker", callback_data=f"tconf:setsticker:{target['id']}")])
        rows.extend([
            [InlineKeyboardButton("Save target settings", callback_data=f"tconf:save:{target['id']}")],
            [InlineKeyboardButton("Cancel", callback_data="dash:cancel:back")],
        ])
        markup = self._with_flow_navigation(InlineKeyboardMarkup(rows), self._chat_id(message_or_query))
        prompt = (
            f"Content settings for @{target['username']}\n"
            f"Text: {'Enabled' if text_enabled else 'Disabled'} — {text_summary}\n"
            f"Sticker: {'Enabled' if sticker_enabled else 'Disabled'} — {sticker_summary}\n\n"
            "At least one content type must be enabled."
        )
        if edit:
            await message_or_query.edit_message_text(prompt, reply_markup=markup)
        else:
            await message_or_query.reply_text(prompt, reply_markup=markup)

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
        if purpose in {"message", "campaign", "target_content", "test"}:
            markup = self._with_flow_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        else:
            markup = self._dashboard_navigation(InlineKeyboardMarkup(rows) if rows else None, self._chat_id(message_or_query))
        text = "Select a sticker:" if purpose in {"message", "campaign", "target_content", "test"} else f"Stickers: {len(stickers)}"
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

    def _campaign_target_content_config(
        self, target_id: str, saved: dict | None = None, legacy_sticker_id: str | None = None,
    ) -> dict | None:
        target = self._target_by_id(str(target_id))
        if target is None or target.get("opted_in") is not True:
            return None
        if saved is None and legacy_sticker_id:
            saved = {
                "target_id": str(target_id),
                "text_enabled": True,
                "promotional_text": None,
                "legacy_rotating_text": True,
                "sticker_enabled": True,
                "sticker_id": legacy_sticker_id,
            }
        elif saved is None:
            saved = target.get("promotion_config")
        if not isinstance(saved, dict) or (
            saved.get("target_id") is not None and str(saved.get("target_id")) != str(target_id)
        ):
            return None
        text_enabled = saved.get("text_enabled") is True
        sticker_enabled = saved.get("sticker_enabled") is True
        if not text_enabled and not sticker_enabled:
            return None
        text = saved.get("promotional_text")
        legacy_rotating_text = saved.get("legacy_rotating_text") is True
        if text_enabled and not legacy_rotating_text and (
            not isinstance(text, str) or not text.strip() or len(text.strip()) > _TEXT_LIMIT
        ):
            return None
        sticker_id = saved.get("sticker_id")
        if sticker_enabled and self._get_sticker(str(sticker_id or "")) is None:
            return None
        return {
            "target_id": str(target_id),
            "target_username": str(target["username"]),
            "target_display_name": f"@{target['username']}",
            "text_enabled": text_enabled,
            "promotional_text": text.strip() if text_enabled and isinstance(text, str) else None,
            "legacy_rotating_text": legacy_rotating_text,
            "sticker_enabled": sticker_enabled,
            "sticker_id": str(sticker_id) if sticker_enabled else None,
        }

    def _campaign_content_configs(self, campaign: dict) -> dict[str, dict] | None:
        target_ids = campaign.get("target_ids")
        saved_configs = campaign.get("target_configs")
        if not isinstance(target_ids, list) or not target_ids:
            return None
        configs: dict[str, dict] = {}
        for raw_id in target_ids:
            target_id = str(raw_id)
            saved = saved_configs.get(target_id) if isinstance(saved_configs, dict) else None
            config = self._campaign_target_content_config(
                target_id, saved,
                str(campaign.get("sticker_id")) if not isinstance(saved_configs, dict) else None,
            )
            if config is None:
                return None
            configs[target_id] = config
        if isinstance(saved_configs, dict) and set(saved_configs) != set(configs):
            return None
        return configs

    def _format_target_content_summary(
        self, target_id: str, config: dict, rotation_index: int = 0,
    ) -> str:
        target = self._target_by_id(str(target_id)) or {}
        target_name = str(config.get("target_display_name") or f"@{target.get('username', 'unknown')}")
        if not config.get("text_enabled"):
            text_summary = "text off"
        elif config.get("legacy_rotating_text"):
            sample = PROMOTION_MESSAGES[rotation_index % len(PROMOTION_MESSAGES)]
            text_summary = f"rotating text ({sample[:45]})"
        else:
            text_summary = f"text '{str(config.get('promotional_text') or '')[:60]}'"
        if config.get("sticker_enabled"):
            sticker = self._get_sticker(str(config.get("sticker_id") or ""))
            sticker_summary = f"sticker {self._sticker_name(sticker or {})}"
        else:
            sticker_summary = "sticker off"
        return f"{target_name}: {text_summary}; {sticker_summary}"

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
        if self._campaign_content_configs(campaign) is None:
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
        execution_mode = campaign.get("execution_mode", "sequential")
        if execution_mode not in {"sequential", "parallel"}:
            return None
        try:
            concurrency_limit = int(campaign.get("concurrency_limit", 3))
        except (ValueError, TypeError):
            return None
        if not 1 <= concurrency_limit <= 5:
            return None
        changes["execution_mode"] = execution_mode
        changes["concurrency_limit"] = concurrency_limit
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
        target_names_summary = ", ".join(target_names[:8])
        if len(target_names) > 8:
            target_names_summary += f", +{len(target_names) - 8} more"
        content_configs = self._campaign_content_configs(campaign) or {}
        progress = campaign.get("progress") or {}
        total = max(0, int(progress.get("total", 0)))
        completed = max(0, int(progress.get("completed", 0)))
        restricted = int(progress.get("restricted", 0))
        if self.campaign_execution is not None:
            restricted = sum(
                int(task.get("restricted", 0))
                for task in await asyncio.to_thread(
                    self.campaign_execution.task_records, str(campaign["id"]),
                )
            )
        percent = min(100, int(completed * 100 / total)) if total else 0
        bar = "#" * (percent // 5) + "-" * (20 - percent // 5)
        rotation = int(campaign.get("rotation_index", 0)) % len(PROMOTION_MESSAGES)
        content_lines = [
            self._format_target_content_summary(target_id, config, rotation)
            for target_id, config in content_configs.items()
        ]
        additional_content_targets = max(0, len(content_lines) - 8)
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
            f"Targets ({len(target_names)}): {target_names_summary}\n"
            f"Execution: {campaign.get('execution_mode', 'sequential')}\n"
            f"Cycles per target: {campaign.get('total_cycles', 1)}\n"
            + "\n".join(content_lines[:8])
            + (f"\n+ {additional_content_targets} additional targets" if additional_content_targets else "")
            + "\n"
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
            [InlineKeyboardButton("Edit target content", callback_data=f"campaign_content:{campaign['id']}")],
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
            if len(self._campaign_last_edit) > 512:
                oldest = min(self._campaign_last_edit, key=self._campaign_last_edit.get)
                self._campaign_last_edit.pop(oldest, None)
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

    async def _show_campaign_content_targets(
        self, message_or_query, campaign: dict, page: int = 0, edit: bool = False,
    ) -> None:
        targets = [
            self._target_by_id(str(target_id)) for target_id in campaign.get("target_ids", [])
        ]
        targets = [target for target in targets if target is not None]
        pages = max(1, (len(targets) + _PAGE_SIZE - 1) // _PAGE_SIZE)
        page = min(max(int(page), 0), pages - 1)
        start = page * _PAGE_SIZE
        rows = [[InlineKeyboardButton(
            f"@{target['username']}"[:60],
            callback_data=f"campaign_content_target:{campaign['id']}:{index}",
        )] for index, target in enumerate(targets[start:start + _PAGE_SIZE], start=start)]
        if pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton(
                    "Previous", callback_data=f"campaign_content_page:{campaign['id']}:{page - 1}",
                ))
            nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
            if page + 1 < pages:
                nav.append(InlineKeyboardButton(
                    "Next", callback_data=f"campaign_content_page:{campaign['id']}:{page + 1}",
                ))
            rows.append(nav)
        rows.append([InlineKeyboardButton("Campaign", callback_data=f"campaign_view:{campaign['id']}")])
        markup = self._dashboard_navigation(
            InlineKeyboardMarkup(rows), self._chat_id(message_or_query),
            back_data=f"campaign_view:{campaign['id']}",
        )
        text = "Choose one target to edit its saved content only:"
        if edit:
            await message_or_query.edit_message_text(text, reply_markup=markup)
        else:
            await message_or_query.reply_text(text, reply_markup=markup)

    def _lookup_action_record(self, kind: str, record_id: str) -> dict | None:
        if not _RECORD_ID_PATTERN.fullmatch(record_id):
            return None
        if kind == "target":
            item = self._target_by_id(record_id)
            scheduled_target_ids = (
                (self.scheduler_service.execution_service.scheduled_promotion_model() or {}).get("target_ids", [])
                if self.scheduler_service is not None else []
            )
            if item is not None and (
                any(record_id in campaign.get("target_ids", []) for campaign in self.campaign_repository.list())
                or record_id in scheduled_target_ids
            ):
                return None
            return item
        if kind == "sticker":
            item = self._get_sticker(record_id)
            scheduled_model = (
                self.scheduler_service.execution_service.scheduled_promotion_model() or {}
                if self.scheduler_service is not None else {}
            )
            if item is not None and (
                any(record.get("sticker_id") == record_id for record in self.message_repository.list())
                or any(record.get("sticker_id") == record_id for record in self.campaign_repository.list())
                or any(
                    (target.get("promotion_config") or {}).get("sticker_id") == record_id
                    for target in self.target_repository.list()
                )
                or any(
                    (record.get("target_configs") or {}).get(str(target_id), {}).get("sticker_id") == record_id
                    for record in self.campaign_repository.list()
                    for target_id in record.get("target_ids", [])
                )
                or any(
                    (scheduled_model.get("target_configs") or {}).get(str(target_id), {}).get("sticker_id") == record_id
                    for target_id in scheduled_model.get("target_ids", [])
                )
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
        admin_id = self.settings.admin_id
        if confirmation is None or confirmation.chat_id != chat_id or confirmation.expires_at <= datetime.now(timezone.utc):
            await query.edit_message_text("This confirmation expired or is no longer valid. Start the command again.")
            return
        if action == "action_no" or action == "content_cancel":
            if admin_id is None:
                await query.edit_message_text("Owner configuration is unavailable.")
                return
            key = (admin_id, chat_id)
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
            if admin_id is None:
                await query.edit_message_text("Owner configuration is unavailable.")
                return
            key = (admin_id, chat_id)
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
                submitted_configs = workflow.data.get("target_configs")
                target_configs = {}
                if isinstance(submitted_configs, dict):
                    for target in targets:
                        if target is None:
                            continue
                        target_id = str(target["id"])
                        config = self._campaign_target_content_config(
                            target_id, submitted_configs.get(target_id),
                        )
                        if config is not None:
                            target_configs[target_id] = config
                cycles = int(workflow.data.get("total_cycles", 0))
                if (not accounts or any(item is None for item in accounts) or not targets
                        or any(item is None or item.get("opted_in") is not True for item in targets)
                        or len(target_configs) != len(targets)
                        or workflow.data.get("execution_mode") not in {"sequential", "parallel"}
                        or cycles < 1 or cycles > 100000):
                    await query.edit_message_text("Campaign selections changed or are unavailable. Nothing was saved or started.")
                else:
                    record_id = uuid.uuid4().hex
                    total = cycles * len(account_ids) * len(targets)
                    record = self.campaign_repository.create({
                        "id": record_id,
                        "name": workflow.data["name"],
                        "account_ids": account_ids,
                        "target_ids": [item["id"] for item in targets],
                        "target_configs": target_configs,
                        "execution_mode": workflow.data["execution_mode"],
                        "concurrency_limit": 3,
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
