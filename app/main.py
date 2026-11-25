"""FastAPI application entry point."""

from contextlib import asynccontextmanager
from collections.abc import Awaitable
import logging

from fastapi import FastAPI, Request

from app.config import get_settings
from app.control_bot import ControlBotService
from app.campaigns import CampaignExecutionService
from app.core.logging_security import TelegramBotTokenRedactionFilter
from app.storage.repositories import AccountRepository
from app.storage.repositories.campaign_repository import CampaignRepository
from app.storage.repositories.sticker_repository import StickerRepository
from app.storage.repositories.target_repository import TargetRepository
from app.storage.repositories.restriction_repository import RestrictionRepository
from app.storage.repositories.scheduler_repository import SchedulerRepository
from app.storage.repositories.scheduled_reporting_repository import ScheduledReportingRepository
from app.restrictions import RestrictionService
from app.scheduler import SchedulerService
from app.reporting import ScheduledReportingService
from app.telegram.auth_service import TelegramAuthenticationService
from app.telegram.client_manager import TelegramClientManager


settings = get_settings()


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Initialize local services, restore sessions, and close clients on shutdown."""

    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    token_filter = TelegramBotTokenRedactionFilter()
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(token_filter)
    logging.getLogger("httpx").addFilter(token_filter)
    logging.getLogger("httpcore").addFilter(token_filter)
    logging.getLogger(__name__).info("Application startup")
    client_manager = TelegramClientManager(settings)
    account_repository = AccountRepository(settings.records_dir)
    campaign_repository = CampaignRepository(settings.records_dir)
    restriction_service = RestrictionService(RestrictionRepository(settings.records_dir))
    campaign_execution = CampaignExecutionService(
        campaign_repository,
        account_repository,
        TargetRepository(settings.records_dir),
        StickerRepository(settings.records_dir),
        client_manager,
        restriction_service=restriction_service,
    )
    scheduled_reporting = ScheduledReportingService(
        ScheduledReportingRepository(settings.records_dir), campaign_execution, campaign_repository,
    )
    auth_service = TelegramAuthenticationService(client_manager, account_repository)
    application.state.telegram_client_manager = client_manager
    application.state.account_repository = account_repository
    application.state.telegram_auth_service = auth_service
    application.state.campaign_execution_service = campaign_execution
    application.state.scheduled_reporting_service = scheduled_reporting
    scheduler_service = None
    control_bot = None
    shutdown_errors: list[Exception] = []
    try:
        await scheduled_reporting.start()
        scheduler_service = SchedulerService(
            SchedulerRepository(settings.records_dir), campaign_repository, account_repository,
            campaign_execution, restriction_service, scheduled_reporting,
        )
        control_bot = ControlBotService(
            account_repository, auth_service, client_manager, settings, campaign_execution,
            scheduler_service, scheduled_reporting,
        )
        scheduler_service.set_takeover_notifier(control_bot.notify_normal_campaign_suspended)
        scheduler_service.set_report_notifier(control_bot.deliver_scheduled_report)
        scheduler_service.set_recovery_notifier(control_bot.notify_scheduler_recovery)
        restriction_service.set_notifier(control_bot.notify_restriction)
        application.state.scheduler_service = scheduler_service
        application.state.control_bot_service = control_bot
        restriction_service.load_active()
        await client_manager.reconnect_saved_accounts(account_repository)
        await campaign_execution.recover_interrupted()
        scheduled_reporting.close_orphaned_assignments()
        await scheduler_service.start()
        await control_bot.start()
        await scheduler_service.notify_recovery_required()
        yield
    finally:
        async def stop_component(name: str, operation: Awaitable[None]) -> None:
            try:
                await operation
            except Exception as exc:
                shutdown_errors.append(exc)
                logging.getLogger(__name__).error(
                    "Application component shutdown failed (component=%s, error=%s)",
                    name, type(exc).__name__,
                )

        if scheduler_service is not None:
            await stop_component("scheduler", scheduler_service.stop())
        await stop_component("campaign_execution", campaign_execution.shutdown())
        if control_bot is not None:
            await stop_component("control_bot", control_bot.stop())
        await stop_component("scheduled_reporting", scheduled_reporting.stop())
        await stop_component("authentication", auth_service.close())
        await stop_component("telegram_clients", client_manager.disconnect_all())
        logging.getLogger(__name__).info("Application shutdown")
        if shutdown_errors:
            raise RuntimeError(
                f"Application shutdown failed for {len(shutdown_errors)} component(s)"
            ) from shutdown_errors[0]


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/health", tags=["health"])
async def health_check(request: Request) -> dict:
    """Report web and control-bot lifecycle health without blocking on Telegram."""

    control_bot = getattr(request.app.state, "control_bot_service", None)
    if control_bot is None:
        return {"status": "starting", "control_bot": {"state": "starting"}}
    snapshot = control_bot.health_snapshot()
    configured = bool(
        settings.control_bot_token.get_secret_value() and settings.admin_id is not None
    )
    if not configured:
        return {"status": "ok", "control_bot": {"state": "disabled"}}
    status = "ok" if snapshot["polling"] else (
        "starting" if snapshot["state"] == "restarting" else "degraded"
    )
    return {"status": status, "control_bot": snapshot}
