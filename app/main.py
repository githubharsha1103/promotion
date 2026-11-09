"""FastAPI application entry point."""

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

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
    scheduler_service = SchedulerService(
        SchedulerRepository(settings.records_dir), campaign_repository, account_repository,
        campaign_execution, restriction_service, scheduled_reporting,
    )
    auth_service = TelegramAuthenticationService(client_manager, account_repository)
    control_bot = ControlBotService(
        account_repository, auth_service, client_manager, settings, campaign_execution, scheduler_service,
        scheduled_reporting,
    )
    scheduler_service.set_takeover_notifier(control_bot.notify_normal_campaign_suspended)
    scheduler_service.set_report_notifier(control_bot.deliver_scheduled_report)
    scheduler_service.set_recovery_notifier(control_bot.notify_scheduler_recovery)
    restriction_service.set_notifier(control_bot.notify_restriction)
    application.state.telegram_client_manager = client_manager
    application.state.account_repository = account_repository
    application.state.telegram_auth_service = auth_service
    application.state.campaign_execution_service = campaign_execution
    application.state.scheduler_service = scheduler_service
    application.state.scheduled_reporting_service = scheduled_reporting
    application.state.control_bot_service = control_bot
    try:
        restriction_service.load_active()
        await client_manager.reconnect_saved_accounts(account_repository)
        await campaign_execution.recover_interrupted()
        scheduled_reporting.close_orphaned_assignments()
        await scheduler_service.start()
        try:
            await control_bot.start()
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "Control bot startup failed; continuing without it (error=%s)", type(exc).__name__
            )
        await scheduler_service.notify_recovery_required()
        yield
    finally:
        await scheduler_service.stop()
        await campaign_execution.shutdown()
        await control_bot.stop()
        await auth_service.close()
        await client_manager.disconnect_all()
        logging.getLogger(__name__).info("Application shutdown")


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/health", tags=["health"])
async def health_check() -> dict[str, str]:
    """Report that the web application process is responding."""

    return {"status": "ok"}
