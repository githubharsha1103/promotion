# MTProto Promotion Application

A modular Telegram promotion management application with local JSON storage, Telethon account sessions, and an owner-only Telegram control bot. The control bot is the only user interface. Campaigns target only bots explicitly marked opted in and support sequential or bounded parallel target execution.

## Technology stack

- Python and FastAPI for the application backend
- Uvicorn as the ASGI server
- JSON files for structured local records
- Telethon for independent MTProto client/session management
- FastAPI for the backend and application lifecycle
- python-telegram-bot for the separate centralized Bot API control interface
- VPS filesystem paths for sessions, media, logs, exports, and backups

The control bot and future modules use shared application services. This project uses no database. Structured records are stored in separate JSON files under `data/records/`; actual Telethon session and media files, logs, exports, and backups remain on the VPS filesystem. The FastAPI lifespan reconnects saved authenticated account sessions at startup and disconnects all managed clients at shutdown. Account OTP and 2FA credentials are submitted directly to Telethon and are not persisted.

## Folder structure

```text
app/                 Application code and shared configuration
  storage/           JSON store, file manager, locks, backups, repositories
  core/              Cross-cutting application concerns
  database/          Reserved for future project organization
  telegram/          MTProto client and authentication services
  control_bot/       The sole user interface and control bot lifecycle
  bot_automation/    Separate external Telegram bot automation module
  campaigns/         Campaign domain and workflows
  services/          Shared application logic used by interfaces
  schemas/           Request and response data models
data/                Local runtime data stored on the VPS filesystem
  sessions/           Telethon session files
  records/            JSON records, one file per data category
  media/              Media files grouped by type
    stickers/
    images/
    videos/
    documents/
  exports/             Generated exports
  backups/             Local backups
logs/                Runtime logs stored on the VPS filesystem
  application/        Application logs
  telegram/           Telegram integration logs
tests/               Unit, integration, and fixture directories
scripts/             Developer and operations scripts
```

## Environment configuration

The application reads settings from environment variables and the project `.env` file. `.env` contains blank credential placeholders and is excluded by Git. `.env.example` documents the available variables.

Set `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` in `.env` for MTProto access; the aliases `API_ID` and `API_HASH` are also accepted. Set `CONTROL_BOT_TOKEN` to the centralized Telegram Bot API token and `ADMIN_ID` to the owner's numeric Telegram user ID. `ADMIN_ID` is parsed as a positive integer; usernames are never used for authorization. The control bot remains disabled unless both settings are supplied. Application and storage paths are loaded from environment variables and `.env`.

Daily chat-limit detection always includes the built-in phrases. Add optional phrases to `DAILY_CHAT_LIMIT_PHRASES` separated by `|`. `DAILY_CHAT_LIMIT_RESET_TIME` sets the daily eligibility time in 24-hour `HH:MM` format (IST; defaults to `00:00`).

Storage paths can be overridden with `DATA_DIR`, `SESSIONS_DIR`, `RECORDS_DIR`, `MEDIA_DIR`, `STICKERS_DIR`, `IMAGES_DIR`, `VIDEOS_DIR`, `DOCUMENTS_DIR`, `EXPORTS_DIR`, `BACKUPS_DIR`, `BACKUP_STAGING_DIR`, `LOGS_DIR`, `APPLICATION_LOGS_DIR`, and `TELEGRAM_LOGS_DIR`. These are VPS filesystem paths. Keep this data on persistent storage and restrict access to session and credential files. `.gitignore` excludes credentials, session state, JSON records, media, exports, backups, logs, and common key files while preserving directory layouts with `.gitkeep` files.

## Local storage layout

The JSON repositories use these separate files under `data/records/`:

- `accounts.json`
- `chats.json`
- `stickers.json`
- `messages.json`
- `campaigns.json`
- `campaign_tasks.json`
- `targets.json`
- `scheduler.json`
- `bot_sessions.json`
- `bot_configurations.json`
- `settings.json`

`JsonStore` assigns unique IDs and uses file locks plus same-directory atomic replacement for writes. `FileManager` handles physical files under an allowed root. `BackupManager` creates local ZIP backups and restores only paths belonging to configured storage roots. No database is configured, created, or contacted.

## Telegram account login through the control bot

In a private chat with the control bot, use `/addaccount`, then send the phone number in international format, the login code, and a Telegram 2FA password if requested. Telegram bot chats are not end-to-end encrypted. The bot attempts to delete phone, code, and password messages after processing, but deletion depends on Telegram permissions. Pending flows expire after ten minutes and are held only in process memory, so run a single application worker while an authentication flow is active. `/cancel` cancels the pending flow. Session files are created under `SESSIONS_DIR` using fresh generated account IDs; account metadata stores only the relative session filename, and existing account sessions are not selected or overwritten. `/health` remains public and does not expose account data.

## Control bot

The control bot uses Telegram Bot API polling alongside the Telethon account manager in the FastAPI process. Update handling is bounded-concurrent; slow control callbacks are acknowledged immediately and run as a bounded set of background operations. Campaign checkpoint JSON I/O is offloaded from the asyncio event loop. A polling supervisor probes Telegram connectivity and restarts a failed poller with capped exponential backoff; it will not create another poller unless shutdown of the previous updater is confirmed. `/health` reports polling state, last successfully processed update, event-loop lag, process CPU/RSS where supported, and handler/background failures. A first-priority update guard checks the sender's numeric ID on every update, including callback queries, and stops processing unauthorized updates. `/start` opens the owner-only inline dashboard with Accounts, Targets, Stickers, Messages, Campaigns, Reports, and Settings sections. Each section has Back/Home navigation and routes to repository-backed workflows. Target profiles can be configured independently with text, sticker, or both; target opt-out is available from the target detail view. Scheduler settings include a bounded parallel-target concurrency limit. Existing commands remain available, including `/help`, `/status`, `/accounts`, `/account`, `/disconnect`, `/reconnect`, `/removeaccount`, `/addaccount`, and `/cancel`. Account actions use inline selection, and removal requires a short-lived confirmation. `/disconnect` preserves the session; confirmed removal disconnects the client, deletes its session files, and removes its JSON record.

Phase 5.1 also adds `/addtarget`, `/targets`, `/target`, and `/removetarget`; target records are stored in `targets.json` and default to `opted_in=false`. The owner must explicitly confirm opt-in before a target can be selected. Target content profiles persist under each target as `promotion_config`, including its ID/display name, `text_enabled`, `promotional_text`, `sticker_enabled`, and `sticker_id`. A profile must enable at least one type and only requires data for enabled types. `/uploadsticker`, `/stickers`, and `/removesticker` manage named physical sticker files under `STICKERS_DIR`; `/createmessage` and related commands continue managing reusable sticker-then-text records. `/createcampaign`, `/campaigns`, `/campaign`, and `/deletecampaign` manage campaigns with internal IDs, inline selection, and confirmation. `/teststicker @botusername` remains a one-sticker test restricted to an opted-in target that resolves to a Telegram bot. HTTPX log records are filtered to redact Telegram Bot API tokens embedded in URLs. If bot startup fails, the FastAPI backend and Telethon account manager continue without the control bot.

For development, install the dependencies and run the focused storage checks with `python -m pytest tests/unit/test_storage.py`.

## Campaign execution (Phase 5.3)

Configure each target's content from Targets → target → Configure content. Then create a campaign by entering a name, selecting authenticated accounts and explicitly opted-in targets, setting cycles, choosing sequential or parallel target execution, reviewing the configuration summary, and confirming. Campaign JSON stores a `target_configs` map keyed by target ID, so settings cannot leak between destinations; the selected execution mode and concurrency limit are saved with the campaign. Sequential mode finishes one target before moving on. Parallel mode runs independent account/target tasks concurrently, capped at five and further limited by the scheduler's `parallel_target_concurrency` setting. Progress remains independently checkpointed for each account/target pair. The dashboard provides start/resume, pause, stop, cycle-count editing, and paginated progress.

Each cycle resolves the target username and rechecks opt-in before sending. Enabled content is sent in the established sticker-then-text order; disabled content is skipped. Existing JSON campaigns without `target_configs` retain their prior sticker-plus-rotating-text behavior. The built-in rotation is retained only for those legacy records. Telegram rate limits and restrictions are honored; opt-outs stop further sends.

Timing is internal and is not prompted from the campaign owner. The central `CampaignTiming` defaults currently use 1.5 seconds after `/search`, sticker, text, and `/stop`; the last wait is also the delay before the next cycle. `BotEngine.cs` and its timing configuration were not present in this project workspace, so these are explicit defaults and are not claimed to be copied from that source. Adjust these named defaults in `app/campaigns/execution_service.py` when the legacy timing values are available.

Campaign definitions and per-account/target cycle tasks are stored in local JSON. Scheduled promotion configuration and its target-specific snapshots are stored in `scheduler.json`; both retain legacy single-target records. A restart marks interrupted campaign work paused and does not automatically resume it. Completed cycles are not repeated; an interrupted in-flight cycle is recorded as failed and skipped, with `/stop` cleanup attempted only while the target remains opted in. Because Telegram sends and local JSON writes cannot form one transaction, exactly-once delivery cannot be guaranteed if a process stops while Telegram is accepting a send. The engine avoids replaying such uncertain sends. Flood waits are honored, opt-in is required, and Telegram restrictions are not bypassed. No database is used.

Campaigns require no additional dependencies. Install and start the application as shown below. Tests and application startup were not run for this change.

## Installation

From this directory, create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Start the application

```powershell
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

The default address is `http://127.0.0.1:8000`. The public health check is available at `http://127.0.0.1:8000/health`; it retains the top-level `status` field and includes a `control_bot` health snapshot when configured.
