# MTProto Promotion Application

A modular Telegram promotion management application with local JSON storage, Telethon account sessions, and an owner-only Telegram control bot. The control bot is the only user interface. Campaigns execute sequentially through Telethon and only target bots marked explicitly opted in.

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
- `bot_sessions.json`
- `bot_configurations.json`
- `settings.json`

`JsonStore` assigns unique IDs and uses file locks plus same-directory atomic replacement for writes. `FileManager` handles physical files under an allowed root. `BackupManager` creates local ZIP backups and restores only paths belonging to configured storage roots. No database is configured, created, or contacted.

## Telegram account login through the control bot

In a private chat with the control bot, use `/addaccount`, then send the phone number in international format, the login code, and a Telegram 2FA password if requested. Telegram bot chats are not end-to-end encrypted. The bot attempts to delete phone, code, and password messages after processing, but deletion depends on Telegram permissions. Pending flows expire after ten minutes and are held only in process memory, so run a single application worker while an authentication flow is active. `/cancel` cancels the pending flow. Session files are created under `SESSIONS_DIR` using fresh generated account IDs; account metadata stores only the relative session filename, and existing account sessions are not selected or overwritten. `/health` remains public and does not expose account data.

## Control bot

The control bot runs separately from the Telethon account manager using Telegram Bot API polling. A first-priority update guard checks the sender's numeric ID on every update, including callback queries, and stops processing unauthorized updates. `/start` opens the owner-only inline dashboard with Accounts, Targets, Stickers, Messages, Campaigns, Reports, and Settings sections. Each section has Back/Home navigation and routes to the existing repository-backed workflows used by slash commands. Target usernames, promotion message text, and campaign draft names have guided edit flows; message edits preview the sticker followed by text before saving. Reports and Settings are placeholders. Existing commands remain available, including `/help`, `/status`, `/accounts`, `/account`, `/disconnect`, `/reconnect`, `/removeaccount`, `/addaccount`, and `/cancel`. Account actions use inline selection, and removal requires a short-lived confirmation. `/disconnect` preserves the session; confirmed removal disconnects the client, deletes its session files, and removes its JSON record.

Phase 5.1 also adds `/addtarget`, `/targets`, `/target`, and `/removetarget`; target records are stored in `targets.json` and default to `opted_in=false`. The owner must explicitly confirm a recipient's opt-in before that target becomes selectable in a campaign. `/uploadsticker`, `/stickers`, and `/removesticker` manage named physical sticker files under `STICKERS_DIR`; each sticker record retains its Telegram file reference. `/createmessage`, `/messages`, `/message`, and `/removemessage` manage reusable sticker-then-text content, but campaigns use the built-in rotating promotion text described below. `/createcampaign`, `/campaigns`, `/campaign`, and `/deletecampaign` manage campaigns with internal IDs, inline selection, and confirmation. `/teststicker @botusername` sends one selected saved sticker using a selected connected MTProto account; it is restricted to a target marked opted in and the username must resolve to a Telegram bot. This explicit single-sticker test does not execute a campaign. HTTPX log records are filtered to redact Telegram Bot API tokens embedded in URLs. If bot startup fails, the FastAPI backend and Telethon account manager continue without the control bot.

For development, install the dependencies and run the focused storage checks with `python -m pytest tests/unit/test_storage.py`.

## Campaign execution (Phase 5.3)

Create a campaign by entering a name, selecting one or more authenticated accounts, choosing explicitly opted-in targets, entering the number of cycles per target, selecting a named sticker, reviewing the sticker-plus-text preview, and confirming. Each selected account runs every selected target sequentially. Campaigns that share an account are serialized, while account failures are isolated from other selected accounts. The dashboard provides start/resume, pause, stop, cycle-count editing, and paginated per-account/target progress. It edits the same progress message no more often than every five seconds during execution.

Each cycle resolves the target username, sends `/search`, waits, sends the selected sticker, waits, sends the next built-in text, waits, sends `/stop`, then waits before the next cycle. It does not inspect bot replies or click inline buttons. The eight texts rotate in this order: “Join fast”, “Join it”, “This one is best”, “Its completely free”, “Highly recommended”, “Try it now”, “Best one here”, and “Don't miss this”. The rotation index is shared across all workers in a campaign and persisted before each text send.

Timing is internal and is not prompted from the campaign owner. The central `CampaignTiming` defaults currently use 1.5 seconds after `/search`, sticker, text, and `/stop`; the last wait is also the delay before the next cycle. `BotEngine.cs` and its timing configuration were not present in this project workspace, so these are explicit defaults and are not claimed to be copied from that source. Adjust these named defaults in `app/campaigns/execution_service.py` when the legacy timing values are available.

Progress and per-account/target cycle tasks are stored in local JSON. A restart marks an interrupted campaign paused and does not automatically resume it. Completed cycles are not repeated; an interrupted in-flight cycle is recorded as failed and skipped, with `/stop` cleanup attempted when `/search` may have started. Because Telegram sends and local JSON writes cannot form one transaction, exactly-once delivery cannot be guaranteed if a process stops while Telegram is accepting a send. The engine avoids replaying such uncertain sends. Flood waits are honored, target opt-in is required, and Telegram restrictions are not bypassed. No database is used.

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

The default address is `http://127.0.0.1:8000`. The health check is available at `http://127.0.0.1:8000/health` and returns `{"status":"ok"}`.
