"""Environment-backed application settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Configuration shared by the application's interfaces and services."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "MTProto Promotion"
    app_env: str = "development"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"
    ban_detection_phrases: str = (
        "you are banned|you have been banned|you are temporarily banned|you cannot use this bot|"
        "your account is blocked|access to this bot has been denied"
    )
    daily_chat_limit_phrases: str = ""
    daily_chat_limit_reset_time: str = "00:00"

    telegram_api_id: int | None = Field(
        default=None, validation_alias=AliasChoices("TELEGRAM_API_ID", "API_ID")
    )
    telegram_api_hash: SecretStr = Field(
        default=SecretStr(""), validation_alias=AliasChoices("TELEGRAM_API_HASH", "API_HASH")
    )
    control_bot_token: SecretStr = SecretStr("")
    admin_id: int | None = Field(default=None, ge=1, validation_alias="ADMIN_ID")

    data_dir: Path = PROJECT_ROOT / "data"
    sessions_dir: Path = PROJECT_ROOT / "data" / "sessions"
    records_dir: Path = PROJECT_ROOT / "data" / "records"
    media_dir: Path = PROJECT_ROOT / "data" / "media"
    stickers_dir: Path = PROJECT_ROOT / "data" / "media" / "stickers"
    images_dir: Path = PROJECT_ROOT / "data" / "media" / "images"
    videos_dir: Path = PROJECT_ROOT / "data" / "media" / "videos"
    documents_dir: Path = PROJECT_ROOT / "data" / "media" / "documents"
    exports_dir: Path = PROJECT_ROOT / "data" / "exports"
    backups_dir: Path = PROJECT_ROOT / "data" / "backups"
    backup_staging_dir: Path = PROJECT_ROOT / "data" / "backups" / ".staging"
    logs_dir: Path = PROJECT_ROOT / "logs"
    application_logs_dir: Path = PROJECT_ROOT / "logs" / "application"
    telegram_logs_dir: Path = PROJECT_ROOT / "logs" / "telegram"


@lru_cache
def get_settings() -> Settings:
    """Return the cached application settings instance."""

    return Settings()
