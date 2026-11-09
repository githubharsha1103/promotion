"""Logging filters for sensitive Telegram Bot API URLs."""

import logging
import re
import traceback


_BOT_API_URL_TOKEN = re.compile(r"(?i)(api\.telegram\.org/bot)[^/\s\"'<>]+")


class TelegramBotTokenRedactionFilter(logging.Filter):
    """Redact bot tokens embedded in Telegram Bot API URLs in log records."""

    @staticmethod
    def _redact(value: str) -> str:
        return _BOT_API_URL_TOKEN.sub(r"\1[REDACTED]", value)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.msg = self._redact(record.getMessage())
            record.args = ()
        except Exception:
            record.msg = "[log message redacted]"
            record.args = ()
        if record.exc_info:
            record.exc_text = self._redact("".join(traceback.format_exception(*record.exc_info)))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = self._redact(record.exc_text)
        if record.stack_info:
            record.stack_info = self._redact(record.stack_info)
        return True
