"""
Telegram logging handler.

Adds WARNING/ERROR/CRITICAL log records to a background queue and sends them
to a Telegram bot chat without blocking the main asyncio loop.

Setup:
    1. Create a bot with @BotFather → copy the token to TELEGRAM_API_KEY in .env
    2. Send any message to your bot in the Telegram app, then run:
           python -c "from utils.telegram_logger import discover_chat_id; discover_chat_id()"
       to print your chat ID. Add it to .env as TELEGRAM_CHAT_ID.
    3. Restart the bot — Telegram logging activates automatically.
"""

import logging
import queue
import threading
import time
from typing import Optional

import requests

MAX_MSG_LEN = 4000  # Telegram cap is 4096; leave margin for emoji / prefix

_LEVEL_EMOJI = {
    logging.CRITICAL: "🚨 CRITICAL",
    logging.ERROR: "❌ ERROR",
    logging.WARNING: "⚠️ WARNING",
}


class _TelegramFormatter(logging.Formatter):
    """Compact formatter for Telegram: emoji + module name + message."""

    def format(self, record: logging.LogRecord) -> str:
        prefix = _LEVEL_EMOJI.get(record.levelno, "ℹ️")
        return f"{prefix} [{record.name}]\n{record.getMessage()}"


class TelegramHandler(logging.Handler):
    """
    Non-blocking logging handler that sends records to a Telegram chat.

    Messages are queued in memory and dispatched by a daemon thread at a rate
    of at most one per 1.5 s to stay within Telegram's per-chat rate limit.
    If the queue fills up (>50 pending messages), new records are silently
    dropped — the main bot loop is never blocked.
    """

    def __init__(self, token: str, chat_id: str):
        super().__init__()
        self.token = token
        self.chat_id = str(chat_id)
        self.setFormatter(_TelegramFormatter())
        self._queue: queue.Queue[str] = queue.Queue(maxsize=50)
        self._thread = threading.Thread(target=self._worker, daemon=True, name="TelegramLog")
        self._thread.start()

    # ------------------------------------------------------------------
    # logging.Handler interface
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._queue.put_nowait(self.format(record))
        except queue.Full:
            pass  # Never block the caller

    # ------------------------------------------------------------------
    # Background worker
    # ------------------------------------------------------------------

    def _worker(self) -> None:
        while True:
            try:
                msg = self._queue.get(timeout=10)
                self._send(msg)
                time.sleep(1.5)          # ~40 msg/min — well under Telegram limit
            except queue.Empty:
                continue
            except Exception:
                time.sleep(5)

    def _send(self, text: str) -> None:
        if len(text) > MAX_MSG_LEN:
            text = text[:MAX_MSG_LEN] + "…"
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": text},
                timeout=5,
            )
        except Exception:
            pass  # Silently swallow network errors so logging never crashes the bot


# ------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------

def send_message(token: str, chat_id: str, text: str) -> bool:
    """Send a one-off message (e.g. startup notification). Returns True on success."""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text[:MAX_MSG_LEN]},
            timeout=5,
        )
        return r.status_code == 200
    except Exception:
        return False


def discover_chat_id(token: Optional[str] = None) -> Optional[str]:
    """
    Retrieve your Telegram chat ID from the bot's most recent update.

    Send any message to your bot in the Telegram app first, then call this.
    Prints the chat ID and returns it as a string, or None if not found.
    """
    import os
    from dotenv import load_dotenv

    load_dotenv(override=True)
    token = token or os.getenv("TELEGRAM_API_KEY")
    if not token:
        print("❌ TELEGRAM_API_KEY not set in .env")
        return None

    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            timeout=5,
        )
        updates = resp.json().get("result", [])
        if not updates:
            print(
                "❌ No messages found.\n"
                "   → Open Telegram, find your bot, and send it any message (e.g. /start).\n"
                "   → Then run this again."
            )
            return None

        chat_id = str(updates[-1]["message"]["chat"]["id"])
        print(f"✅ Your Telegram chat ID: {chat_id}")
        print(f"   Add this to .env:  TELEGRAM_CHAT_ID={chat_id}")
        return chat_id

    except Exception as e:
        print(f"❌ Error calling Telegram API: {e}")
        return None


if __name__ == "__main__":
    discover_chat_id()
