"""
start_dashboard.py — Launch the Streamlit dashboard with public ngrok access.

Usage:
    python start_dashboard.py

What it does:
    1. Starts ngrok tunnel on port 8501 (where Streamlit listens).
    2. Polls the local ngrok API until the public HTTPS URL is ready.
    3. Sends the URL to Telegram so you can open it from anywhere.
    4. Launches Streamlit (blocks until you press Ctrl+C).
    5. Terminates ngrok cleanly on exit.
"""

import os
import sys
import time
import subprocess
import atexit
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

STREAMLIT_PORT = 8501
NGROK_API      = "http://localhost:4040/api/tunnels"
NGROK_TIMEOUT  = 15   # seconds to wait for ngrok to start
POLL_INTERVAL  = 0.5  # seconds between ngrok API polls


def _get_ngrok_url(timeout: int = NGROK_TIMEOUT) -> str | None:
    """Poll the local ngrok API until a public HTTPS tunnel URL appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(NGROK_API, timeout=3)
            tunnels = resp.json().get("tunnels", [])
            for t in tunnels:
                url = t.get("public_url", "")
                if url.startswith("https://"):
                    return url
        except Exception:
            pass
        time.sleep(POLL_INTERVAL)
    return None


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text},
            timeout=5,
        )
    except Exception:
        pass


def main():
    tg_token = os.getenv("TELEGRAM_API_KEY")
    tg_chat  = os.getenv("TELEGRAM_CHAT_ID")

    # 1. Start ngrok
    print(f"🌐 Starting ngrok on port {STREAMLIT_PORT}...")
    ngrok_proc = subprocess.Popen(
        ["ngrok", "http", str(STREAMLIT_PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Register cleanup so ngrok is always killed on exit
    def _stop_ngrok():
        if ngrok_proc.poll() is None:
            ngrok_proc.terminate()
            print("🛑 ngrok terminated.")
    atexit.register(_stop_ngrok)

    # 2. Wait for the public URL
    public_url = _get_ngrok_url()
    if public_url:
        print(f"✅ Dashboard URL: {public_url}")
        if tg_token and tg_chat:
            _send_telegram(tg_token, tg_chat,
                           f"📊 Dashboard is live!\n{public_url}")
            print("📨 URL sent to Telegram.")
        else:
            print("⚠️  Telegram not configured — URL printed above only.")
    else:
        print("⚠️  Could not get ngrok URL within timeout. "
              "Check that ngrok is installed and authenticated.")
        if tg_token and tg_chat:
            _send_telegram(tg_token, tg_chat,
                           "⚠️ Dashboard started but ngrok URL could not be retrieved.")

    # 3. Launch Streamlit (blocks)
    print(f"🚀 Launching Streamlit on port {STREAMLIT_PORT}...")
    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", "dashboard.py",
             "--server.port", str(STREAMLIT_PORT),
             "--server.headless", "true"],
            check=False,
        )
    except KeyboardInterrupt:
        pass

    print("👋 Dashboard stopped.")


if __name__ == "__main__":
    main()
