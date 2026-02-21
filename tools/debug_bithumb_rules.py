import sys
import io
import asyncio
import os
import signal
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Services
from clients.upbit_client import UpbitClient
from clients.bithumb_client import BithumbClient
from services.strategy_scanner import StrategyScanner
from services.overnight_position_manager import PositionManager
from database.session import init_db

# Windows Console Fix (Forces UTF-8 for emojis)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


def setup_logging():
    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Ensure directory exists
    log_dir = "data/logs"
    os.makedirs(log_dir, exist_ok=True)

    # Log to file
    log_file = os.path.join(log_dir, "bot_execution.log")

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(log_formatter)

    # Log to console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)

    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])


async def run_bot():
    setup_logging()
    logger = logging.getLogger("Main")

    # 1. Init DB
    init_db()
    logger.info("✅ Database tables initialized.")

    # 2. Init Clients
    upbit = UpbitClient(
        "UPBIT", os.getenv("UPBIT_ACCESS_KEY"), os.getenv("UPBIT_SECRET_KEY")
    )
    bithumb = BithumbClient(
        "BITHUMB", os.getenv("BITHUMB_ACCESS_KEY"), os.getenv("BITHUMB_SECRET_KEY")
    )

    spot_clients = {}
    if os.getenv("ENABLE_UPBIT", "true").lower() == "true":
        spot_clients["UPBIT"] = upbit
    if os.getenv("ENABLE_BITHUMB", "true").lower() == "true":
        spot_clients["BITHUMB"] = bithumb

    # 3. Init Services
    scanner = StrategyScanner(spot_clients)
    manager = PositionManager(spot_clients, thresholds=scanner.THRESHOLDS)

    logger.info("🤖 SNIPER BOT STARTED (IOC Entry / Market Hedge)")
    for name in spot_clients:
        logger.info(f"    ✅ {name} Enabled")

    # Signal Handling
    SHUTDOWN_EVENT = asyncio.Event()

    def signal_handler(sig, frame):
        logger.warning("\n🛑 Shutdown Signal Received! Closing gracefully...")
        SHUTDOWN_EVENT.set()

    if sys.platform != "win32":
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    # --- MAIN LOOP ---
    exit_task = asyncio.create_task(manager.run_exit_logic())
    logger.info("    🛡️ Unwinder (Exit Engine) Started.")

    try:
        while not SHUTDOWN_EVENT.is_set():
            # --- [FEATURE 1] PAUSE CHECK ---
            # Checks if you paused the bot from the Dashboard
            if scanner.THRESHOLDS.get("SYSTEM", {}).get("PAUSED", False):
                logger.info("   ⏸️  System Paused via Dashboard. Sleeping 10s...")
                await asyncio.sleep(10)
                continue

            # 0. Sync Capacity & Snapshot
            await manager.sync_capacity()

            # --- [FEATURE 2] ZOMBIE PREVENTION (CRITICAL) ---
            # Aligns DB with Wallet. Prevents "Zombie" false positives on restart.
            await manager.sync_positions()
            # ------------------------------------------------

            # Hourly Snapshot Trigger
            if datetime.now().minute == 0 and datetime.now().second < 10:
                logger.info("⏰ Triggering Hourly Portfolio Snapshot...")
                await manager.save_portfolio_snapshot()
                await asyncio.sleep(10)  # Avoid double save

            # A. Exit Logic (Background Task is running)

            # B. Scan Market
            logger.info("    📡 Refreshing Spot Markets & Symbol Map...")
            market_data = await scanner.scan()

            # C. Execute Snipe
            if market_data:
                await manager.execute_sniper_logic(market_data)

            # Rate Limit Sleep
            await asyncio.sleep(5)

    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logger.warning("\n🧹 CLEANUP INITIATED...")

        if not exit_task.done():
            exit_task.cancel()
            try:
                await exit_task
            except asyncio.CancelledError:
                pass

        if scanner:
            await scanner.close()
        if manager:
            logger.info("📸 Saving Final Snapshot...")
            await manager.save_portfolio_snapshot()
            await manager.close()
        for c in spot_clients.values():
            await c.close()

        logger.info("✅ CLEANUP COMPLETE. Bye!")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        try:
            asyncio.run(run_bot())
        except KeyboardInterrupt:
            pass
    else:
        asyncio.run(run_bot())
