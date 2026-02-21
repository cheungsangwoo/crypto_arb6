import sys
import io
import asyncio
import os
import signal
import json
from datetime import datetime
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

# Services
from clients.upbit_client import UpbitClient
from clients.bithumb_client import BithumbClient
from services.strategy_scanner import StrategyScanner
from services.overnight_position_manager import PositionManager
from database.session import init_db

# Windows Console Fix
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")


def setup_logging():
    log_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    log_dir = "data/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "bot_unwind.log")  # Separate log file

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


setup_logging()
logger = logging.getLogger("UnwindMain")
load_dotenv(override=True)
init_db()

shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.warning("\n🛑 SHUTDOWN SIGNAL RECEIVED. Cleaning up...")
    shutdown_event.set()


async def background_exit_loop(manager):
    """
    Continuous background task that manages Exits (Unwinding).
    """
    logger.info("   🛡️ Unwinder (Exit Engine) Started.")
    while not shutdown_event.is_set():
        try:
            # Check Pause State (Manual Hot Reload)
            is_paused = False
            try:
                with open("bot_config.json", "r") as f:
                    config = json.load(f)
                    is_paused = config.get("SYSTEM", {}).get("PAUSED", False)
            except:
                pass

            if is_paused:
                await asyncio.sleep(5)
                continue

            # Run Exit Logic
            await manager.run_exit_logic()
            await asyncio.sleep(1.0)

        except Exception as e:
            logger.error(f"   ⚠️ Unwinder Error: {e}")
            await asyncio.sleep(5.0)


async def run_unwind_bot():
    logger.info("🔻 UNWIND BOT STARTED (Exits & Data Logging ONLY)")
    logger.info("   🚫 Sniping (Entry Logic) is DISABLED.")

    spot_clients = {}
    if os.getenv("UPBIT_ACCESS_KEY"):
        spot_clients["UPBIT"] = UpbitClient(
            "UPBIT", os.getenv("UPBIT_ACCESS_KEY"), os.getenv("UPBIT_SECRET_KEY")
        )
    if os.getenv("BITHUMB_ACCESS_KEY"):
        spot_clients["BITHUMB"] = BithumbClient(
            "BITHUMB", os.getenv("BITHUMB_ACCESS_KEY"), os.getenv("BITHUMB_SECRET_KEY")
        )

    if not spot_clients:
        logger.error("❌ No spot clients enabled.")
        return

    scanner = StrategyScanner(spot_clients)
    manager = PositionManager(
        spot_clients, shutdown_event=shutdown_event, thresholds=scanner.THRESHOLDS
    )

    # Startup Sequence
    await manager.save_portfolio_snapshot()
    last_snapshot_time = asyncio.get_event_loop().time()
    SNAPSHOT_INTERVAL = 3600

    logger.info("   🔄 Synchronizing Database with Wallet Balances...")
    await manager.sync_capacity()
    await manager.sync_positions()
    logger.info("   ✅ Sync Complete.")

    # Start Exit Engine
    exit_task = asyncio.create_task(background_exit_loop(manager))

    try:
        while not shutdown_event.is_set():
            # 1. Hot Reload Config (to update Exit Thresholds)
            try:
                with open("bot_config.json", "r") as f:
                    config = json.load(f)
                    scanner.THRESHOLDS = config
                    manager.thresholds = config
            except:
                pass

            # 2. Sync Logic
            await manager.sync_capacity()
            await manager.sync_positions()

            # 3. Snapshot
            current_time = asyncio.get_event_loop().time()
            if (current_time - last_snapshot_time) > SNAPSHOT_INTERVAL:
                logger.info("⏰ Triggering Hourly Portfolio Snapshot...")
                asyncio.create_task(manager.save_portfolio_snapshot())
                last_snapshot_time = current_time

            # 4. Scan Market (DATA LOGGING ONLY)
            # We call this to keep populating the 'strategy_collector' table for the Optimizer
            market_data = await scanner.scan()

            # --- STATUS REPORT ---
            active_count = len(manager.get_active_positions())
            if active_count == 0:
                logger.info(
                    "   ✨ No active positions. Scanning market for data history..."
                )
            else:
                logger.info(
                    f"   ⏳ Unwinding {active_count} positions. Scanning market..."
                )

            # 🚫 ENTRY LOGIC REMOVED
            # await manager.execute_sniper_logic(market_data) <--- DELETED

            await asyncio.sleep(5)

    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logger.warning("\n🧹 CLEANUP INITIATED...")
        if not exit_task.done():
            exit_task.cancel()
            try:
                await exit_task
            except:
                pass

        # Graceful Cancel of OPEN ORDERS (Exits only)
        logger.info("🛑 Cancelling pending exit orders...")
        if "UPBIT" in spot_clients:
            await spot_clients["UPBIT"].cancel_all_orders()
        if "BITHUMB" in spot_clients:
            try:
                active_pos = manager.get_active_positions()
                for p in active_pos:
                    if p.spot_exchange == "BITHUMB":
                        orders = await spot_clients["BITHUMB"].fetch_open_orders(
                            p.symbol
                        )
                        for o in orders:
                            await spot_clients["BITHUMB"].cancel_order(
                                o["id"], p.symbol
                            )
            except:
                pass

        if scanner:
            await scanner.close()
        if manager:
            logger.info("📸 Saving Final Snapshot...")
            await manager.save_portfolio_snapshot()
            await manager.close()
        for c in spot_clients.values():
            await c.close()

        logger.info("✅ UNWIND COMPLETE. Bye!")


if __name__ == "__main__":
    if sys.platform != "win32":
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run_unwind_bot())
    except KeyboardInterrupt:
        pass
