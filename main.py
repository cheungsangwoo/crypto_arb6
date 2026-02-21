import sys
import io
import asyncio
import os
import signal
import json
import logging
import threading
from datetime import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

# Services
from clients.upbit_client import UpbitClient
from clients.bithumb_client import BithumbClient
from clients.coinone_client import CoinoneClient
from services.strategy_scanner import StrategyScanner
from services.position_manager import PositionManager

# from services.optimizer import run_optimizer
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

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)


setup_logging()
logger = logging.getLogger("Main")
load_dotenv(override=True)
init_db()

# Global Shutdown Event
shutdown_event = asyncio.Event()
optimizer_stop_event = threading.Event()


def signal_handler(sig, frame):
    logger.warning("\n🛑 SHUTDOWN SIGNAL RECEIVED. Cleaning up...")
    shutdown_event.set()
    optimizer_stop_event.set()


async def background_exit_loop(manager):
    """
    Continuous background task that manages Exits (Unwinding).
    It runs independently of the Sniper loop.
    """
    logger.info("   🛡️ Unwinder (Exit Engine) Started.")
    while not shutdown_event.is_set():
        try:
            # Check if system is paused (Manual Hot Reload for Safety)
            is_paused = False
            try:
                with open("bot_config.json", "r") as f:
                    config = json.load(f)
                    is_paused = config.get("SYSTEM", {}).get("PAUSED", False)
            except:
                pass

            if is_paused:
                await asyncio.sleep(10)
                continue

            # Run the parallel exit logic (Limit Sell -> Wait -> Hedge)
            await manager.run_exit_logic()

            # Sleep briefly to avoid hammering APIs
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.error(f"   ⚠️ Unwinder Error: {e}")
            await asyncio.sleep(5.0)


async def run_bot():
    logger.info("🤖 SNIPER BOT STARTED (IOC Entry / Market Hedge)")

    spot_clients = {}

    # 1. Initialize Clients
    if os.getenv("UPBIT_ACCESS_KEY"):
        spot_clients["UPBIT"] = UpbitClient(
            "UPBIT", os.getenv("UPBIT_ACCESS_KEY"), os.getenv("UPBIT_SECRET_KEY")
        )
        logger.info("   ✅ Upbit Enabled")

    if os.getenv("BITHUMB_ACCESS_KEY"):
        spot_clients["BITHUMB"] = BithumbClient(
            "BITHUMB", os.getenv("BITHUMB_ACCESS_KEY"), os.getenv("BITHUMB_SECRET_KEY")
        )
        logger.info("   ✅ Bithumb Enabled")

    if os.getenv("COINONE_ACCESS_KEY"):
        spot_clients["COINONE"] = CoinoneClient(
            "COINONE", os.getenv("COINONE_ACCESS_KEY"), os.getenv("COINONE_SECRET_KEY")
        )
        logger.info("   ✅ Coinone Enabled")

    if not spot_clients:
        logger.error("❌ No spot clients enabled. Check .env file.")
        return

    # 2. Initialize Services
    scanner = StrategyScanner(spot_clients)
    manager = PositionManager(
        spot_clients, shutdown_event=shutdown_event, thresholds=scanner.THRESHOLDS
    )

    # --- [FIX] INITIALIZE TIME SYNC & FX RATE ---
    logger.info("   🔄 Initializing Time Sync & Market Data...")
    await manager.binance.load_time_difference()  # Syncs local time with Binance to prevent -1021
    market_data, current_fx = await scanner.scan()
    if current_fx > 0:
        manager.shared_fx_rate = current_fx

    # --- SNAPSHOT ON STARTUP ---
    await manager.save_portfolio_snapshot()
    last_snapshot_time = asyncio.get_event_loop().time()
    SNAPSHOT_INTERVAL = 3600  # 1 Hour (in seconds)

    # --- SYNC FIRST, THEN START ENGINE ---
    logger.info("   🔄 Synchronizing Database with Wallet Balances...")
    await manager.sync_capacity()  # Update slots
    await manager.sync_positions()  # Update position quantities
    logger.info("   ✅ Sync Complete.")

    # 3. Start Background Unwinder
    exit_task = asyncio.create_task(background_exit_loop(manager))

    # 4. Start Background Optimizer (Parallel Thread)
    # This runs ONCE at startup to tune the bot for the current market session
    # logger.info("   🧠 Starting Optimizer in Background Thread...")
    # asyncio.create_task(asyncio.to_thread(run_optimizer, optimizer_stop_event))

    # State tracking for Pause Mode
    was_paused = False

    # 5. Main Sniper Loop
    try:
        while not shutdown_event.is_set():
            try:
                loop_start_time = asyncio.get_event_loop().time()
                # --- ROBUST PAUSE LOGIC ---
                # 1. Manual Config Reload (Essential to detect "Resume" command)
                try:
                    with open("bot_config.json", "r") as f:
                        config = json.load(f)
                        # Update scanner thresholds so it sees the change
                        scanner.THRESHOLDS = config
                        manager.thresholds = config
                except Exception:
                    pass  # Use existing config if file read fails

                is_paused = scanner.THRESHOLDS.get("SYSTEM", {}).get("PAUSED", False)

                if is_paused:
                    if not was_paused:
                        logger.warning(
                            "⏸️ PAUSE COMMAND RECEIVED. Halting & Cancelling ALL Orders..."
                        )

                        # A. Cancel Upbit
                        if "UPBIT" in spot_clients:
                            await spot_clients["UPBIT"].cancel_all_orders()

                        # B. Cancel Bithumb (Iterate active positions)
                        if "BITHUMB" in spot_clients:
                            active_pos = manager.get_active_positions()
                            for p in active_pos:
                                if p.exchange == "BITHUMB":
                                    orders = await spot_clients[
                                        "BITHUMB"
                                    ].fetch_open_orders(p.symbol)
                                    for o in orders:
                                        await spot_clients["BITHUMB"].cancel_order(
                                            o["id"], p.symbol
                                        )
                                        logger.info(
                                            f"   🗑️ Paused: Cancelled {p.symbol}"
                                        )

                        # C. Cancel Coinone (Iterate active positions)
                        if "COINONE" in spot_clients:
                            active_pos = manager.get_active_positions()
                            for p in active_pos:
                                if p.exchange == "COINONE":
                                    orders = await spot_clients[
                                        "COINONE"
                                    ].fetch_open_orders(p.symbol)
                                    for o in orders:
                                        await spot_clients["COINONE"].cancel_order(
                                            o["id"], p.symbol
                                        )
                                        logger.info(
                                            f"   🗑️ Paused: Cancelled {p.symbol}"
                                        )

                        logger.info(
                            "   💤 Bot is now in WAIT MODE. Sleeping until resumed..."
                        )
                        was_paused = True

                    # Sleep and skip loop (Wait Mode)
                    await asyncio.sleep(5)
                    continue

                # Reset state if we just resumed
                if was_paused:
                    logger.info("▶️ RESUME COMMAND RECEIVED. Restarting operations.")
                    was_paused = False
                # ------------------------------------

                # --- PERIODIC SNAPSHOT ---
                current_time = asyncio.get_event_loop().time()
                if (current_time - last_snapshot_time) > SNAPSHOT_INTERVAL:
                    logger.info("⏰ Triggering Hourly Portfolio Snapshot...")
                    asyncio.create_task(manager.save_portfolio_snapshot())
                    last_snapshot_time = current_time

                # B. Scan Market
                market_data, current_fx = await scanner.scan()  # [CHANGE]

                # 3. Update Manager with the fresh rate BEFORE syncing/executing
                if current_fx > 0:
                    manager.shared_fx_rate = current_fx  # [NEW]

                # 0. Sync Capacity & Snapshot
                await manager.sync_capacity()
                await manager.sync_positions()

                # C. Execute Snipe
                if market_data:
                    await manager.execute_maker_strategy(market_data)

                # D. Execute Active Exit (New)
                await manager.run_active_exit()

                # --- 6. CYCLE SLEEP ---
                # Calculate remaining time to hit exactly 15s cycle
                elapsed = asyncio.get_event_loop().time() - loop_start_time
                sleep_time = max(1.0, 15.0 - elapsed)

                # logger.info(f"   💤 Cycle complete. Sleeping {sleep_time:.1f}s...")
                await asyncio.sleep(sleep_time)

            except Exception as e:  # <--- CATCH ALL ERRORS
                logger.error(f"❌ Cycle Error: {e}")
                await asyncio.sleep(5.0)  # Wait a bit before retrying

    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logger.warning("\n🧹 CLEANUP INITIATED...")
        optimizer_stop_event.set()

        # Cancel Background Task
        if not exit_task.done():
            exit_task.cancel()
            try:
                await exit_task
            except asyncio.CancelledError:
                pass

        # Emergency Cancellation on Shutdown - use cancel_all_orders for all exchanges
        logger.info("🛑 Cancelling all open orders...")
        try:
            if "UPBIT" in spot_clients:
                logger.info("   Cancelling Upbit orders...")
                await spot_clients["UPBIT"].cancel_all_orders()
                logger.info("   ✅ Upbit orders cancelled")
        except Exception as e:
            logger.error(f"   ❌ Upbit cancellation error: {e}")

        try:
            if "BITHUMB" in spot_clients:
                logger.info("   Cancelling Bithumb orders...")
                await spot_clients["BITHUMB"].cancel_all_orders()
                logger.info("   ✅ Bithumb orders cancelled")
        except Exception as e:
            logger.error(f"   ❌ Bithumb cancellation error: {e}")

        try:
            if "COINONE" in spot_clients:
                logger.info("   Cancelling Coinone orders...")
                await spot_clients["COINONE"].cancel_all_orders()
                logger.info("   ✅ Coinone orders cancelled")
        except Exception as e:
            logger.error(f"   ❌ Coinone cancellation error: {e}")

        # Close Sessions
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
    if sys.platform != "win32":
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        pass
