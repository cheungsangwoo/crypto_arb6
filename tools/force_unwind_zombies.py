import asyncio
import os
import ccxt.async_support as ccxt
from database.session import SessionLocal
from database.models import Position
from services.hedger import SYMBOL_MAP
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ZombieHunter")


async def recover_zombies():
    # 1. Initialize Binance
    binance = ccxt.binance(
        {
            "apiKey": os.getenv("BINANCE_HEDGE_API_KEY"),
            "secret": os.getenv("BINANCE_HEDGE_SECRET_KEY"),
            "options": {"defaultType": "future"},
        }
    )

    try:
        logger.info("🔍 Scanning Binance for active short positions...")
        b_positions = await binance.fetch_positions()
        # Filter for active short positions (contracts > 0)
        active_shorts = [p for p in b_positions if float(p.get("contracts", 0)) > 0]

        if not active_shorts:
            logger.info("✅ No active positions found on Binance.")
            return

        with SessionLocal() as db:
            for s_pos in active_shorts:
                symbol = s_pos["symbol"].split(":")[0] + ":USDT"
                qty = float(s_pos["contracts"])

                # Strip 'USDT' to get the base asset
                base_asset = symbol.replace("USDT", "")

                # Check if this exists in our DB as an OPEN position
                db_pos = (
                    db.query(Position)
                    .filter(Position.symbol == base_asset, Position.status == "OPEN")
                    .first()
                )

                if not db_pos:
                    logger.warning(
                        f"🚨 ZOMBIE DETECTED: {symbol} has {qty} shorted, but no OPEN record in DB."
                    )

                    # ACTION: Close the position
                    try:
                        logger.info(f"📉 Closing Zombie Hedge: {symbol} (Qty: {qty})")
                        await binance.create_market_buy_order(
                            symbol=f"{base_asset}/USDT",
                            amount=qty,
                            params={"reduceOnly": True, "positionSide": "SHORT"},
                        )
                        logger.info(f"✅ Successfully closed {symbol}")
                    except Exception as e:
                        logger.error(f"❌ Failed to close {symbol}: {e}")
                else:
                    logger.info(
                        f"🟢 Position Healthy: {symbol} matches DB ID {db_pos.id}"
                    )

    finally:
        await binance.close()


if __name__ == "__main__":
    asyncio.run(recover_zombies())
