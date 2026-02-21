import sys
import os
import logging
import ccxt.async_support as ccxt
import asyncio
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.getcwd())

from crypto_arb4.database.models import TradeCircuitTemp, Base, StrategyCollector

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SmartBackfill")

load_dotenv(override=True)

# --- CONFIG ---
# If FX lookup fails, use this default
DEFAULT_FX = 1450.0


def get_db_session():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return Session()


async def fetch_order_safe(client, symbol, order_id):
    """Robust fetch that handles different exchange quirks."""
    if not order_id or order_id == "None" or order_id == "BACKFILL":
        return None

    try:
        # Bithumb requires currency pair often
        order = await client.fetch_order(order_id, symbol)
        return order
    except Exception as e:
        logger.warning(f"   ⚠️ Could not fetch {symbol} order {order_id}: {e}")
        return None


def get_historical_fx(session, timestamp):
    """Finds the nearest FX rate in strategy_collector history."""
    try:
        # Look for a record within 5 minutes of the trade
        record = (
            session.query(StrategyCollector)
            .filter(
                StrategyCollector.timestamp >= timestamp - timedelta(minutes=5),
                StrategyCollector.timestamp <= timestamp + timedelta(minutes=5),
            )
            .first()
        )

        if record and record.ref_usdt_ask:
            return float(record.ref_usdt_ask)
    except:
        pass
    return DEFAULT_FX


async def run_backfill():
    session = get_db_session()

    # 1. Initialize Clients
    binance = ccxt.binance(
        {
            "apiKey": os.getenv("BINANCE_HEDGE_API_KEY"),
            "secret": os.getenv("BINANCE_HEDGE_SECRET_KEY"),
            "options": {"defaultType": "future"},
        }
    )

    spot_clients = {}
    if os.getenv("UPBIT_ACCESS_KEY"):
        spot_clients["UPBIT"] = ccxt.upbit(
            {
                "apiKey": os.getenv("UPBIT_ACCESS_KEY"),
                "secret": os.getenv("UPBIT_SECRET_KEY"),
            }
        )
    if os.getenv("BITHUMB_ACCESS_KEY"):
        spot_clients["BITHUMB"] = ccxt.bithumb(
            {
                "apiKey": os.getenv("BITHUMB_ACCESS_KEY"),
                "secret": os.getenv("BITHUMB_SECRET_KEY"),
            }
        )

    # 2. Find Incomplete Circuits
    # (Rows that have an Order ID but NO Price data)
    circuits = (
        session.query(TradeCircuitTemp)
        .filter(
            (TradeCircuitTemp.entry_spot_order_id.isnot(None))
            & (TradeCircuitTemp.entry_spot_price.is_(None))
        )
        .all()
    )

    logger.info(f"🔍 Found {len(circuits)} skeleton records to hydrate...")

    for c in circuits:
        logger.info(f"💧 Hydrating {c.symbol} ({c.exchange})...")

        spot_client = spot_clients.get(c.exchange)
        if not spot_client:
            logger.error(f"   ❌ No client for {c.exchange}")
            continue

        from crypto_arb4.services.hedger import SYMBOL_MAP

        # Reverse map needed? Usually SYMBOL_MAP is Binance->Spot.
        # We need Spot->Binance.
        inv_map = {v: k for k, v in SYMBOL_MAP.items()}
        b_key = inv_map.get(c.symbol, c.symbol)

        spot_sym = f"{c.symbol}/KRW"
        hedge_sym = f"{b_key}/USDT"

        # --- A. FETCH ENTRY DATA ---
        spot_entry = await fetch_order_safe(
            spot_client, spot_sym, c.entry_spot_order_id
        )
        hedge_entry = await fetch_order_safe(binance, hedge_sym, c.entry_hedge_order_id)

        if spot_entry:
            c.entry_time = datetime.fromtimestamp(spot_entry["timestamp"] / 1000)
            c.entry_spot_price = spot_entry["average"] or spot_entry["price"]
            c.entry_spot_qty = spot_entry["filled"]
            c.entry_spot_amount_krw = spot_entry["cost"]  # Qty * Price

            # Lookup FX based on this timestamp
            c.entry_usdt_rate = get_historical_fx(session, c.entry_time)

        if hedge_entry:
            c.entry_hedge_price = hedge_entry["average"] or hedge_entry["price"]
            c.entry_hedge_qty = hedge_entry["filled"]
            c.entry_hedge_amount_usdt = hedge_entry["cost"]

        # --- B. FETCH EXIT DATA (If IDs exist) ---
        if c.exit_spot_order_id:
            spot_exit = await fetch_order_safe(
                spot_client, spot_sym, c.exit_spot_order_id
            )
            if spot_exit:
                c.exit_time = datetime.fromtimestamp(spot_exit["timestamp"] / 1000)
                c.exit_spot_price = spot_exit["average"] or spot_exit["price"]
                c.exit_spot_qty = spot_exit["filled"]
                c.exit_spot_amount_krw = spot_exit["cost"]
                c.exit_usdt_rate = get_historical_fx(session, c.exit_time)
                c.status = "CLOSED"

        if c.exit_hedge_order_id:
            hedge_exit = await fetch_order_safe(
                binance, hedge_sym, c.exit_hedge_order_id
            )
            if hedge_exit:
                c.exit_hedge_price = hedge_exit["average"] or hedge_exit["price"]
                c.exit_hedge_qty = hedge_exit["filled"]
                c.exit_hedge_amount_usdt = hedge_exit["cost"]

        # --- C. CALCULATE PnL (If Closed) ---
        if c.status == "CLOSED" and c.exit_spot_amount_krw and c.entry_spot_amount_krw:
            # 1. Gross Spot PnL (KRW)
            c.gross_spot_pnl_krw = c.exit_spot_amount_krw - c.entry_spot_amount_krw

            # 2. Gross Hedge PnL (USDT) - Short Logic
            if c.entry_hedge_amount_usdt and c.exit_hedge_amount_usdt:
                # Entry Cost (Sold) - Exit Cost (Bought Back)
                c.gross_hedge_pnl_usdt = (
                    c.entry_hedge_amount_usdt - c.exit_hedge_amount_usdt
                )

            # 3. Fees (Estimate 0.15% if specific fee data missing)
            total_vol_usdt = (
                (c.entry_spot_amount_krw / c.entry_usdt_rate)
                + c.entry_hedge_amount_usdt
                + (c.exit_spot_amount_krw / c.exit_usdt_rate)
                + c.exit_hedge_amount_usdt
            )
            c.total_fees_usdt = total_vol_usdt * 0.0015

            # 4. Net PnL
            spot_pnl_usdt = c.gross_spot_pnl_krw / c.exit_usdt_rate
            c.net_pnl_usdt = spot_pnl_usdt + c.gross_hedge_pnl_usdt - c.total_fees_usdt

            # 5. Premiums
            # Entry Premium = (SpotPrice/FX) / HedgePrice - 1
            if c.entry_hedge_price:
                spot_usd = c.entry_spot_price / c.entry_usdt_rate
                c.calc_entry_premium = ((spot_usd / c.entry_hedge_price) - 1) * 100

            if c.exit_hedge_price:
                spot_usd = c.exit_spot_price / c.exit_usdt_rate
                c.calc_exit_premium = ((spot_usd / c.exit_hedge_price) - 1) * 100

        # Commit per row in case of errors
        session.commit()

    logger.info("✅ Smart Backfill Complete.")
    await binance.close()
    for client in spot_clients.values():
        await client.close()
    session.close()


if __name__ == "__main__":
    asyncio.run(run_backfill())
