import sys
import os
import logging

# Add project root to path
sys.path.append(os.getcwd())

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from database.models import Position, TradeCircuitTemp, Base
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Backfill")

load_dotenv(override=True)


def get_db_session():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def backfill():
    session, engine = get_db_session()

    # 1. Ensure Table Exists
    # (In case it wasn't created yet, we create it based on the model definition)
    Base.metadata.create_all(engine)

    # 2. Fetch All Existing Positions
    positions = session.query(Position).all()
    logger.info(f"🔍 Found {len(positions)} positions to migrate.")

    migrated_count = 0

    for pos in positions:
        try:
            # Check if already migrated (deduplication by entry_time + symbol)
            exists = (
                session.query(TradeCircuitTemp)
                .filter(
                    TradeCircuitTemp.symbol == pos.symbol,
                    TradeCircuitTemp.entry_time == pos.entry_time,
                )
                .first()
            )

            if exists:
                continue

            # --- A. RECONSTRUCT ENTRY DATA ---
            # 1. Derive FX Rate: KRW Amount / USDT Cost
            entry_fx = 1450.0  # Default fallback
            if pos.entry_krw_amount and pos.entry_cost_usdt and pos.entry_cost_usdt > 0:
                entry_fx = pos.entry_krw_amount / pos.entry_cost_usdt

            # 2. Derive Spot Price: KRW Amount / Spot Qty
            entry_spot_price = 0.0
            if pos.spot_qty and pos.spot_qty > 0 and pos.entry_krw_amount:
                entry_spot_price = pos.entry_krw_amount / pos.spot_qty

            # 3. Derive Hedge Price from Premium
            # Formula: Hedge = (Spot / FX) / (1 + Premium%)
            entry_hedge_price = 0.0
            if entry_spot_price > 0 and pos.entry_premium is not None:
                spot_usdt = entry_spot_price / entry_fx
                entry_hedge_price = spot_usdt / (1 + (pos.entry_premium / 100.0))

            # 4. Hedge Value
            entry_hedge_val = (
                (pos.binance_qty * entry_hedge_price) if pos.binance_qty else 0.0
            )

            # --- B. PREPARE CIRCUIT OBJECT ---
            circuit = TradeCircuitTemp(
                symbol=pos.symbol,
                exchange=pos.spot_exchange,
                status=pos.status,
                # Entry Data (High Confidence)
                entry_time=pos.entry_time,
                entry_spot_qty=pos.spot_qty,
                entry_spot_price=entry_spot_price,
                entry_spot_amount_krw=pos.entry_krw_amount,
                entry_hedge_qty=pos.binance_qty,
                entry_hedge_price=entry_hedge_price,
                entry_hedge_amount_usdt=entry_hedge_val,
                # Audit
                entry_usdt_rate=entry_fx,
                calc_entry_premium=pos.entry_premium,
                config_entry_threshold=0.0,  # Unknown history, default to 0
                # Exit Data (Partial / Null for Manual Fill)
                exit_time=pos.exit_time,
                calc_exit_premium=pos.exit_premium,
                # Integrity (Placeholders for Manual Entry)
                entry_spot_order_id="BACKFILL",
                entry_hedge_order_id="BACKFILL",
            )

            # --- C. EXIT HANDLING ---
            # If status is closed, we leave price fields NULL for you to fill
            # via CSV or manual lookup, rather than guessing wrong numbers.

            session.add(circuit)
            migrated_count += 1

        except Exception as e:
            logger.error(f"❌ Error migrating {pos.symbol} (ID: {pos.id}): {e}")
            continue

    session.commit()
    logger.info(
        f"✅ Successfully backfilled {migrated_count} records into trade_circuits_temp."
    )
    session.close()


if __name__ == "__main__":
    backfill()
