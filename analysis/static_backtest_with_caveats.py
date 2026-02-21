import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

# --- CONFIGURATION (The "Old Way" + Caveats) ---
BACKTEST_START = "2026-02-10 06:59:04"
BACKTEST_END = "2026-02-11 06:59:04"

# Set your fixed thresholds here
ENTRY_THRESHOLD = -1.0  # Buy spot when entry premium is <= -1.0%
EXIT_THRESHOLD = 0.5  # Sell spot when exit premium is >= 0.5%

MAX_HOLD_MINUTES = 240  # 4-Hour Time Stop
BATCH_SIZE_USD = 100.0
MAX_POS_PER_EXCHANGE = 10
GLOBAL_MAX_POS = 20
FEE_RATE = 0.0015


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def run_static_backtest():
    print(f"🚀 Starting Static Backtest with Caveats...")
    print(
        f"📌 Settings: Entry {ENTRY_THRESHOLD}%, Exit {EXIT_THRESHOLD}%, Max {GLOBAL_MAX_POS} positions"
    )

    engine = get_db_engine()
    query = f"SELECT * FROM strategy_collector WHERE timestamp BETWEEN '{BACKTEST_START}' AND '{BACKTEST_END}' ORDER BY timestamp ASC"
    df = pd.read_sql(query, engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    trades = []
    active_positions = {}
    exchange_counts = {"UPBIT": 0, "BITHUMB": 0}

    for row in df.itertuples():
        key = (row.symbol, row.spot_exchange)

        # LOGIC: EXIT
        if key in active_positions:
            entry_data = active_positions[key]
            current_profit = (
                (row.exit_premium_pct - entry_data["entry_prem"]) / 100
            ) - FEE_RATE
            hold_time = (row.timestamp - entry_data["time"]).total_seconds() / 60

            # EXIT Condition: Hit Static Target OR Time Stop
            if row.exit_premium_pct >= EXIT_THRESHOLD or hold_time >= MAX_HOLD_MINUTES:
                trades.append(
                    {
                        "pnl_usd": BATCH_SIZE_USD * current_profit,
                        "exchange": row.spot_exchange,
                        "symbol": row.symbol,
                        "hold_min": hold_time,
                    }
                )
                exchange_counts[row.spot_exchange] -= 1
                del active_positions[key]

        # LOGIC: ENTRY
        elif row.entry_premium_pct <= ENTRY_THRESHOLD:
            if exchange_counts[row.spot_exchange] < MAX_POS_PER_EXCHANGE:
                if len(active_positions) < GLOBAL_MAX_POS:
                    active_positions[key] = {
                        "entry_prem": row.entry_premium_pct,
                        "time": row.timestamp,
                    }
                    exchange_counts[row.spot_exchange] += 1

    if not trades:
        print("📭 No trades were triggered. Try lowering the ENTRY_THRESHOLD.")
        return

    results = pd.DataFrame(trades)
    print("-" * 50)
    print(f"💰 Total Net Profit: ${results['pnl_usd'].sum():.2f}")
    print(f"📊 Total Trades:     {len(results)}")
    print(f"📈 Win Rate:         {(results['pnl_usd'] > 0).mean():.2%}")
    print(f"⏱️ Avg Hold Time:    {results['hold_min'].mean():.1f} min")
    print("-" * 50)


if __name__ == "__main__":
    run_static_backtest()
