import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from sklearn.model_selection import (
    ParameterGrid,
)  # [NEW] Using scikit-learn for optimization
from dotenv import load_dotenv

load_dotenv(override=True)

# --- CONFIGURATION ---
TRAIN_START, TRAIN_END = "2026-02-08 06:59:04", "2026-02-10 06:59:04"
TEST_START, TEST_END = "2026-02-10 06:59:04", "2026-02-11 06:59:04"

BATCH_SIZE_USD = 100.0
MAX_POS_PER_EXCHANGE = 10
GLOBAL_MAX_POS = 20
FEE_RATE = 0.0015
MAX_HOLD_MINUTES = 240  # 4-Hour Caveat


def get_db_engine():
    user, password = os.getenv("DB_USER", "root"), os.getenv("DB_PASSWORD", "")
    host, port = os.getenv("DB_HOST", "127.0.0.1"), os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def simulate_with_constraints(df, entry_thresh, exit_thresh):
    """Core simulation engine with capital constraints."""
    trades = []
    active_positions = {}
    exchange_counts = {"UPBIT": 0, "BITHUMB": 0}

    for row in df.itertuples():
        key = (row.symbol, row.spot_exchange)
        if key in active_positions:
            entry_data = active_positions[key]
            current_profit = (
                (row.exit_premium_pct - entry_data["entry_prem"]) / 100
            ) - FEE_RATE
            hold_time = (row.timestamp - entry_data["time"]).total_seconds() / 60

            if row.exit_premium_pct >= exit_thresh or hold_time >= MAX_HOLD_MINUTES:
                trades.append(BATCH_SIZE_USD * current_profit)
                exchange_counts[row.spot_exchange] -= 1
                del active_positions[key]
        elif row.entry_premium_pct <= entry_thresh:
            if (
                exchange_counts[row.spot_exchange] < MAX_POS_PER_EXCHANGE
                and len(active_positions) < GLOBAL_MAX_POS
            ):
                active_positions[key] = {
                    "entry_prem": row.entry_premium_pct,
                    "time": row.timestamp,
                }
                exchange_counts[row.spot_exchange] += 1
    return sum(trades) if trades else -1e9  # Penalty for no trades


def run_optimization():
    print("⏳ Loading 72h data universe...")
    engine = get_db_engine()
    df = pd.read_sql("SELECT * FROM strategy_collector ORDER BY timestamp ASC", engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 1. Split Data
    df_train = df[(df["timestamp"] >= TRAIN_START) & (df["timestamp"] < TRAIN_END)]
    df_test = df[(df["timestamp"] >= TEST_START) & (df["timestamp"] < TEST_END)]

    # 2. Define Search Space using scikit-learn's ParameterGrid
    param_grid = ParameterGrid(
        {
            "entry": [-0.3, -0.5, -0.7, -1.0, -1.3, -1.5, -2.0],
            "exit": [0.0, 0.3, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0],
        }
    )

    # 3. Optimize on Training Data
    best_params = {"UPBIT": None, "BITHUMB": None}
    best_pnl = {"UPBIT": -1e9, "BITHUMB": -1e9}

    for exch in ["UPBIT", "BITHUMB"]:
        print(f"🔎 Optimizing {exch} on 48h training set...")
        df_train_exch = df_train[df_train["spot_exchange"] == exch]

        for params in param_grid:
            pnl = simulate_with_constraints(
                df_train_exch, params["entry"], params["exit"]
            )
            if pnl > best_pnl[exch]:
                best_pnl[exch] = pnl
                best_params[exch] = params

    # 4. Out-of-Sample Validation
    print("\n🏆 OPTIMAL PARAMETERS FOUND (48h Training):")
    for exch in ["UPBIT", "BITHUMB"]:
        print(
            f"   {exch}: Entry {best_params[exch]['entry']}% | Exit {best_params[exch]['exit']}% (Train PnL: ${best_pnl[exch]:.2f})"
        )

    print("\n🧪 Validating on 24h Out-of-Sample Test Set...")
    total_test_pnl = 0
    for exch in ["UPBIT", "BITHUMB"]:
        df_test_exch = df_test[df_test["spot_exchange"] == exch]
        test_pnl = simulate_with_constraints(
            df_test_exch, best_params[exch]["entry"], best_params[exch]["exit"]
        )
        print(f"   {exch} Test PnL: ${test_pnl:.2f}")
        total_test_pnl += test_pnl

    print(f"\n💰 Total Projected 24h PnL: ${total_test_pnl:.2f}")


if __name__ == "__main__":
    run_optimization()
