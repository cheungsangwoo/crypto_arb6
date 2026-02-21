import pandas as pd
import numpy as np
import joblib
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

# --- REVISED CONFIGURATION ---
BACKTEST_START = "2026-02-10 06:59:04"
BACKTEST_END = "2026-02-11 06:59:04"
MIN_PROFIT_THRESHOLD = 0.003  # [UPDATED] 0.3% Net Profit Floor
MAX_HOLD_MINUTES = 240  # [UPDATED] 4-Hour Time Stop
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


def run_constrained_backtest():
    print(f"🚀 Starting Defined Backtest ({BACKTEST_START} to {BACKTEST_END})")

    # 1. Load Regressor Model and Scaler
    try:
        model = joblib.load("analysis/arb_model.pkl")
        scaler = joblib.load("analysis/scaler.pkl")
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        return

    # 2. Load and Engineer Features
    engine = get_db_engine()
    query = f"SELECT * FROM strategy_collector WHERE timestamp BETWEEN '{BACKTEST_START}' AND '{BACKTEST_END}' ORDER BY timestamp ASC"
    df = pd.read_sql(query, engine)

    from analysis.strategy_optimizer_ml import engineer_advanced_features

    df_feat = engineer_advanced_features(df)

    features = [
        "entry_premium_pct",
        "funding_rate",
        "annualized_funding_pct",
        "ba_spread",
        "premium_momentum",
        "relative_premium",
        "prem_z_5",
        "prem_z_30",
        "prem_z_60",
    ]

    # 3. Predict Expected Net Profit
    X_scaled = scaler.transform(df_feat[features])
    df_feat["predicted_net_profit"] = model.predict(X_scaled)

    # 4. Simulation Loop
    trades = []
    active_positions = {}
    exchange_counts = {"UPBIT": 0, "BITHUMB": 0}

    for row in df_feat.itertuples():
        key = (row.symbol, row.spot_exchange)

        # LOGIC: EXIT
        if key in active_positions:
            entry_data = active_positions[key]
            current_profit = (
                (row.exit_premium_pct - entry_data["entry_prem"]) / 100
            ) - FEE_RATE
            hold_time = (row.timestamp - entry_data["time"]).total_seconds() / 60

            # EXIT CONDITIONS: Target met OR Time stop hit OR Safety target (0.5%) hit
            if (
                current_profit >= entry_data["target_profit"]
                or hold_time >= MAX_HOLD_MINUTES
                or row.exit_premium_pct >= 0.5
            ):
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
        elif row.predicted_net_profit >= MIN_PROFIT_THRESHOLD:
            if exchange_counts[row.spot_exchange] < MAX_POS_PER_EXCHANGE:
                if len(active_positions) < GLOBAL_MAX_POS:
                    active_positions[key] = {
                        "entry_prem": row.entry_premium_pct,
                        "target_profit": row.predicted_net_profit,
                        "time": row.timestamp,
                    }
                    exchange_counts[row.spot_exchange] += 1

    # 5. Results
    if not trades:
        print("📭 No trades met the criteria.")
        return

    results = pd.DataFrame(trades)
    print("-" * 50)
    print(f"💰 Total Net Profit: ${results['pnl_usd'].sum():.2f}")
    print(f"📊 Total Trades:     {len(results)}")
    print(f"📈 Win Rate:         {(results['pnl_usd'] > 0).mean():.2%}")
    print(f"⏱️ Avg Hold Time:    {results['hold_min'].mean():.1f} min")
    print("-" * 50)


if __name__ == "__main__":
    run_constrained_backtest()
