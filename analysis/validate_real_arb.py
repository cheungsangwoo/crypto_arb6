import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def validate_execution():
    engine = get_db_engine()

    # Load completed trades from positions table
    query = "SELECT * FROM positions WHERE status IN ('CLOSED', 'OPEN')"
    df = pd.read_sql(query, engine)

    if df.empty:
        print("❌ No trade history found in 'positions' table to validate.")
        return

    print("📊 ANALYZING EXECUTION ACCURACY 📊")
    print("-" * 50)

    # 1. Entry Slippage (Target vs Actual)
    # entry_premium = Scanning price, entry_premium_pct = Actual Fill
    df["entry_slippage"] = df["entry_premium"] - df["entry_premium_pct"]
    avg_entry_slip = df["entry_slippage"].mean()

    # 2. Exit Slippage (If closed)
    closed_trades = df[df["status"] == "CLOSED"].copy()
    if not closed_trades.empty:
        # Note: Your model records exit_premium_pct upon closing
        avg_exit_slip = (
            closed_trades["exit_premium"] - closed_trades["exit_premium_pct"]
        ).mean()
    else:
        avg_exit_slip = 0.0

    # 3. Latency Impact
    avg_latency = df["fill_latency_seconds"].mean()

    print(f"✅ Total Trades Analyzed: {len(df)}")
    print(f"🕒 Avg Fill Latency:      {avg_latency:.2f}s")
    print(f"📉 Avg Entry Slippage:    {avg_entry_slip:.4f}%")
    print(f"📉 Avg Exit Slippage:     {avg_exit_slip:.4f}%")
    print("-" * 50)

    if abs(avg_entry_slip) > 0.1:
        print(
            "⚠️ WARNING: High Entry Slippage detected. Adjusting 'Real Arb' formula recommended."
        )
    else:
        print("🚀 Execution is CLEAN. Theory matches reality.")


if __name__ == "__main__":
    validate_execution()
