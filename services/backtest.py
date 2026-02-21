import pandas as pd
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Setup
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Backtest")
load_dotenv(override=True)

# --- USER CONFIGURATION (EDIT HERE) ---
# Define the thresholds you want to test
THRESHOLDS = {
    "UPBIT": {"ENTRY": -0.6, "EXIT": -0.1},
    "BITHUMB": {"ENTRY": -1.5, "EXIT": -0.5},
}

# Simulation Settings
BATCH_SIZE = 100.0  # USDT per trade
MAX_SLOTS_PER_EXCH = 10  # Max concurrent positions per exchange
FEE_RATE = 0.002  # 0.2% round trip fees
HOURS_LOOKBACK = 24  # Backtest window


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def load_data(hours=24):
    """
    Fetches raw price data and RECALCULATES premiums using vectorized math.
    Matches the V3 Optimizer logic for consistency.
    """
    logger.info(f"⏳ Loading market data (Last {hours}h)...")
    engine = get_db_engine()
    cutoff = datetime.now() - timedelta(hours=hours)

    query = text(
        """
        SELECT 
            timestamp, symbol, spot_exchange, 
            kimchi_premium_pct, 
            spot_bid_price, spot_ask_price,
            binance_bid_price, binance_ask_price,
            ref_usdt_ask
        FROM strategy_collector 
        WHERE timestamp >= :cutoff
        ORDER BY timestamp ASC
    """
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"cutoff": cutoff})

    if df.empty:
        return df

    # --- VECTORIZED RECALCULATION ---
    df["ref_usdt_ask"] = df["ref_usdt_ask"].replace(0, 1450.0).fillna(1450.0)

    # Real Entry Premium (Paying Spot Ask / Selling Future Bid)
    df["real_entry_prem"] = (
        ((df["spot_ask_price"] / df["ref_usdt_ask"]) / df["binance_bid_price"]) - 1
    ) * 100

    # Real Exit Premium (Selling Spot Bid / Buying Future Ask)
    df["real_exit_prem"] = (
        ((df["spot_bid_price"] / df["ref_usdt_ask"]) / df["binance_ask_price"]) - 1
    ) * 100

    # Fallback to logged values if raw prices missing
    df["real_entry_prem"] = df["real_entry_prem"].fillna(df["kimchi_premium_pct"])
    df["real_exit_prem"] = df["real_exit_prem"].fillna(df["kimchi_premium_pct"])

    # Cleanup infinite/NaN values
    df.replace([float("inf"), float("-inf")], float("nan"), inplace=True)
    df.dropna(subset=["real_entry_prem", "real_exit_prem"], inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def run_simulation(df, exch_name, entry_th, exit_th):
    """
    Simulates trading for a specific exchange and returns detailed stats.
    """
    # Filter for specific exchange
    df_exch = df[df["spot_exchange"] == exch_name].copy()
    if df_exch.empty:
        return 0, 0, 0.0

    active_positions = []  # List of {'symbol': str, 'entry_prem': float}

    trades_entered = 0
    trades_completed = 0
    total_pnl = 0.0

    # 1. Filter rows that trigger EITHER entry OR exit
    # This reduces iteration count significantly while maintaining chronological order
    relevant_rows = df_exch[
        (df_exch["real_entry_prem"] <= entry_th)
        | (df_exch["real_exit_prem"] >= exit_th)
    ]

    for row in relevant_rows.itertuples():
        # --- A. Check Exits (FIFO) ---
        # We iterate a copy of the list to allow modification
        for pos in active_positions[:]:
            if pos["symbol"] == row.symbol:
                if row.real_exit_prem >= exit_th:
                    # EXECUTE EXIT
                    gross_spread = row.real_exit_prem - pos["entry_prem"]
                    # PnL = Trade Size * (Spread - Fees)
                    net_spread_pct = gross_spread - (FEE_RATE * 100)
                    pnl = BATCH_SIZE * (net_spread_pct / 100.0)

                    total_pnl += pnl
                    trades_completed += 1
                    active_positions.remove(pos)

        # --- B. Check Entries ---
        if len(active_positions) < MAX_SLOTS_PER_EXCH:
            if row.real_entry_prem <= entry_th:
                # Prevent duplicate positions for same symbol (Simple Logic)
                is_held = any(p["symbol"] == row.symbol for p in active_positions)
                if not is_held:
                    # EXECUTE ENTRY
                    active_positions.append(
                        {"symbol": row.symbol, "entry_prem": row.real_entry_prem}
                    )
                    trades_entered += 1

    return trades_entered, trades_completed, total_pnl


def main():
    print(f"\n📊 STARTING BACKTEST (Last {HOURS_LOOKBACK} Hours)")
    print("=" * 60)

    # 1. Load Data
    df = load_data(hours=HOURS_LOOKBACK)
    if df.empty:
        print("❌ No data found.")
        return

    total_pnl = 0.0

    # 2. Run Simulations
    for exch, params in THRESHOLDS.items():
        print(f"\n🔵 EXCHANGE: {exch}")
        print(f"   Testing: Entry {params['ENTRY']}% | Exit {params['EXIT']}%")

        entered, completed, pnl = run_simulation(
            df, exch, params["ENTRY"], params["EXIT"]
        )

        print(f"   --------------------------------")
        print(f"   📥 Entered:    {entered}")
        print(f"   📤 Completed:  {completed}")
        print(f"   💰 Net PnL:    ${pnl:.2f}")

        total_pnl += pnl

    print("\n" + "=" * 60)
    print(f"🏆 TOTAL PROJECTED PnL: ${total_pnl:.2f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
