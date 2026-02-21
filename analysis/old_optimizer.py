import pandas as pd
import itertools
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("Optimizer")
load_dotenv(override=True)

# --- CONFIGURATION ---
BATCH_SIZE = 100.0  # $100 per trade
FEE_RATE = 0.002  # 0.2% round trip (Conservative)
MAX_CAPITAL_PER_EXCHANGE = 1000.0  # $1000 limit per Spot Exchange


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def load_data():
    logger.info("⏳ Loading and Recalculating historical data (Vectorized)...")
    engine = get_db_engine()

    query = text(
        """
        SELECT 
            timestamp, symbol, spot_exchange, 
            kimchi_premium_pct, 
            spot_bid_price, spot_ask_price,
            binance_bid_price, binance_ask_price,
            ref_usdt_ask
        FROM strategy_collector
        WHERE timestamp >= NOW() - INTERVAL 7 DAY 
        ORDER BY timestamp ASC
    """
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        return df

    # Vectorized Calculation
    df["ref_usdt_ask"] = df["ref_usdt_ask"].replace(0, 1450.0).fillna(1450.0)

    # Calculate Entry (Spot Ask / Future Bid)
    valid_entry = (df["spot_ask_price"] > 0) & (df["binance_bid_price"] > 0)
    df.loc[valid_entry, "real_entry_prem"] = (
        (
            (
                df.loc[valid_entry, "spot_ask_price"]
                / df.loc[valid_entry, "ref_usdt_ask"]
            )
            / df.loc[valid_entry, "binance_bid_price"]
        )
        - 1
    ) * 100

    # Calculate Exit (Spot Bid / Future Ask)
    valid_exit = (df["spot_bid_price"] > 0) & (df["binance_ask_price"] > 0)
    df.loc[valid_exit, "real_exit_prem"] = (
        (
            (df.loc[valid_exit, "spot_bid_price"] / df.loc[valid_exit, "ref_usdt_ask"])
            / df.loc[valid_exit, "binance_ask_price"]
        )
        - 1
    ) * 100

    df["real_entry_prem"] = df["real_entry_prem"].fillna(df["kimchi_premium_pct"])
    df["real_exit_prem"] = df["real_exit_prem"].fillna(df["kimchi_premium_pct"])

    logger.info(f"✅ Loaded {len(df)} rows.")
    return df


class SimulatedPosition:
    def __init__(self, entry_prem):
        self.entry_prem = entry_prem


def simulate_single_exchange(df_exchange, entry_threshold, exit_threshold):
    """
    Runs simulation for a SINGLE exchange (Upbit OR Bithumb).
    """
    active_positions = {}  # Key: Symbol
    completed_trades = 0
    total_pnl = 0.0
    current_invested = 0.0
    max_utilization = 0.0

    # Filter is done before passing df, so we just iterate
    for row in df_exchange.itertuples():
        sym = row.symbol
        entry_prem = row.real_entry_prem
        exit_prem = row.real_exit_prem

        # 1. CHECK EXITS
        if sym in active_positions:
            pos = active_positions[sym]
            if exit_prem >= exit_threshold:
                # Profit Calc
                spread_delta = exit_prem - pos.entry_prem
                gross_profit = BATCH_SIZE * (spread_delta / 100.0)
                net_profit = gross_profit - (BATCH_SIZE * FEE_RATE)

                total_pnl += net_profit
                completed_trades += 1
                current_invested -= BATCH_SIZE
                del active_positions[sym]

        # 2. CHECK ENTRIES
        else:
            if current_invested + BATCH_SIZE <= MAX_CAPITAL_PER_EXCHANGE:
                if entry_prem <= entry_threshold:
                    active_positions[sym] = SimulatedPosition(entry_prem)
                    current_invested += BATCH_SIZE
                    if current_invested > max_utilization:
                        max_utilization = current_invested

    return total_pnl, max_utilization, completed_trades


def optimize_exchange(df, exchange_name, entry_range, exit_range):
    print(f"\n🔎 Optimizing {exchange_name}...")
    
    # Pre-filter DataFrame for speed
    df_subset = df[df["spot_exchange"] == exchange_name].copy()
    
    combinations = list(itertools.product(entry_range, exit_range))
    results = []

    for i, (entry, exit) in enumerate(combinations):
        if i % 20 == 0:
            print(f"   Testing {exchange_name} config {i}/{len(combinations)}...")
            
        pnl, max_cap, trades = simulate_single_exchange(df_subset, entry, exit)
        
        results.append({
            "Entry": entry,
            "Exit": exit,
            "Net_PnL": pnl,
            "Trades": trades,
            "Max_Cap": max_cap
        })

    # Sort by PnL
    return sorted(results, key=lambda x: x["Net_PnL"], reverse=True)


def run_optimization():
    df = load_data()
    if df.empty:
        return

    # --- DEFINING RANGES ---
    # Now we can test WAY more combinations because we split the workload
    upbit_entries = [-0.1, -0.3, -0.5, -0.7, -1.0, -1.2, -1.5]
    upbit_exits = [0.3, 0.5, 0.8, 1.0, 1.2, 1.5, 2.0]
    
    bithumb_entries = [-0.5, -0.8, -1.0, -1.2, -1.5, -2.0, -2.5]
    bithumb_exits = [1.0, 1.5, 2.0, 2.5, 3.0]

    # 1. Optimize Upbit
    upbit_results = optimize_exchange(df, "UPBIT", upbit_entries, upbit_exits)
    best_upbit = upbit_results[0]

    # 2. Optimize Bithumb
    bithumb_results = optimize_exchange(df, "BITHUMB", bithumb_entries, bithumb_exits)
    best_bithumb = bithumb_results[0]

    # 3. Combine Results
    total_pnl = best_upbit["Net_PnL"] + best_bithumb["Net_PnL"]
    total_trades = best_upbit["Trades"] + best_bithumb["Trades"]

    print("\n🏆 OPTIMIZED CONFIGURATION FOUND 🏆")
    print("=" * 60)
    print(f"💰 Total Projected PnL: ${total_pnl:.2f}")
    print(f"📊 Total Trades:        {total_trades}")
    print("-" * 60)
    print(f"UPBIT SETTINGS:")
    print(f"   Entry: {best_upbit['Entry']}%")
    print(f"   Exit:  {best_upbit['Exit']}%")
    print(f"   PnL:   ${best_upbit['Net_PnL']:.2f} ({best_upbit['Trades']} trades)")
    print("-" * 60)
    print(f"BITHUMB SETTINGS:")
    print(f"   Entry: {best_bithumb['Entry']}%")
    print(f"   Exit:  {best_bithumb['Exit']}%")
    print(f"   PnL:   ${best_bithumb['Net_PnL']:.2f} ({best_bithumb['Trades']} trades)")
    print("=" * 60)

    # Save to file
    with open("optimized_params.txt", "w") as f:
        f.write(f"UPBIT_ENTRY={best_upbit['Entry']}\n")
        f.write(f"UPBIT_EXIT={best_upbit['Exit']}\n")
        f.write(f"BITHUMB_ENTRY={best_bithumb['Entry']}\n")
        f.write(f"BITHUMB_EXIT={best_bithumb['Exit']}\n")
    
    print("\n✅ Saved best parameters to optimized_params.txt")


if __name__ == "__main__":
    run_optimization()