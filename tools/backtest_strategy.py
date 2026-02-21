import pandas as pd
import numpy as np
import os
import sys
import random
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Optimizer")
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv(override=True)

# --- CONFIGURATION ---
# [UPDATED] Matches your current pilot settings
INITIAL_CAPITAL = 2000.0  # Assumed capital for the test
BATCH_SIZE = 100.0  # Updated to $100 per trade
MAX_POSITIONS = int(INITIAL_CAPITAL / BATCH_SIZE)
FILL_PROBABILITY = 0.20  # Conservative fill rate estimate

# Fees
FEE_MAKER_UPBIT = 0.0005  # 0.05%
FEE_TAKER_BINANCE = 0.0005  # 0.05%

# [CRITICAL] Blacklist broken coins that cause fake 10,000% profits
BLACKLIST = ["HOLO", "SNT", "WAVES", "NUC", "GXC"]


def get_db_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def load_data():
    print("📥 Loading market data...")
    engine = get_db_engine()

    # 1. Fetch Data
    query = """
        SELECT timestamp, symbol, 
               upbit_bid_price, upbit_ask_price,
               binance_bid_price, binance_mark_price, 
               funding_rate, kimchi_premium_pct, ref_usdt_ask
        FROM strategy_collector
        WHERE kimchi_premium_pct BETWEEN -15.0 AND 5.0 -- Strict range filter
          AND binance_mark_price > 0 
          AND upbit_bid_price > 0
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(text(query), engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 2. Apply Blacklist
    original_len = len(df)
    df = df[~df["symbol"].isin(BLACKLIST)]
    filtered_len = len(df)

    print(f"🧹 Filtered {original_len - filtered_len} rows (Blacklisted: {BLACKLIST})")
    print(f"✅ Loaded {filtered_len} clean rows.")
    return df


class StatefulSimulation:
    def __init__(self, entry_threshold, exit_threshold):
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold

        # We track "Slots" instead of raw dollars to ensure we never exceed 10 positions
        self.open_slots = MAX_POSITIONS
        self.open_positions = {}
        self.trade_log = []
        self.total_profit = 0.0

    def run(self, df):
        grouped = df.groupby("timestamp")

        for ts, market_snapshot in grouped:
            self._process_tick(ts, market_snapshot)

        self._finalize(df)
        return self._calculate_results()

    def _process_tick(self, ts, df_snapshot):
        current_prices = df_snapshot.set_index("symbol").to_dict("index")

        # --- A. EXIT LOGIC ---
        for symbol in list(self.open_positions.keys()):
            pos = self.open_positions[symbol]
            if symbol not in current_prices:
                continue

            row = current_prices[symbol]
            current_premium = row["kimchi_premium_pct"]

            # Exit if spread has normalized (or flipped positive)
            if current_premium >= self.exit_threshold:
                self._close_trade(symbol, pos, row, "TARGET")

        # --- B. ENTRY LOGIC ---
        # Sort by best discount
        candidates = df_snapshot[
            (df_snapshot["kimchi_premium_pct"] <= self.entry_threshold)
            & (df_snapshot["funding_rate"] > -0.0005)
        ].sort_values("kimchi_premium_pct", ascending=True)

        for _, row in candidates.iterrows():
            symbol = row["symbol"]

            if symbol in self.open_positions:
                continue

            # [GUARDRAIL] Strict Position Limit
            if self.open_slots <= 0:
                break

            if random.random() > FILL_PROBABILITY:
                continue

            self._open_trade(symbol, row)

    def _open_trade(self, symbol, row):
        self.open_slots -= 1

        # Calculate Fees (USDT)
        # Upbit (0.05%) + Binance (0.05%) = 0.1% total entry cost
        entry_cost = BATCH_SIZE * (FEE_MAKER_UPBIT + FEE_TAKER_BINANCE)

        self.open_positions[symbol] = {
            "entry_time": row["timestamp"],
            "entry_premium": row["kimchi_premium_pct"],
            "entry_cost": entry_cost,
            "size": BATCH_SIZE,
        }

    def _close_trade(self, symbol, pos, row, reason):
        self.open_slots += 1

        # PnL = Notional * (Exit_Prem - Entry_Prem)
        # Note: Divide by 100 because premiums are in % (e.g. -5.0)
        spread_pnl = pos["size"] * (
            (row["kimchi_premium_pct"] - pos["entry_premium"]) / 100.0
        )

        # Funding PnL
        hours = (row["timestamp"] - pos["entry_time"]).total_seconds() / 3600
        funding_pnl = (hours / 8.0) * row["funding_rate"] * pos["size"]

        # Exit Fees
        exit_cost = pos["size"] * (FEE_MAKER_UPBIT + FEE_TAKER_BINANCE)

        net_profit = spread_pnl + funding_pnl - pos["entry_cost"] - exit_cost

        # [GUARDRAIL] Cap crazy profits (Data errors usually result in > 20% inst-gain)
        if net_profit > (pos["size"] * 0.20):
            net_profit = 0.0  # Discard anomaly result

        self.total_profit += net_profit

        self.trade_log.append(
            {
                "symbol": symbol,
                "entry_time": pos["entry_time"],
                "exit_time": row["timestamp"],
                "net_profit": round(net_profit, 2),
                "entry_prem": pos["entry_premium"],
                "exit_prem": row["kimchi_premium_pct"],
                "reason": reason,
            }
        )
        del self.open_positions[symbol]

    def _finalize(self, df):
        # Force close remaining
        last_prices = df.groupby("symbol").last().to_dict("index")
        for symbol in list(self.open_positions.keys()):
            if symbol in last_prices:
                row = last_prices[symbol]
                # Adjust time to be "Now"
                row["timestamp"] = df["timestamp"].max()
                self._close_trade(symbol, self.open_positions[symbol], row, "STUCK")

    def _calculate_results(self):
        return {
            "profit": self.total_profit,
            "trades": len(self.trade_log),
            "stuck": len([t for t in self.trade_log if t["reason"] == "STUCK"]),
            "log": self.trade_log,
        }


def run_grid_search(df):
    print(f"🧠 Focused Grid Search (0.1% Steps) | {len(df)} rows")
    print(f"   Max Positions: {MAX_POSITIONS} | Trade Size: ${BATCH_SIZE}")
    print("-" * 65)
    print(f"{'ENTRY':<8} | {'EXIT':<8} | {'PROFIT':<10} | {'TRADES':<8} | {'STUCK'}")
    print("-" * 65)

    # Focused Range based on previous insights
    entry_range = [np.round(x, 1) for x in np.arange(-0.2, -1.0, -0.1)]
    exit_range = [np.round(x, 1) for x in np.arange(-0.1, 0.4, 0.1)]

    best_config = None
    best_pnl = -9999.0

    for entry in entry_range:
        for exit_pct in exit_range:
            if exit_pct <= (entry + 0.2):
                continue

            random.seed(42)
            sim = StatefulSimulation(entry, exit_pct)
            res = sim.run(df)

            if res["trades"] > 0:
                print(
                    f"{entry:<8} | {exit_pct:<8} | ${res['profit']:<9.2f} | {res['trades']:<8} | {res['stuck']}"
                )

            if res["profit"] > best_pnl:
                best_pnl = res["profit"]
                best_config = {"entry": entry, "exit": exit_pct, "res": res}

    print("-" * 65)

    if best_config:
        print(
            f"\n🏆 WINNER: Entry {best_config['entry']}% -> Exit {best_config['exit']}%"
        )
        print(f"   Net Profit: ${best_config['res']['profit']:.2f}")

        # Save Log
        pd.DataFrame(best_config["res"]["log"]).to_csv(
            "optimized_trades.csv", index=False
        )
        print("   💾 Log saved to optimized_trades.csv")
    else:
        print("❌ No profitable strategy found.")


if __name__ == "__main__":
    data = load_data()
    if not data.empty:
        run_grid_search(data)
