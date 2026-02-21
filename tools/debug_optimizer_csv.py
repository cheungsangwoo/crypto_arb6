import pandas as pd
import numpy as np
import random
import os
import time

# --- CONFIGURATION ---
BATCH_SIZE_USDT = 100.0
UPBIT_MAX_SLOTS = 8
BITHUMB_MAX_SLOTS = 7
GLOBAL_MAX_POSITIONS = 11
MIN_TRADE_VAL_KRW = 10000.0
SNIPE_SUCCESS_RATE = 0.20
TAKER_FEE_PCT = 0.004
GAP_TOLERANCE_SECONDS = 12 * 3600


def load_csv_data(file_path):
    print(f"📥 Loading debug data from: {file_path}")
    if not os.path.exists(file_path):
        print("❌ File not found.")
        return pd.DataFrame()

    df = pd.read_csv(file_path, na_values=["\\N", "NULL", "None"])

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

    print(f"   Rows loaded: {len(df)}")

    # Force Numeric
    cols_to_fix = [
        "spot_bid_price",
        "spot_ask_price",
        "spot_ask_size",
        "spot_bid_size",
        "ref_usdt_ask",
        "entry_premium_pct",
        "exit_premium_pct",
    ]

    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "ref_usdt_ask" in df.columns:
        df["ref_usdt_ask"] = df["ref_usdt_ask"].fillna(1450.0)

    # Create columns
    df["spot_bid"] = df["spot_bid_price"]
    df["spot_ask"] = df["spot_ask_price"]
    # Even though we ignore liquidity for logic, we map it just in case
    df["spot_ask_size"] = df["spot_ask_size"].fillna(999999)
    df["spot_bid_size"] = df["spot_bid_size"].fillna(999999)
    df["real_entry_prem"] = df["entry_premium_pct"]
    df["real_exit_prem"] = df["exit_premium_pct"]

    # Filter only totally invalid price data (Zero/Negative prices)
    df = df[(df["spot_bid"] > 0) & (df["spot_ask"] > 0)].copy()

    print(f"   Valid ticks after filter: {len(df)}")
    return df


class GridSimulator:
    def __init__(self, u_entry, u_exit, b_entry, b_exit, debug_mode=False):
        self.u_entry = u_entry
        self.u_exit = u_exit
        self.b_entry = b_entry
        self.b_exit = b_exit
        self.debug_mode = debug_mode

        self.active_positions = {}
        self.total_pnl = 0.0
        self.trade_count = 0
        self.total_hold_time = 0.0
        self.global_slots = GLOBAL_MAX_POSITIONS
        self.slots = {"UPBIT": UPBIT_MAX_SLOTS, "BITHUMB": BITHUMB_MAX_SLOTS}
        self.last_trade_time = {}
        self.COOLDOWN_SECONDS = 60

    def run(self, ticks):
        random.seed(42)
        last_row_time = None

        for row in ticks:
            exch = row.spot_exchange
            sym = row.symbol
            curr_time = row.timestamp

            # --- GAP HANDLING ---
            if (
                last_row_time
                and (curr_time - last_row_time).total_seconds() > GAP_TOLERANCE_SECONDS
            ):
                if self.active_positions:
                    if self.debug_mode:
                        print(
                            f"   ⚠️ LARGE DATA GAP ({curr_time - last_row_time}). Resetting {len(self.active_positions)} positions."
                        )
                    self.active_positions.clear()
                    self.slots = {
                        "UPBIT": UPBIT_MAX_SLOTS,
                        "BITHUMB": BITHUMB_MAX_SLOTS,
                    }
                    self.global_slots = GLOBAL_MAX_POSITIONS

            last_row_time = curr_time

            # --- EXIT LOGIC ---
            if (sym, exch) in self.active_positions:
                pos_info = self.active_positions[(sym, exch)]
                trade_size_usdt = pos_info["size"]
                threshold = self.u_exit if exch == "UPBIT" else self.b_exit

                # Check for Valid Exit Premium
                if pd.notna(row.real_exit_prem) and row.real_exit_prem >= threshold:

                    # [MODIFIED] IGNORE LIQUIDITY CHECK
                    # We assume sufficient depth for the simulation.

                    if random.random() < 0.90:
                        self._close_position(
                            sym, exch, row.real_exit_prem, trade_size_usdt, curr_time
                        )
                        self.last_trade_time[(sym, exch)] = curr_time

            # --- ENTRY LOGIC ---
            else:
                last_exit = self.last_trade_time.get((sym, exch))
                if (
                    last_exit
                    and (curr_time - last_exit).total_seconds() < self.COOLDOWN_SECONDS
                ):
                    continue

                if self.global_slots <= 0 or self.slots.get(exch, 0) <= 0:
                    continue

                threshold = self.u_entry if exch == "UPBIT" else self.b_entry

                if pd.notna(row.real_entry_prem) and row.real_entry_prem <= threshold:
                    if random.random() > SNIPE_SUCCESS_RATE:
                        continue

                    ref_fx = row.ref_usdt_ask if row.ref_usdt_ask > 0 else 1450.0
                    if ref_fx <= 0 or row.spot_ask <= 0:
                        continue

                    # [MODIFIED] Assume we can always buy full batch
                    trade_size_usdt = BATCH_SIZE_USDT

                    if (trade_size_usdt * ref_fx) >= MIN_TRADE_VAL_KRW:
                        self._open_position(
                            sym, exch, row.real_entry_prem, trade_size_usdt, curr_time
                        )

        # --- FORCE CLOSE AT END ---
        if self.active_positions:
            print(
                f"\n   🛑 FORCE CLOSING {len(self.active_positions)} POSITIONS AT END OF DATA..."
            )
            # Optional: Calculate approximate PnL for open positions if desired
            pass

        return self.total_pnl, self.trade_count, self.total_hold_time

    def _open_position(self, symbol, exch, premium, size_usdt, timestamp):
        self.active_positions[(symbol, exch)] = {
            "entry_prem": premium,
            "size": size_usdt,
            "entry_time": timestamp,
        }
        self.slots[exch] -= 1
        self.global_slots -= 1

        if self.debug_mode:
            print(
                f"   [OPEN]  {symbol} ({exch}) @ {premium:.2f}% | Size: ${size_usdt:.2f} | Time: {timestamp}"
            )

    def _close_position(self, symbol, exch, exit_prem, size_usdt, timestamp):
        pos = self.active_positions[(symbol, exch)]
        hold_seconds = (timestamp - pos["entry_time"]).total_seconds()
        self.total_hold_time += hold_seconds

        raw_pnl_pct = exit_prem - pos["entry_prem"]
        gross_profit = size_usdt * (raw_pnl_pct / 100.0)
        net_profit = gross_profit - (size_usdt * TAKER_FEE_PCT)

        self.total_pnl += net_profit
        self.slots[exch] += 1
        self.global_slots += 1
        self.trade_count += 1
        del self.active_positions[(symbol, exch)]

        if self.debug_mode:
            print(
                f"   [CLOSE] {symbol} ({exch}) @ {exit_prem:.2f}% | PnL: ${net_profit:.2f} | Held: {hold_seconds:.1f}s"
            )


def run_debug_optimization(df):
    print("🚀 Starting Debug Optimization V3 (No Liquidity Check)...")
    ticks = list(df.itertuples(index=False))

    print("\n🔎 Running Simulation with: U[-0.6/0.5] & B[-0.8/0.3]")
    sim = GridSimulator(
        u_entry=-0.6, u_exit=0.5, b_entry=-0.8, b_exit=0.3, debug_mode=True
    )

    pnl, trades, hold_time = sim.run(ticks)
    avg_hold = (hold_time / trades) if trades > 0 else 0

    print("-" * 50)
    print(f"🏁 RESULT:")
    print(f"   Total PnL:    ${pnl:.2f}")
    print(f"   Total Trades: {trades}")
    print(f"   Avg Hold:     {avg_hold:.1f} seconds")
    print("-" * 50)


if __name__ == "__main__":
    csv_path = "data/strategy_collector_LIMIT1000.csv"
    df = load_csv_data(csv_path)
    if not df.empty:
        run_debug_optimization(df)
    else:
        print("⚠️ No data found.")
