import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from joblib import Parallel, delayed
from decimal import Decimal, getcontext

# Setup
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Optimizer")
load_dotenv(override=True)

# Set Decimal Precision high enough
getcontext().prec = 6

# --- CONFIGURATION ---
BATCH_SIZE = 100.0
MAX_CONCURRENT_TRADES = 20  # Matches your PositionManager
FEE_RATE = 0.002
MIN_TRADES = 5
CONFIG_FILE = "bot_config.json"


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def cleanup_old_data(hours=72):
    try:
        logger.info(f"🧹 Cleaning up data older than {hours} hours...")
        engine = get_db_engine()
        cutoff = datetime.now() - timedelta(hours=hours)
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM strategy_collector WHERE timestamp < :cutoff"),
                {"cutoff": cutoff},
            )
    except Exception as e:
        logger.error(f"   ⚠️ Cleanup failed: {e}")


def load_data(hours=72):
    """
    Fetches raw price data and RECALCULATES premiums using MAKER MATH.
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

    # --- VECTORIZED RECALCULATION (ACTIVE MAKER LOGIC) ---
    df["ref_usdt_ask"] = df["ref_usdt_ask"].replace(0, 1450.0).fillna(1450.0)

    # [FIX] Entry Premium (MAKER: Buying at Spot Bid)
    df["real_entry_prem"] = (
        ((df["spot_bid_price"] / df["ref_usdt_ask"]) / df["binance_bid_price"]) - 1
    ) * 100

    # [FIX] Exit Premium (MAKER: Selling at Spot Ask)
    df["real_exit_prem"] = (
        ((df["spot_ask_price"] / df["ref_usdt_ask"]) / df["binance_ask_price"]) - 1
    ) * 100

    df["real_entry_prem"] = df["real_entry_prem"].fillna(df["kimchi_premium_pct"])
    df["real_exit_prem"] = df["real_exit_prem"].fillna(df["kimchi_premium_pct"])

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=["real_entry_prem", "real_exit_prem"], inplace=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


def simulate_single_combo(df_subset, entry_th, exit_th):
    """
    Runs a single simulation on the exchange-specific dataset.
    Note: entry_th/exit_th passed here are already floats, but clean ones.
    """
    total_pnl = 0.0
    trade_count = 0
    active_positions = []

    # Use <= and >= for comparison. Pandas/Numpy handles float comparison fine
    # if the threshold is clean (e.g. -0.4).
    relevant_rows = df_subset[
        (df_subset["real_entry_prem"] <= entry_th)
        | (df_subset["real_exit_prem"] >= exit_th)
    ]

    if relevant_rows.empty:
        return {"entry": entry_th, "exit": exit_th, "pnl": 0.0, "trades": 0}

    for row in relevant_rows.itertuples():
        # A. Manage Exits (FIFO)
        for pos in active_positions[:]:
            if pos["symbol"] == row.symbol:
                if row.real_exit_prem >= exit_th:
                    gross_spread = row.real_exit_prem - pos["entry_prem"]
                    net_spread_pct = gross_spread - (FEE_RATE * 100)

                    pnl = BATCH_SIZE * (net_spread_pct / 100.0)
                    total_pnl += pnl
                    trade_count += 1
                    active_positions.remove(pos)

        # B. Manage Entries
        if len(active_positions) < MAX_CONCURRENT_TRADES:
            if row.real_entry_prem <= entry_th:
                is_held = any(p["symbol"] == row.symbol for p in active_positions)
                if not is_held:
                    active_positions.append(
                        {"symbol": row.symbol, "entry_prem": row.real_entry_prem}
                    )

    return {"entry": entry_th, "exit": exit_th, "pnl": total_pnl, "trades": trade_count}


def make_decimal_range(start, stop, step):
    """Generates a list of floats derived from precise Decimal steps."""
    current = Decimal(str(start))
    stop_d = Decimal(str(stop))
    step_d = Decimal(str(step))

    vals = []
    # Replicate np.arange behavior (exclusive of stop)
    while current < stop_d + (
        step_d / Decimal("100")
    ):  # epsilon for float-like inclusion if needed, or strict <
        # Use strict < to match np.arange intent usually,
        # but if we want to hit the top exactly, we check tolerance.
        if current >= stop_d:
            break
        vals.append(float(current))
        current += step_d
    return vals


def run_grid_search(df, exch_name, stop_event=None):
    df_exch = df[df["spot_exchange"] == exch_name]
    if df_exch.empty:
        return None

    # [UPDATED] Use Decimal Range Generator
    if exch_name == "UPBIT":
        # Entry: -1.5 to 0.1
        entry_range = make_decimal_range(-1.5, 0.15, 0.1)
        exit_range = make_decimal_range(0.5, 2.55, 0.1)

    elif exch_name == "BITHUMB":
        # Entry: -2.5 to 0.0
        entry_range = make_decimal_range(-2.5, 0.05, 0.1)
        exit_range = make_decimal_range(0.0, 3.05, 0.1)

    else:
        entry_range = make_decimal_range(-3.0, 0.1, 0.1)
        exit_range = make_decimal_range(0.0, 3.1, 0.1)

    tasks = []
    for entry in entry_range:
        for exit in exit_range:
            # Simple Logic: Exit must be higher than Entry + Buffer
            if exit > (entry + 0.3):
                tasks.append((entry, exit))

    logger.info(
        f"   🔎 Optimizing {exch_name}: Parallel testing {len(tasks)} combinations..."
    )

    results = Parallel(n_jobs=-1)(
        delayed(simulate_single_combo)(df_exch, e, x) for e, x in tasks
    )

    if stop_event and stop_event.is_set():
        logger.warning("   🛑 Optimizer Aborted.")
        return None

    return pd.DataFrame(results)


def save_to_config(best_params):
    logger.info("\n💾 Updating bot_config.json...")
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
        else:
            config = {}

        updated = False
        for exch, params in best_params.items():
            if params is not None:
                if exch not in config:
                    config[exch] = {}

                # Because we used clean floats from Decimal, these will save cleanly
                config[exch]["ENTRY"] = params["entry"]
                config[exch]["EXIT"] = params["exit"]

                logger.info(
                    f"   ✅ Set {exch}: Entry {params['entry']} | Exit {params['exit']}"
                )
                updated = True

        if updated:
            with open(CONFIG_FILE, "w") as f:
                json.dump(config, f, indent=4)
            logger.info("   🔄 Config saved.")

    except Exception as e:
        logger.error(f"   ❌ Config Update Error: {e}")


def run_optimizer(stop_event=None):
    cleanup_old_data(hours=72)
    df = load_data(hours=72)
    if df.empty:
        logger.error("❌ No data found.")
        return

    print("\n" + "=" * 60)
    print(f"🚀 OPTIMIZATION STARTING (Maker Strategy | Last 72h)")
    print("=" * 60)

    final_recommendation = {}

    for exch in ["UPBIT", "BITHUMB", "COINONE"]:
        if stop_event and stop_event.is_set():
            break
        print(f"\n🔵 EXCHANGE: {exch}")

        results = run_grid_search(df, exch, stop_event)

        if results is not None and not results.empty:
            valid_results = results[results["trades"] >= MIN_TRADES]

            if not valid_results.empty:
                best = valid_results.sort_values(by="pnl", ascending=False).iloc[0]
                print(
                    f"   👑 Best Settings: Entry {best['entry']} | Exit {best['exit']}"
                )
                print(f"      PnL: ${best['pnl']:.2f} | Trades: {int(best['trades'])}")
                final_recommendation[exch] = best
            else:
                print(f"   ❌ No settings met the >{MIN_TRADES} trades constraint.")
                fallback = results.sort_values(by="pnl", ascending=False).iloc[0]
                print(
                    f"      (Best fallback: ${fallback['pnl']:.2f} with {int(fallback['trades'])} trades)"
                )
                final_recommendation[exch] = None
        else:
            print("   ⚠️ No trading opportunities found.")
            final_recommendation[exch] = None

    if stop_event and stop_event.is_set():
        return

    valid_recs = [v for v in final_recommendation.values() if v is not None]
    if valid_recs:
        save_to_config(final_recommendation)


if __name__ == "__main__":
    run_optimizer()
