import pandas as pd
import itertools
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("FuturesOptimizer")

# Load Env
load_dotenv(override=True)

# Configuration
CAPITAL_LIMIT = 2000.0  # Max Capital
BATCH_SIZE = 100.0  # Position Size per Trade
MAX_SLOTS = int(CAPITAL_LIMIT / BATCH_SIZE)
FEE_RATE = 0.0005  # 0.05% Taker Fee


def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def load_data():
    """
    Loads data and RECONSTRUCTS missing Binance prices from Spot/FX/Premium.
    """
    logger.info("⏳ Loading historical data...")
    engine = get_db_engine()

    # 1. Fetch Columns needed for Reconstruction
    query = text(
        """
        SELECT 
            timestamp, symbol, spot_exchange, kimchi_premium_pct,
            spot_ask_price, implied_fx, binance_bid_price
        FROM strategy_collector 
        ORDER BY timestamp ASC
    """
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    logger.info(f"✅ Loaded {len(df)} rows.")

    # 2. [CRITICAL FIX] Data Reconstruction
    # If binance_bid_price is 0, calculate it: Binance = (Spot / FX) / (1 + Premium/100)

    def recover_price(row):
        price = row["binance_bid_price"]
        if price > 0:
            return price

        # Reconstruction Logic
        try:
            spot_krw = row["spot_ask_price"]
            fx = row["implied_fx"]
            prem = row["kimchi_premium_pct"]

            if spot_krw > 0 and fx > 0:
                # Formula: Binance = SpotUSD / (1 + Prem%)
                spot_usd = spot_krw / fx
                derived_price = spot_usd / (1 + (prem / 100.0))
                return derived_price
        except:
            return 0.0
        return 0.0

    # Apply reconstruction efficiently
    logger.info("🔧 Reconstructing missing Binance prices from Spot & Premium data...")
    df["price"] = df.apply(recover_price, axis=1)

    # 3. Final Cleanup
    initial_count = len(df)
    df = df[df["price"] > 0].copy()
    dropped = initial_count - len(df)

    if dropped > 0:
        logger.warning(f"⚠️ Dropped {dropped} rows where price could not be recovered.")
    else:
        logger.info("✅ 100% of data successfully recovered.")

    return df


class FuturesPosition:
    def __init__(self, symbol, exchange, entry_price, side, time):
        self.symbol = symbol
        self.exchange = exchange
        self.entry_price = float(entry_price)
        self.side = side
        self.entry_time = time


def simulate_strategy(df, u_entry, u_exit, b_entry, b_exit):
    active_positions = {}
    total_pnl = 0.0
    trade_count = 0
    used_slots = 0

    for _, row in df.iterrows():
        prem = row["kimchi_premium_pct"]
        exch = row["spot_exchange"]
        sym = row["symbol"]
        price = row["price"]
        key = (sym, exch)

        # Thresholds
        thresh_short = u_entry if exch == "UPBIT" else b_entry
        thresh_long = u_exit if exch == "UPBIT" else b_exit

        # 1. SIGNAL: SHORT (Premium Low -> Spot Cheap / Future Expensive?? No.)
        # Logic Check:
        # If Prem is -5% (Low), Spot is Cheap. Normal Arb = Buy Spot / Short Future.
        # But here we are purely directional trading on Futures.
        # Signal Hypothesis: Low Premium means "Oversold in Spot, Futures will drop to match"?
        # OR "Spot will bounce, pulling Futures up"?
        # Your prompt said: "enter short futures... when entry premium hits a certain level (e.g. -0.5%)"
        # So we stick to your rule: Low Prem = Short Signal.

        if prem <= thresh_short:
            # Close LONG
            if key in active_positions and active_positions[key].side == "LONG":
                pos = active_positions[key]
                # PnL Long: (Exit - Entry)
                raw_pnl = (price - pos.entry_price) * (BATCH_SIZE / pos.entry_price)
                net_pnl = raw_pnl - (BATCH_SIZE * FEE_RATE * 2)
                total_pnl += net_pnl
                trade_count += 1
                del active_positions[key]
                used_slots -= 1

            # Open SHORT
            if key not in active_positions and used_slots < MAX_SLOTS:
                active_positions[key] = FuturesPosition(
                    sym, exch, price, "SHORT", row["timestamp"]
                )
                used_slots += 1

        # 2. SIGNAL: LONG (Premium High)
        elif prem >= thresh_long:
            # Close SHORT
            if key in active_positions and active_positions[key].side == "SHORT":
                pos = active_positions[key]
                # PnL Short: (Entry - Exit)
                raw_pnl = (pos.entry_price - price) * (BATCH_SIZE / pos.entry_price)
                net_pnl = raw_pnl - (BATCH_SIZE * FEE_RATE * 2)
                total_pnl += net_pnl
                trade_count += 1
                del active_positions[key]
                used_slots -= 1

            # Open LONG
            if key not in active_positions and used_slots < MAX_SLOTS:
                active_positions[key] = FuturesPosition(
                    sym, exch, price, "LONG", row["timestamp"]
                )
                used_slots += 1

    return total_pnl, trade_count


def run_optimization():
    df = load_data()
    if df.empty:
        return

    # --- PARAMETER GRID ---
    upbit_shorts = [-0.5, -1.0, -2.0]
    upbit_longs = [0.5, 1.5, 2.5]
    bithumb_shorts = [-1.0, -1.5, -3.0]
    bithumb_longs = [1.0, 2.0, 3.5]

    combinations = list(
        itertools.product(upbit_shorts, upbit_longs, bithumb_shorts, bithumb_longs)
    )
    print(
        f"\n🚀 Simulating {len(combinations)} Strategies with ${CAPITAL_LIMIT} Capital..."
    )

    results = []

    for i, (us, ul, bs, bl) in enumerate(combinations):
        pnl, trades = simulate_strategy(df, us, ul, bs, bl)
        results.append(
            {
                "Config": f"U:{us}/{ul} B:{bs}/{bl}",
                "Net_PnL": pnl,
                "Trades": trades,
                "ROI": (pnl / CAPITAL_LIMIT) * 100,
            }
        )
        if i % 10 == 0:
            print(f"   Processed {i+1}...")

    sorted_results = sorted(results, key=lambda x: x["Net_PnL"], reverse=True)

    print("\n🏆 TOP 5 FUTURES STRATEGIES 🏆")
    print("=" * 80)
    print(
        f"{'Config (Short/Long Thresholds)':<35} | {'Net PnL':<10} | {'ROI':<8} | {'Trades':<6}"
    )
    print("-" * 80)

    for r in sorted_results[:5]:
        print(
            f"{r['Config']:<35} | ${r['Net_PnL']:<9.2f} | {r['ROI']:<7.2f}% | {r['Trades']:<6}"
        )


if __name__ == "__main__":
    run_optimization()
