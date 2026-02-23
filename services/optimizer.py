"""
services/optimizer.py — Hedged Holder Optimizer
================================================
Objective: find entry/exit thresholds per exchange such that:
  1. ~75 % of trading capacity is deployed (open positions or pending buys)
     at any given scan cycle  →  entry threshold
  2. P&L is maximised given that entry threshold  →  exit threshold

Algorithm
---------
  Phase A  Scan entry thresholds; simulate 48 h (24 h warmup + 24 h eval).
           Pick the threshold where avg capital utilisation ≈ TARGET_UTIL.
  Phase B  Scan exit thresholds with the fixed entry; maximise eval-window P&L.
           Use sklearn polynomial regression to smooth the P&L curve and find
           a less-noisy peak.  Optionally blend with GBR trained on real trades.

Other features
--------------
  • Fetches from strategy_collector, market_metrics, positions, portfolio_snapshots
  • Deletes records older than 72 h from strategy_collector, market_metrics,
    portfolio_snapshots
  • Surfaces execution quality (slippage, win-rate, hold-time) from positions
  • Prints recommendations to terminal only — does NOT touch bot_config.json

Run standalone:   python services/optimizer.py
"""

import warnings
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
from dotenv import load_dotenv
import logging

from joblib import Parallel, delayed
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# sklearn — graceful fallback if not installed
try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.model_selection import cross_val_score

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# ── Setup ──────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Optimizer")
load_dotenv(override=True)
getcontext().prec = 6

# ── Constants ──────────────────────────────────────────────────────────────────
BATCH_SIZE = 100.0        # USDT notional per simulated position
MAX_CONCURRENT = 20       # max concurrent positions per exchange in simulation
FEE_RATE = 0.002          # 0.2 % round-trip
TARGET_UTIL = 0.75        # target capital utilisation (75 %)
WARMUP_HOURS = 24         # hours to skip before measuring util / P&L
DATA_WINDOW_HOURS = 48    # total simulation window (warmup + eval)
EVAL_HOURS = DATA_WINDOW_HOURS - WARMUP_HOURS  # evaluation window = 24 h
CLEANUP_HOURS = 72        # delete data older than this
MIN_TRADES = 5            # min simulated trades (eval window) for a valid result
MIN_POSITIONS_ML = 8      # min real closed positions to attempt ML blend
DEFAULT_EXIT_FOR_UTIL = 1.5   # exit threshold used in Phase A (utilisation scan)


# ── Database ───────────────────────────────────────────────────────────────────
def get_db_engine():
    return create_engine(
        "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=os.getenv("DB_USER", "root"),
            pw=os.getenv("DB_PASSWORD", ""),
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=os.getenv("DB_PORT", "3306"),
            db=os.getenv("DB_NAME", "crypto_arb3"),
        ),
        pool_pre_ping=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# 1.  CLEANUP
# ══════════════════════════════════════════════════════════════════════════════
def cleanup_old_data(hours=CLEANUP_HOURS):
    """Delete strategy_collector, market_metrics, and portfolio_snapshots rows
    older than `hours`."""
    cutoff = datetime.now() - timedelta(hours=hours)
    tables = [
        ("strategy_collector", "timestamp"),
        ("market_metrics",     "timestamp"),
        ("portfolio_snapshots","timestamp"),
    ]
    engine = get_db_engine()
    total = 0
    with engine.begin() as conn:
        for table, col in tables:
            try:
                r = conn.execute(
                    text(f"DELETE FROM {table} WHERE {col} < :cutoff"),
                    {"cutoff": cutoff},
                )
                n = r.rowcount
                total += n
                if n:
                    logger.info(f"   Deleted {n:,} rows from {table}")
            except Exception as exc:
                logger.warning(f"   ⚠️  Could not clean {table}: {exc}")
    return total


# ══════════════════════════════════════════════════════════════════════════════
# 2.  DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════
def load_market_data(hours=DATA_WINDOW_HOURS):
    """Load strategy_collector and recalculate maker-side premiums."""
    cutoff = datetime.now() - timedelta(hours=hours)
    q = text("""
        SELECT timestamp, symbol, spot_exchange,
               spot_bid_price, spot_ask_price,
               binance_bid_price, binance_ask_price,
               entry_premium_pct, exit_premium_pct,
               ref_usdt_ask
        FROM strategy_collector
        WHERE timestamp >= :cutoff
        ORDER BY timestamp ASC
    """)
    engine = get_db_engine()
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"cutoff": cutoff})

    if df.empty:
        return df

    df["ref_usdt_ask"] = df["ref_usdt_ask"].replace(0, 1450.0).fillna(1450.0)

    # Maker math: buy at bid (entry), sell at ask (exit)
    df["real_entry_prem"] = (
        (df["spot_bid_price"] / df["ref_usdt_ask"]) / df["binance_bid_price"] - 1
    ) * 100
    df["real_exit_prem"] = (
        (df["spot_ask_price"] / df["ref_usdt_ask"]) / df["binance_ask_price"] - 1
    ) * 100

    # Fallback to stored values if recalc fails
    df["real_entry_prem"] = df["real_entry_prem"].fillna(df["entry_premium_pct"])
    df["real_exit_prem"]  = df["real_exit_prem"].fillna(df["exit_premium_pct"])

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=["real_entry_prem", "real_exit_prem"], inplace=True)

    # Clip extreme outliers per-exchange (data corruption / USDT rate anomalies).
    # Normal Kimchi premium range is roughly −5 % to +5 %; cap at ±10 % to be safe.
    df["real_entry_prem"] = df["real_entry_prem"].clip(-10.0, 10.0)
    df["real_exit_prem"]  = df["real_exit_prem"].clip(-10.0, 10.0)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_positions_data(hours=CLEANUP_HOURS):
    """Load recent positions for execution-quality analysis."""
    cutoff = datetime.now() - timedelta(hours=hours)
    q = text("""
        SELECT id, symbol, exchange, status,
               entry_time, exit_time,
               calc_entry_premium, calc_exit_premium,
               config_entry_threshold, config_exit_threshold,
               net_pnl_usdt
        FROM positions
        WHERE entry_time >= :cutoff
        ORDER BY entry_time ASC
    """)
    engine = get_db_engine()
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"cutoff": cutoff})

    if df.empty:
        return df
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["exit_time"]  = pd.to_datetime(df["exit_time"])
    return df


def load_market_metrics(hours=24):
    """Load recent market_metrics rows (display only)."""
    cutoff = datetime.now() - timedelta(hours=hours)
    q = text("""
        SELECT timestamp, exchange,
               median_entry_premium, median_exit_premium, opportunity_count
        FROM market_metrics
        WHERE timestamp >= :cutoff
        ORDER BY timestamp ASC
    """)
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn, params={"cutoff": cutoff})
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception:
        return pd.DataFrame()


def load_latest_snapshot():
    """Return the single latest portfolio_snapshots row (display only)."""
    q = text("SELECT * FROM portfolio_snapshots ORDER BY id DESC LIMIT 1")
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
        return df.iloc[0] if not df.empty else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 3.  EXECUTION QUALITY
# ══════════════════════════════════════════════════════════════════════════════
def compute_execution_quality(pos_df, exch):
    """Per-exchange execution stats from real closed positions."""
    empty = dict(
        trade_count=0, closed_count=0, completion_rate=None,
        win_rate=None, avg_pnl=None, pnl_std=None,
        avg_entry_slippage=0.0, avg_exit_slippage=0.0,
        avg_hold_hours=None,
    )
    if pos_df.empty:
        return empty

    ex = pos_df[pos_df["exchange"] == exch]
    if ex.empty:
        return empty

    closed = ex[ex["status"] == "CLOSED"].copy()
    stats = {
        "trade_count":     len(ex),
        "closed_count":    len(closed),
        "completion_rate": len(closed) / len(ex) if len(ex) > 0 else None,
    }

    if closed.empty:
        stats.update(dict(
            win_rate=None, avg_pnl=None, pnl_std=None,
            avg_entry_slippage=0.0, avg_exit_slippage=0.0,
            avg_hold_hours=None,
        ))
        return stats

    pnl = closed["net_pnl_usdt"].dropna()
    stats["avg_pnl"]  = float(pnl.mean())  if not pnl.empty else None
    stats["pnl_std"]  = float(pnl.std())   if len(pnl) > 1  else 0.0
    stats["win_rate"] = float((pnl > 0).mean()) if not pnl.empty else None

    # Entry slippage: calc_entry_premium − config_entry_threshold
    # Positive means we entered at a higher premium than the threshold triggered (worse fill)
    se = closed.dropna(subset=["calc_entry_premium", "config_entry_threshold"])
    stats["avg_entry_slippage"] = float(
        (se["calc_entry_premium"] - se["config_entry_threshold"]).mean()
    ) if not se.empty else 0.0

    # Exit slippage: calc_exit_premium − config_exit_threshold
    # Positive means we exited at a higher premium than threshold (better fill)
    sx = closed.dropna(subset=["calc_exit_premium", "config_exit_threshold"])
    stats["avg_exit_slippage"] = float(
        (sx["calc_exit_premium"] - sx["config_exit_threshold"]).mean()
    ) if not sx.empty else 0.0

    # Hold time
    ht = closed.dropna(subset=["entry_time", "exit_time"])
    if not ht.empty:
        hold_h = (ht["exit_time"] - ht["entry_time"]).dt.total_seconds() / 3600.0
        stats["avg_hold_hours"] = float(hold_h.mean())
    else:
        stats["avg_hold_hours"] = None

    return stats


# ══════════════════════════════════════════════════════════════════════════════
# 4.  CORE SIMULATION (warmup-aware)
# ══════════════════════════════════════════════════════════════════════════════
def simulate_with_warmup(df_subset, entry_th, exit_th, eval_start,
                         slip_e=0.0, slip_x=0.0):
    """
    Simulate the maker strategy over the full data window.

    Slippage convention
    -------------------
    slip_e = mean(calc_entry_premium − config_entry_threshold)
             Negative → actual fill is at a LOWER (better) premium than the threshold.
    slip_x = mean(calc_exit_premium  − config_exit_threshold)
             Negative → actual fill is at a LOWER (worse)  premium than the threshold.

    Trigger conditions use raw market premiums (no slippage adjustment).
    P&L uses the *threshold + slippage* as fixed fill prices, which correctly
    represents what we expect to receive regardless of how deep the market moved.

      per-trade gross  = (exit_th + slip_x) − (entry_th + slip_e)
      per-trade net    = gross − fee

    Returns
    -------
    (avg_utilisation, eval_pnl, eval_trade_count)
    """
    active = []          # list of {"symbol": str}
    eval_pnl = 0.0
    eval_trades = 0
    util_samples = []

    # Pre-compute constant per-trade P&L for this (entry_th, exit_th) pair.
    gross_pct    = (exit_th + slip_x) - (entry_th + slip_e)
    net_pct      = gross_pct - FEE_RATE * 100
    pnl_per_trade = BATCH_SIZE * (net_pct / 100.0)

    for row in df_subset.itertuples():
        is_eval = row.timestamp >= eval_start

        # A. Manage exits — trigger on raw exit premium reaching exit_th
        for pos in active[:]:
            if pos["symbol"] == row.symbol and row.real_exit_prem >= exit_th:
                if is_eval:
                    eval_pnl += pnl_per_trade
                    eval_trades += 1
                active.remove(pos)

        # B. Manage entries — trigger on raw entry premium at or below entry_th
        if len(active) < MAX_CONCURRENT and row.real_entry_prem <= entry_th:
            if not any(p["symbol"] == row.symbol for p in active):
                active.append({"symbol": row.symbol})

        # C. Sample utilisation (eval window only)
        if is_eval:
            util_samples.append(len(active) / MAX_CONCURRENT)

    avg_util = float(np.mean(util_samples)) if util_samples else 0.0
    return avg_util, eval_pnl, eval_trades


def _phase_a_worker(df_exch, entry_th, eval_start, slip_e):
    """Joblib worker for Phase A (utilisation scan)."""
    util, _, _ = simulate_with_warmup(
        df_exch, entry_th, DEFAULT_EXIT_FOR_UTIL, eval_start, slip_e, 0.0
    )
    return {"entry": entry_th, "util": util}


def _phase_b_worker(df_exch, fixed_entry, exit_th, eval_start, slip_e, slip_x):
    """Joblib worker for Phase B (exit / P&L scan)."""
    util, pnl, trades = simulate_with_warmup(
        df_exch, fixed_entry, exit_th, eval_start, slip_e, slip_x
    )
    return {"exit": exit_th, "pnl": pnl, "trades": trades, "util": util}


# ══════════════════════════════════════════════════════════════════════════════
# 5.  TWO-PHASE THRESHOLD SEARCH
# ══════════════════════════════════════════════════════════════════════════════
def make_decimal_range(start, stop, step):
    current = Decimal(str(start))
    stop_d  = Decimal(str(stop))
    step_d  = Decimal(str(step))
    vals = []
    while current < stop_d:
        vals.append(float(current))
        current += step_d
    return vals


def _entry_range_for(exch):
    if exch == "UPBIT":
        return make_decimal_range(-2.0, 0.1, 0.1)
    if exch == "BITHUMB":
        return make_decimal_range(-3.0, 0.1, 0.1)
    return make_decimal_range(-3.0, 0.1, 0.1)  # COINONE


def _exit_range_for(exch):
    if exch == "UPBIT":
        return make_decimal_range(0.3, 3.0, 0.1)
    if exch == "BITHUMB":
        return make_decimal_range(0.0, 4.0, 0.1)
    return make_decimal_range(0.0, 4.0, 0.1)   # COINONE


def find_entry_for_utilisation(df_exch, exch, eval_start, slip_e):
    """
    Phase A: scan entry thresholds to find the one where avg capital
    utilisation (eval window) is closest to TARGET_UTIL.
    """
    entry_range = _entry_range_for(exch)
    logger.info(f"   Phase A — scanning {len(entry_range)} entry thresholds …")

    rows = Parallel(n_jobs=-1)(
        delayed(_phase_a_worker)(df_exch, e, eval_start, slip_e)
        for e in entry_range
    )
    util_df = pd.DataFrame(rows).sort_values("entry")

    # Find closest to target utilisation
    util_df["diff"] = (util_df["util"] - TARGET_UTIL).abs()
    best_row = util_df.loc[util_df["diff"].idxmin()]
    return float(best_row["entry"]), float(best_row["util"]), util_df


def find_best_exit(df_exch, fixed_entry, exch, eval_start, slip_e, slip_x):
    """
    Phase B: scan exit thresholds and return the one that maximises eval P&L.
    Uses sklearn polynomial regression to find a smooth optimum.
    """
    exit_range = [x for x in _exit_range_for(exch) if x > fixed_entry + 0.3]
    if not exit_range:
        return None, None, pd.DataFrame()

    logger.info(f"   Phase B — scanning {len(exit_range)} exit thresholds …")

    rows = Parallel(n_jobs=-1)(
        delayed(_phase_b_worker)(df_exch, fixed_entry, x, eval_start, slip_e, slip_x)
        for x in exit_range
    )
    exit_df = pd.DataFrame(rows).sort_values("exit")

    # Raw best (must have MIN_TRADES)
    valid = exit_df[exit_df["trades"] >= MIN_TRADES]
    raw_best = valid.sort_values("pnl", ascending=False).iloc[0] if not valid.empty \
               else exit_df.sort_values("pnl", ascending=False).iloc[0]

    smoothed_best = None
    if SKLEARN_AVAILABLE and len(exit_df) >= 5:
        try:
            X = exit_df[["exit"]].values
            y = exit_df["pnl"].values

            poly = PolynomialFeatures(degree=3, include_bias=False)
            X_p  = poly.fit_transform(X)
            lr   = LinearRegression().fit(X_p, y)

            # Evaluate fitted curve on a fine grid
            fine_x = np.linspace(exit_df["exit"].min(), exit_df["exit"].max(), 500).reshape(-1, 1)
            fine_y = lr.predict(poly.transform(fine_x))

            smooth_peak_exit = float(fine_x[np.argmax(fine_y)])
            # Round to nearest 0.1 step
            smooth_peak_exit = round(smooth_peak_exit * 10) / 10

            # Build a pseudo-row for the smoothed recommendation
            closest = exit_df.iloc[(exit_df["exit"] - smooth_peak_exit).abs().argsort()[:1]]
            smoothed_best = closest.iloc[0].to_dict()
            smoothed_best["exit"] = smooth_peak_exit
        except Exception:
            smoothed_best = None

    return raw_best, smoothed_best, exit_df


# ══════════════════════════════════════════════════════════════════════════════
# 6.  ML BLEND (optional, GBR on real trades)
# ══════════════════════════════════════════════════════════════════════════════
def ml_blend_exit(exit_df, pos_exch):
    """
    If ≥ MIN_POSITIONS_ML real closed trades exist, train a GBR on
    (config_entry_threshold, config_exit_threshold) → net_pnl_usdt,
    cross-validate for R², and if R² > 0.10 blend 50/50 with simulation.

    Returns (blended_exit, r2) or (None, None).
    """
    if not SKLEARN_AVAILABLE:
        return None, None

    closed = pos_exch[pos_exch["status"] == "CLOSED"].dropna(
        subset=["config_entry_threshold", "config_exit_threshold", "net_pnl_usdt"]
    )
    if len(closed) < MIN_POSITIONS_ML:
        return None, None

    X_train = closed[["config_entry_threshold", "config_exit_threshold"]].values
    y_train = closed["net_pnl_usdt"].values

    model = GradientBoostingRegressor(n_estimators=100, max_depth=3, random_state=42)

    # Cross-validate
    cv_r2 = cross_val_score(
        model, X_train, y_train,
        cv=min(5, len(closed)), scoring="r2"
    )
    r2 = float(np.mean(cv_r2))

    if r2 <= 0.10:
        return None, r2

    model.fit(X_train, y_train)

    # Predict P&L for each exit threshold in the scan
    fixed_entry_col = exit_df["exit"].apply(
        lambda _: closed["config_entry_threshold"].mean()
    ).values.reshape(-1, 1)
    exit_col = exit_df[["exit"]].values
    X_pred = np.hstack([fixed_entry_col, exit_col])
    ml_pnl = model.predict(X_pred)

    blended = 0.5 * exit_df["pnl"].values + 0.5 * ml_pnl
    best_idx = int(np.argmax(blended))
    blended_exit = float(exit_df.iloc[best_idx]["exit"])
    return blended_exit, r2


# ══════════════════════════════════════════════════════════════════════════════
# 7.  TERMINAL OUTPUT HELPERS
# ══════════════════════════════════════════════════════════════════════════════
W = 65  # column width


def _hr(char="─"):
    return char * W


def print_market_section(mkt_df, exch):
    ex = mkt_df[mkt_df["spot_exchange"] == exch]
    if ex.empty:
        print("  No market data in 24 h window")
        return
    n = len(ex)
    ep = ex["real_entry_prem"].dropna()
    xp = ex["real_exit_prem"].dropna()
    print(f"  Data points : {n:,}")
    if not ep.empty:
        print(f"  Entry prem  : mean {ep.mean():+.3f}%  std {ep.std():.3f}%"
              f"  p10 {np.percentile(ep, 10):+.3f}%  p25 {np.percentile(ep, 25):+.3f}%")
    if not xp.empty:
        print(f"  Exit  prem  : mean {xp.mean():+.3f}%  std {xp.std():.3f}%"
              f"  p75 {np.percentile(xp, 75):+.3f}%  p90 {np.percentile(xp, 90):+.3f}%")
        # Warn if exit premiums look anomalous (bid-ask spread >> normal)
        if xp.mean() - ep.mean() > 3.0:
            print(f"  ⚠️  DATA WARNING: avg bid-ask spread "
                  f"({xp.mean() - ep.mean():+.2f}%) is abnormally wide. "
                  f"Check scanner USDT rate / ask-price logic for {exch}.")


def print_exec_section(eq):
    tc = eq.get("trade_count", 0)
    if tc == 0:
        print("  No trades recorded in 72 h window")
        return
    cc  = eq.get("closed_count", 0)
    cr  = eq.get("completion_rate")
    wr  = eq.get("win_rate")
    ap  = eq.get("avg_pnl")
    ps  = eq.get("pnl_std")
    ae  = eq.get("avg_entry_slippage", 0.0)
    ax  = eq.get("avg_exit_slippage",  0.0)
    ht  = eq.get("avg_hold_hours")

    line = f"  Trades      : {tc} total | {cc} closed"
    if cr is not None:
        line += f"  |  completion {cr*100:.0f}%"
    print(line)

    if wr is not None:
        win_str = f"  Win rate    : {wr*100:.0f}%"
        if ap is not None:
            win_str += f"   avg PnL/trade ${ap:+.3f}"
            if ps:
                win_str += f"  (±${ps:.3f})"
        print(win_str)

    slip = f"  Slippage    : entry {ae:+.4f}%  exit {ax:+.4f}%"
    slip += "  (+ = better fill than threshold)" if ae < 0 or ax > 0 else ""
    print(slip)

    if ht is not None:
        print(f"  Avg hold    : {ht:.1f} h")


def print_recommendation(rec_entry, rec_exit, sim_util, sim_pnl, sim_trades,
                         eq, method_note):
    fr  = eq.get("completion_rate")
    adj = f"  fill-adj ${sim_pnl * fr:+.2f}" if fr is not None else ""

    # Show opportunity frequency based on recorded utilisation
    print(f"  ┌─ Entry Threshold  : {rec_entry:+.2f}%")
    print(f"  ├─ Exit  Threshold  : {rec_exit:+.2f}%")
    print(f"  ├─ Capital deployed : {sim_util*100:.1f}%  (target {TARGET_UTIL*100:.0f}%)")
    print(f"  ├─ Sim trades       : {int(sim_trades)}  (eval 24 h)")
    print(f"  ├─ Sim P&L          : ${sim_pnl:+.2f}{adj}")
    print(f"  └─ Method           : {method_note}")


# ══════════════════════════════════════════════════════════════════════════════
# 8.  MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
def run_optimizer(stop_event=None):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n" + "=" * W)
    print(f"  🚀  OPTIMIZER   {now_str}")
    print(f"      Strategy: Maker Hedged  |  Target utilisation: {TARGET_UTIL*100:.0f}%")
    print("=" * W)

    # ── Cleanup ────────────────────────────────────────────────────────────
    print(f"\n🧹 Cleaning up records older than {CLEANUP_HOURS} h …")
    cleanup_old_data(CLEANUP_HOURS)

    # ── Load data ──────────────────────────────────────────────────────────
    print(f"\n⏳ Loading data …")
    mkt_df  = load_market_data(hours=DATA_WINDOW_HOURS)
    pos_df  = load_positions_data(hours=CLEANUP_HOURS)
    snap    = load_latest_snapshot()

    if mkt_df.empty:
        logger.error("❌  No market data in the last 48 h — aborting.")
        return

    # Summary line
    exch_counts = mkt_df.groupby("spot_exchange").size()
    print(f"   Market rows : {len(mkt_df):,}  "
          + "  ".join(f"{e} {n:,}" for e, n in exch_counts.items()))
    if not pos_df.empty:
        cc = (pos_df["status"] == "CLOSED").sum()
        print(f"   Positions   : {len(pos_df)} total | {cc} closed  (72 h window)")
    if snap is not None:
        print(f"   Portfolio   : ${snap.get('total_usdt_value', 0):,.2f} USDT  "
              f"FX ₩{snap.get('fx_rate', 0):,.0f}")

    # ── Per-exchange optimisation ──────────────────────────────────────────
    recommendations = {}   # exch -> (entry, exit, util, pnl)
    for exch in ["UPBIT", "BITHUMB", "COINONE"]:
        if stop_event and stop_event.is_set():
            break

        print(f"\n{_hr()}")
        print(f"  📊  {exch}")
        print(_hr())

        df_exch = mkt_df[mkt_df["spot_exchange"] == exch].copy()
        if len(df_exch) < 100:
            print(f"  ⚠️  Insufficient data for {exch} ({len(df_exch)} rows)")
            continue

        # Anchor eval window from the END of available data so it is always
        # the most-recent EVAL_HOURS, with however much warmup precedes it.
        # (If data < 48 h the warmup is shorter; the 75%-util objective still holds.)
        t_min      = df_exch["timestamp"].min()
        t_max      = df_exch["timestamp"].max()
        data_span  = (t_max - t_min).total_seconds() / 3600.0
        eval_start = t_max - pd.Timedelta(hours=EVAL_HOURS)
        warmup_h   = max(0.0, (eval_start - t_min).total_seconds() / 3600.0)
        eval_rows  = df_exch[df_exch["timestamp"] >= eval_start]
        if eval_rows.empty:
            print(f"  ⚠️  No rows in the evaluation window (data span only {data_span:.1f} h)")
            continue
        print(f"  Data span: {data_span:.1f} h  |  warmup: {warmup_h:.1f} h  |  "
              f"eval window: {EVAL_HOURS} h  ({len(eval_rows):,} rows)")

        # Execution quality
        eq = compute_execution_quality(pos_df, exch)
        slip_e = eq.get("avg_entry_slippage", 0.0) or 0.0
        slip_x = eq.get("avg_exit_slippage",  0.0) or 0.0

        # ── Market analysis (last 24 h of the window) ──────────────────────
        print(f"\n  [Market Analysis — last {EVAL_HOURS} h]")
        print_market_section(eval_rows, exch)

        # ── Execution quality ──────────────────────────────────────────────
        print(f"\n  [Execution Quality — last {CLEANUP_HOURS} h]")
        print_exec_section(eq)

        # ── Simulation ────────────────────────────────────────────────────
        print(f"\n  [Simulation — {DATA_WINDOW_HOURS} h window, "
              f"{WARMUP_HOURS} h warmup, {EVAL_HOURS} h eval]")

        # Phase A: entry threshold → 75 % utilisation
        best_entry, best_util, util_curve = find_entry_for_utilisation(
            df_exch, exch, eval_start, slip_e
        )

        util_note = ""
        if abs(best_util - TARGET_UTIL) > 0.15:
            util_note = (
                f"  ⚠️  Closest utilisation is {best_util*100:.1f}% "
                f"(target {TARGET_UTIL*100:.0f}%) — market may lack enough opportunities"
            )

        # Phase B: exit threshold → max P&L
        raw_best, smoothed_best, exit_df = find_best_exit(
            df_exch, best_entry, exch, eval_start, slip_e, slip_x
        )

        if stop_event and stop_event.is_set():
            break

        if raw_best is None:
            print(f"  ⚠️  No valid exit threshold found for {exch}")
            continue

        # ML blend (optional)
        pos_exch = pos_df[pos_df["exchange"] == exch] if not pos_df.empty else pd.DataFrame()
        ml_exit, ml_r2 = (None, None)
        if not exit_df.empty and not pos_exch.empty:
            ml_exit, ml_r2 = ml_blend_exit(exit_df, pos_exch)

        # ── Build recommendation ──────────────────────────────────────────
        # Priority: ML blend > smoothed poly > raw best
        if ml_exit is not None and ml_r2 is not None:
            rec_exit   = ml_exit
            method_str = f"Simulation + ML blend  (GBR R²={ml_r2:.2f})"
        elif smoothed_best is not None:
            rec_exit   = float(smoothed_best["exit"])
            method_str = "Simulation + polynomial smoothing (degree 3)"
        else:
            rec_exit = float(raw_best["exit"])
            method_str = "Simulation (raw grid best)"

        # Get sim stats at the chosen exit threshold
        chosen = exit_df.iloc[(exit_df["exit"] - rec_exit).abs().argsort()[:1]].iloc[0]
        sim_pnl    = float(chosen["pnl"])
        sim_trades = int(chosen["trades"])
        sim_util   = float(chosen.get("util", best_util))

        print(f"   Entry scan : {len(_entry_range_for(exch))} thresholds tested")
        print(f"   Exit  scan : {len(exit_df)} thresholds tested")
        if abs(slip_e) > 0.005 or abs(slip_x) > 0.005:
            print(f"   Slippage adj applied: entry {slip_e:+.4f}%  exit {slip_x:+.4f}%")

        print(f"\n  [Recommendation]")
        if util_note:
            print(util_note)
        print_recommendation(best_entry, rec_exit, sim_util, sim_pnl, sim_trades,
                             eq, method_str)

        recommendations[exch] = (best_entry, rec_exit, sim_util, sim_pnl)

    # ── Final summary table ────────────────────────────────────────────────
    print(f"\n{'═' * W}")
    print("  📋  RECOMMENDED THRESHOLDS  (copy to dashboard or bot_config.json)")
    print(f"{'─' * W}")
    print(f"  {'Exchange':<10}  {'Entry %':>9}  {'Exit %':>9}  {'Capital Deployed':>18}  {'Sim P&L':>10}")
    print(f"  {'─'*10}  {'─'*9}  {'─'*9}  {'─'*18}  {'─'*10}")
    for exch in ["UPBIT", "BITHUMB", "COINONE"]:
        if exch in recommendations:
            e, x, u, p = recommendations[exch]
            print(f"  {exch:<10}  {e:>+9.2f}%  {x:>+9.2f}%  {u*100:>16.1f}%  ${p:>+9.2f}")
        else:
            print(f"  {exch:<10}  {'—':>9}  {'—':>9}  {'—':>18}  {'—':>10}")
    print(f"{'─' * W}")
    print("  ⚠️  These are simulation estimates — apply judgement before updating config.")
    print(f"{'═' * W}\n")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_optimizer()
