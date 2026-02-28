import streamlit as st
import pandas as pd
import json
import os
import time
import altair as alt
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(override=True)
st.set_page_config(page_title="Hedged Holder Command", layout="wide", page_icon="🛡️")

# --- CONFIGURATION ---
CONFIG_FILE = "bot_config.json"
REFRESH_RATE = 10  # Auto-refresh every 10 seconds

# Auto-Refresh Logic
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > REFRESH_RATE:
    st.session_state.last_refresh = time.time()
    st.rerun()


@st.cache_resource
def get_db_engine():
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")

    # [FIX] Added pool_pre_ping and pool_recycle to prevent dropped connections
    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}",
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}


def save_config(config):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)


# --- DATA FETCHING ---
def get_portfolio_history():
    """Fetches the last 24 hours of portfolio snapshots."""
    engine = get_db_engine()
    query = text(
        """
        SELECT timestamp, total_usdt_value, total_krw_value, fx_rate 
        FROM portfolio_snapshots 
        WHERE timestamp >= NOW() - INTERVAL 1 DAY 
        ORDER BY timestamp ASC
    """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def get_latest_snapshot():
    """Fetches the single latest snapshot."""
    engine = get_db_engine()
    query = text(
        """
        SELECT * FROM portfolio_snapshots 
        ORDER BY id DESC LIMIT 1
    """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df.iloc[0] if not df.empty else None


def get_active_positions():
    """
    Fetches OPEN positions and instantly queries the absolute latest price for each coin.
    """
    engine = get_db_engine()
    query = text(
        """
        SELECT 
            p.symbol,
            p.exchange,
            p.entry_time,
            p.calc_entry_premium as entry_premium,
            p.entry_spot_price,
            p.entry_hedge_price,
            p.current_spot_qty,
            p.current_hedge_qty,
            p.entry_usdt_rate,
            (SELECT exit_premium_pct FROM strategy_collector s WHERE s.symbol = p.symbol COLLATE utf8mb4_unicode_ci AND s.spot_exchange = p.exchange COLLATE utf8mb4_unicode_ci ORDER BY s.id DESC LIMIT 1) as current_exit_premium,
            (SELECT spot_ask_price FROM strategy_collector s WHERE s.symbol = p.symbol COLLATE utf8mb4_unicode_ci AND s.spot_exchange = p.exchange COLLATE utf8mb4_unicode_ci ORDER BY s.id DESC LIMIT 1) as live_spot_price,
            (SELECT binance_ask_price FROM strategy_collector s WHERE s.symbol = p.symbol COLLATE utf8mb4_unicode_ci AND s.spot_exchange = p.exchange COLLATE utf8mb4_unicode_ci ORDER BY s.id DESC LIMIT 1) as live_hedge_price
        FROM positions p
        WHERE p.status = 'OPEN'
        ORDER BY p.entry_time DESC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def get_capital_events_in_window():
    """Fetches capital deposits/withdrawals recorded in the last 24 hours."""
    engine = get_db_engine()
    try:
        query = text(
            """
            SELECT timestamp, amount_krw, amount_usdt, fx_rate, notes
            FROM capital_events
            WHERE timestamp >= NOW() - INTERVAL 1 DAY
            ORDER BY timestamp ASC
        """
        )
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame(columns=["timestamp", "amount_krw", "amount_usdt", "fx_rate", "notes"])


def get_all_capital_events():
    """Fetches all capital events (for the history table)."""
    engine = get_db_engine()
    try:
        query = text(
            """
            SELECT timestamp, amount_krw, amount_usdt, fx_rate, notes
            FROM capital_events
            ORDER BY timestamp DESC
            LIMIT 20
        """
        )
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


def record_capital_event(amount_krw: float, amount_usdt: float, fx_rate: float, notes: str):
    engine = get_db_engine()
    with engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO capital_events (timestamp, amount_krw, amount_usdt, fx_rate, notes) "
                "VALUES (NOW(), :krw, :usdt, :fx, :notes)"
            ),
            {"krw": amount_krw, "usdt": amount_usdt, "fx": fx_rate, "notes": notes},
        )
        conn.commit()


def get_closed_positions():
    engine = get_db_engine()
    query = text(
        """
        SELECT 
            symbol, exchange, entry_time, exit_time, 
            calc_entry_premium, calc_exit_premium,
            net_pnl_usdt, gross_spot_pnl_krw, gross_hedge_pnl_usdt
        FROM positions 
        WHERE status = 'CLOSED' 
        ORDER BY exit_time DESC 
        LIMIT 50
    """
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


# --- UI LAYOUT ---
st.title("🛡️ Hedged Holder V2 Dashboard")

# 1. TOP METRICS
snap = get_latest_snapshot()
# [FIX] Check using 'is not None' to avoid ambiguity error
if snap is not None:
    c1, c_krw, c2, c3, c4 = st.columns(5)
    with c1:
        st.metric("Total Equity (USDT)", f"${snap['total_usdt_value']:,.2f}")
    with c_krw:
        st.metric("Total Equity (KRW)", f"₩{snap['total_krw_value']:,.0f}")
    with c2:
        st.metric("Free USDT (Binance)", f"${snap['binance_usdt_free']:,.2f}")
    with c3:
        st.metric("Free KRW (Spot)", f"₩{snap['spot_krw_free']:,.0f}")
    with c4:
        st.metric("FX Rate", f"₩{snap['fx_rate']:,.1f}")

# 2. PORTFOLIO CHART (Last 24h) — all three series indexed to 100 at period start
st.subheader("📈 Portfolio Performance (24h)")
df_chart = get_portfolio_history()
if not df_chart.empty:
    # Subtract capital deposits so the chart shows only organic P&L
    df_events = get_capital_events_in_window()
    if not df_events.empty:
        df_chart["cumulative_deposits_usdt"] = df_chart["timestamp"].apply(
            lambda t: float(df_events[df_events["timestamp"] <= t]["amount_usdt"].sum())
        )
    else:
        df_chart["cumulative_deposits_usdt"] = 0.0

    df_chart["organic_usdt"] = df_chart["total_usdt_value"] - df_chart["cumulative_deposits_usdt"]
    df_chart["organic_krw"]  = df_chart["organic_usdt"] * df_chart["fx_rate"]

    base_usdt = df_chart["organic_usdt"].iloc[0]
    base_krw  = df_chart["organic_krw"].iloc[0]
    base_fx   = df_chart["fx_rate"].iloc[0]

    if base_usdt > 0 and base_krw > 0 and base_fx > 0:
        df_chart = df_chart.copy()
        df_chart["USDT Equity"] = df_chart["organic_usdt"] / base_usdt * 100
        df_chart["KRW Equity"]  = df_chart["organic_krw"]  / base_krw  * 100
        df_chart["FX Rate"]     = df_chart["fx_rate"]       / base_fx   * 100

        df_long = df_chart[["timestamp", "USDT Equity", "KRW Equity", "FX Rate"]].melt(
            id_vars="timestamp", var_name="Series", value_name="Index"
        )

        # Last data point per series — used for end-of-line percentage labels
        df_ends = (
            df_long.sort_values("timestamp")
            .groupby("Series", as_index=False)
            .last()
        )
        df_ends["label"] = df_ends["Index"].apply(lambda v: f"{v - 100:+.1f}%")

        _domain = ["USDT Equity", "KRW Equity", "FX Rate"]
        _range  = ["#00FFAA",     "#FFB300",     "#FF4B4B"]

        lines = (
            alt.Chart(df_long)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X("timestamp:T", axis=alt.Axis(title="Time", format="%H:%M")),
                y=alt.Y(
                    "Index:Q",
                    scale=alt.Scale(zero=False),
                    axis=alt.Axis(title="Indexed to period start  (100 = unchanged)"),
                ),
                color=alt.Color(
                    "Series:N",
                    scale=alt.Scale(domain=_domain, range=_range),
                    legend=alt.Legend(orient="top-left"),
                ),
                tooltip=[
                    alt.Tooltip("timestamp:T", title="Time", format="%m-%d %H:%M"),
                    alt.Tooltip("Series:N"),
                    alt.Tooltip("Index:Q", format=".2f"),
                ],
            )
        )

        end_labels = (
            alt.Chart(df_ends)
            .mark_text(align="left", dx=5, fontWeight="bold", fontSize=12)
            .encode(
                x=alt.X("timestamp:T"),
                y=alt.Y("Index:Q"),
                text=alt.Text("label:N"),
                color=alt.Color(
                    "Series:N",
                    scale=alt.Scale(domain=_domain, range=_range),
                    legend=None,  # legend already shown on the lines layer
                ),
            )
        )

        st.altair_chart(
            (lines + end_labels).properties(height=320, padding={"right": 70}),
            use_container_width=True,
        )

# 2b. CAPITAL MANAGEMENT
with st.expander("💰 Record Capital Deposit / Withdrawal"):
    current_fx = snap["fx_rate"] if snap is not None else 1450.0
    dep_col, hist_col = st.columns([1, 1])

    with dep_col:
        st.caption("Add a deposit (positive) or withdrawal (negative)")
        dep_krw = st.number_input(
            "Amount (KRW)",
            value=0,
            step=100_000,
            format="%d",
            key="dep_krw",
            help="Enter KRW added to Bithumb. Use a negative number for withdrawals.",
        )
        dep_notes = st.text_input("Notes (optional)", key="dep_notes", placeholder="e.g. Bithumb top-up")
        dep_usdt = dep_krw / current_fx if current_fx > 0 else 0.0
        st.caption(f"≈ ${dep_usdt:,.2f} USDT at FX {current_fx:,.0f}")
        if st.button("✅ Record", key="dep_submit", disabled=(dep_krw == 0)):
            record_capital_event(float(dep_krw), dep_usdt, current_fx, dep_notes)
            st.success(f"Recorded: ₩{dep_krw:,} (${dep_usdt:,.2f})")
            st.rerun()

    with hist_col:
        st.caption("Recent capital events")
        df_cap = get_all_capital_events()
        if not df_cap.empty:
            df_cap_disp = df_cap.copy()
            df_cap_disp["timestamp"] = pd.to_datetime(df_cap_disp["timestamp"]).dt.strftime("%m-%d %H:%M")
            df_cap_disp["amount_krw"] = df_cap_disp["amount_krw"].apply(lambda x: f"₩{x:,.0f}")
            df_cap_disp["amount_usdt"] = df_cap_disp["amount_usdt"].apply(lambda x: f"${x:,.2f}")
            df_cap_disp["fx_rate"] = df_cap_disp["fx_rate"].apply(lambda x: f"{x:,.0f}")
            df_cap_disp = df_cap_disp.rename(columns={
                "timestamp": "Time", "amount_krw": "KRW", "amount_usdt": "USDT",
                "fx_rate": "FX", "notes": "Notes"
            })
            st.dataframe(df_cap_disp, use_container_width=True, hide_index=True)
        else:
            st.info("No capital events recorded yet.")

# 3. CONTROL PANEL
st.subheader("⚙️ Bot Controls")
config = load_config()
if config:
    c_sys, c_up, c_bi = st.columns([1, 2, 2])

    with c_sys:
        is_paused = config.get("SYSTEM", {}).get("PAUSED", False)
        btn_label = "▶️ RESUME BOT" if is_paused else "⏸️ PAUSE BOT"
        if st.button(btn_label, type="primary" if not is_paused else "secondary"):
            if "SYSTEM" not in config:
                config["SYSTEM"] = {}
            config["SYSTEM"]["PAUSED"] = not is_paused
            save_config(config)
            st.rerun()
        if is_paused:
            st.warning("⚠️ SYSTEM PAUSED")

    with c_up:
        st.caption("UPBIT Thresholds")
        u_entry = st.number_input(
            "Entry %",
            value=float(config.get("UPBIT", {}).get("ENTRY", -0.5)),
            step=0.1,
            format="%.2f",
            key="u_e",
        )
        u_exit = st.number_input(
            "Exit %",
            value=float(config.get("UPBIT", {}).get("EXIT", 1.5)),
            step=0.1,
            format="%.2f",
            key="u_x",
        )

    with c_bi:
        st.caption("BITHUMB Thresholds")
        b_entry = st.number_input(
            "Entry %",
            value=float(config.get("BITHUMB", {}).get("ENTRY", -1.0)),
            step=0.1,
            format="%.2f",
            key="b_e",
        )
        b_exit = st.number_input(
            "Exit %",
            value=float(config.get("BITHUMB", {}).get("EXIT", 2.0)),
            step=0.1,
            format="%.2f",
            key="b_x",
        )

    if st.button("💾 Save Thresholds"):
        if "UPBIT" not in config:
            config["UPBIT"] = {}
        if "BITHUMB" not in config:
            config["BITHUMB"] = {}
        config["UPBIT"]["ENTRY"] = u_entry
        config["UPBIT"]["EXIT"] = u_exit
        config["BITHUMB"]["ENTRY"] = b_entry
        config["BITHUMB"]["EXIT"] = b_exit
        save_config(config)
        st.success("Configuration Updated!")

# 4. ACTIVE POSITIONS
st.subheader("🟢 Active Positions")
df_open = get_active_positions()

if not df_open.empty:
    # [FIX] Fallback to entry prices if live data is missing to prevent fake negative PnL
    df_open["live_spot_price"] = df_open["live_spot_price"].fillna(
        df_open["entry_spot_price"]
    )
    df_open["live_hedge_price"] = df_open["live_hedge_price"].fillna(
        df_open["entry_hedge_price"]
    )
    df_open["current_exit_premium"] = df_open["current_exit_premium"].fillna(
        df_open["entry_premium"]
    )

    # --- EXPECTED PNL CALCULATION (Maker Exit) ---
    # Spot PnL (KRW) = (Current Sell Ask - Entry Price) * Qty
    df_open["Unrealized Spot (KRW)"] = (
        df_open["live_spot_price"] - df_open["entry_spot_price"]
    ) * df_open["current_spot_qty"]

    # Hedge PnL (USDT) = (Entry Price - Current Buy Ask) * Qty  <-- Short Position logic
    df_open["Unrealized Hedge (USDT)"] = (
        df_open["entry_hedge_price"] - df_open["live_hedge_price"]
    ) * df_open["current_hedge_qty"]

    # Total Expected Net (USDT)
    ref_fx = snap["fx_rate"] if snap is not None else 1450.0
    df_open["Expected Net PnL (USDT)"] = (
        df_open["Unrealized Spot (KRW)"] / ref_fx
    ) + df_open["Unrealized Hedge (USDT)"]

    # Calculate Spread Delta (How close to exit?)
    df_open["Expected Spread Delta"] = (
        df_open["current_exit_premium"] - df_open["entry_premium"]
    )

    # Formatting & Selection
    display_df = df_open[
        [
            "symbol",
            "exchange",
            "entry_time",
            "entry_premium",
            "current_exit_premium",
            "Expected Spread Delta",
            "Expected Net PnL (USDT)",
            "Unrealized Spot (KRW)",
            "Unrealized Hedge (USDT)",
        ]
    ].copy()

    display_df = display_df.fillna(0)

    st.dataframe(
        display_df.style.format(
            {
                "entry_premium": "{:.2f}%",
                "current_exit_premium": "{:.2f}%",
                "Expected Spread Delta": "{:+.2f}%",
                "Expected Net PnL (USDT)": "${:+.2f}",
                "Unrealized Spot (KRW)": "₩{:,.0f}",
                "Unrealized Hedge (USDT)": "${:+.2f}",
                "entry_time": lambda x: (
                    x.strftime("%H:%M:%S") if pd.notnull(x) and x != 0 else "-"
                ),
            },
            na_rep="-",
        ).map(
            lambda x: (
                "color: #ff4b4b"
                if pd.notnull(x) and isinstance(x, (int, float)) and x < 0
                else "color: #00FFAA"
            ),
            subset=["Expected Net PnL (USDT)", "Expected Spread Delta"],
        ),
        use_container_width=True,
    )
else:
    st.info("💤 No active positions currently open.")

# 5. TRADE HISTORY
st.subheader("📜 Recent History (Last 50)")
df_closed = get_closed_positions()

if not df_closed.empty:
    total_realized = df_closed["net_pnl_usdt"].sum()
    c1, c2 = st.columns(2)
    c1.metric("Realized PnL (Displayed)", f"${total_realized:,.2f}")

    # [FIX] Fill numeric NaNs with 0 to prevent NoneType formatting errors
    numeric_cols = [
        "calc_entry_premium",
        "calc_exit_premium",
        "net_pnl_usdt",
        "gross_spot_pnl_krw",
        "gross_hedge_pnl_usdt",
    ]
    df_closed[numeric_cols] = df_closed[numeric_cols].fillna(0)

    st.dataframe(
        df_closed.style.format(
            {
                "calc_entry_premium": "{:.2f}%",
                "calc_exit_premium": "{:.2f}%",
                "net_pnl_usdt": "${:+.2f}",
                "gross_spot_pnl_krw": "₩{:,.0f}",
                "gross_hedge_pnl_usdt": "${:+.2f}",
                "entry_time": lambda x: (
                    x.strftime("%m-%d %H:%M") if pd.notnull(x) else "-"
                ),
                "exit_time": lambda x: (
                    x.strftime("%m-%d %H:%M") if pd.notnull(x) else "-"
                ),
            },
            na_rep="-",
        ).map(
            lambda x: (
                "color: #ff4b4b"
                if pd.notnull(x) and isinstance(x, (int, float)) and x < 0
                else "color: #00FFAA"
            ),
            subset=["net_pnl_usdt"],
        ),
        use_container_width=True,
    )
else:
    st.info("No trade history available.")
