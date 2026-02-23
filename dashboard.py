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

# 2. PORTFOLIO CHART (Last 24h)
st.subheader("📈 Portfolio Performance (24h)")
df_chart = get_portfolio_history()
if not df_chart.empty:
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.caption("Equity (USDT)")
        chart_usdt = (
            alt.Chart(df_chart)
            .mark_line(color="#00FFAA")
            .encode(
                x=alt.X("timestamp", axis=alt.Axis(title="Time", format="%H:%M")),
                y=alt.Y(
                    "total_usdt_value",
                    scale=alt.Scale(zero=False),
                    axis=alt.Axis(title="Equity (USDT)"),
                ),
                tooltip=["timestamp", "total_usdt_value"],
            )
            .properties(height=250)
        )
        st.altair_chart(chart_usdt, use_container_width=True)

    with chart_col2:
        st.caption("Equity (KRW) & FX Rate")

        # Shared X-axis base
        base = alt.Chart(df_chart).encode(
            x=alt.X("timestamp", axis=alt.Axis(title="Time", format="%H:%M"))
        )

        # KRW Line (Left Y-Axis)
        line_krw = base.mark_line(color="#FFB300").encode(
            y=alt.Y(
                "total_krw_value",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(title="Equity (KRW)", titleColor="#FFB300"),
            ),
            tooltip=["timestamp", "total_krw_value"],
        )

        # FX Rate Line (Right Y-Axis)
        line_fx = base.mark_line(color="#FF4B4B").encode(
            y=alt.Y(
                "fx_rate",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(title="FX Rate", titleColor="#FF4B4B"),
            ),
            tooltip=["timestamp", "fx_rate"],
        )

        # Superimpose and resolve Y-axes to be independent
        chart_dual = (
            alt.layer(line_krw, line_fx)
            .resolve_scale(y="independent")
            .properties(height=250)
        )

        st.altair_chart(chart_dual, use_container_width=True)

# 3. CONTROL PANEL
st.subheader("⚙️ Bot Controls")
config = load_config()
if config:
    c_sys, c_up, c_bi, c_co = st.columns([1, 2, 2, 2])

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

    with c_co:
        st.caption("COINONE Thresholds")
        co_entry = st.number_input(
            "Entry %",
            value=float(config.get("COINONE", {}).get("ENTRY", -1.0)),
            step=0.1,
            format="%.2f",
            key="co_e",
        )
        co_exit = st.number_input(
            "Exit %",
            value=float(config.get("COINONE", {}).get("EXIT", 2.0)),
            step=0.1,
            format="%.2f",
            key="co_x",
        )

    if st.button("💾 Save Thresholds"):
        if "UPBIT" not in config:
            config["UPBIT"] = {}
        if "BITHUMB" not in config:
            config["BITHUMB"] = {}
        if "COINONE" not in config:
            config["COINONE"] = {}
        config["UPBIT"]["ENTRY"] = u_entry
        config["UPBIT"]["EXIT"] = u_exit
        config["BITHUMB"]["ENTRY"] = b_entry
        config["BITHUMB"]["EXIT"] = b_exit
        config["COINONE"]["ENTRY"] = co_entry
        config["COINONE"]["EXIT"] = co_exit
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
