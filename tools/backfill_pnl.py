import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv
import glob

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from database.session import SessionLocal

load_dotenv(override=True)


def get_db_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    return create_engine(f"mysql+mysqldb://{user}:{password}@{host}/{db_name}")


def smart_load(keyword):
    """
    Searches for a file containing 'keyword' in ./data or ./ and loads it.
    Handles both CSV and XLSX automatically.
    """
    # Search patterns
    patterns = [
        f"data/*{keyword}*",
        f"*{keyword}*",
    ]

    found_file = None
    for p in patterns:
        matches = glob.glob(p)
        # Filter out temp files or lock files
        matches = [
            m
            for m in matches
            if not m.startswith("~$")
            and (m.lower().endswith(".csv") or m.lower().endswith(".xlsx"))
        ]

        if matches:
            found_file = matches[0]  # Pick the first match
            break

    if not found_file:
        print(f"   ❌ No file found for '{keyword}'")
        return pd.DataFrame()

    print(f"   ✅ Found {keyword}: {found_file}")

    try:
        if found_file.lower().endswith(".xlsx"):
            # Bithumb Excel often has a junk header row, try row 0 or 1
            if "bithumb" in keyword.lower():
                df = pd.read_excel(found_file)
                if "거래일시" not in df.columns:
                    # Try next row as header
                    df = pd.read_excel(found_file, header=1)
                return df
            return pd.read_excel(found_file)
        else:
            return pd.read_csv(found_file)
    except Exception as e:
        print(f"   ⚠️ Error loading {found_file}: {e}")
        return pd.DataFrame()


def load_dataframes():
    print("📂 Smart-Loading Transaction Histories...")
    b_df = smart_load("binance")
    t_df = smart_load("bithumb")
    u_df = smart_load("upbit")

    # --- BINANCE PREP ---
    if not b_df.empty:
        # Standardize Columns
        # Binance Export might have "Date(UTC)"
        col_map = {c: c.strip() for c in b_df.columns}
        b_df.rename(columns=col_map, inplace=True)

        date_col = next((c for c in b_df.columns if "Date" in c), None)
        if date_col:
            b_df[date_col] = pd.to_datetime(b_df[date_col])
            b_df["std_time"] = b_df[date_col]  # Keep UTC

    # --- BITHUMB PREP ---
    if not t_df.empty:
        # Expected: 거래일시
        if "거래일시" in t_df.columns:
            t_df["거래일시"] = pd.to_datetime(t_df["거래일시"])

    # --- UPBIT PREP ---
    if not u_df.empty:
        # Expected: 체결시간
        if "체결시간" in u_df.columns:
            u_df["체결시간"] = pd.to_datetime(u_df["체결시간"])

    return b_df, t_df, u_df


def clean_money_str(val):
    if isinstance(val, str):
        return float(val.replace("KRW", "").replace(",", "").strip())
    return float(val)


def get_closest_fx(conn, timestamp):
    query = text(
        "SELECT ref_usdt_ask FROM strategy_collector WHERE timestamp <= :ts ORDER BY timestamp DESC LIMIT 1"
    )
    res = conn.execute(query, {"ts": timestamp}).fetchone()
    return float(res[0]) if res else 1450.0


def backfill():
    engine = get_db_engine()
    b_df, t_df, u_df = load_dataframes()

    with engine.connect() as conn:
        positions = conn.execute(
            text("SELECT * FROM positions WHERE status='CLOSED'")
        ).fetchall()
        print(f"🔄 Processing {len(positions)} closed positions...")

        updates = 0
        for pos in positions:
            pid = pos.id
            symbol = pos.symbol
            exch = pos.spot_exchange
            exit_time = pos.exit_time

            if not exit_time:
                continue

            # 1. FIND EXIT FX
            exit_fx = get_closest_fx(conn, exit_time)

            # 2. FIND SPOT SELL
            spot_sell_krw = 0.0
            spot_fee_krw = 0.0

            if exch == "UPBIT" and not u_df.empty and "체결시간" in u_df.columns:
                w_start, w_end = exit_time - timedelta(
                    minutes=5
                ), exit_time + timedelta(minutes=5)
                mask = (
                    (u_df["코인"] == symbol)
                    & (u_df["종류"].astype(str).str.contains("Sell|매도", case=False))
                    & (u_df["체결시간"] >= w_start)
                    & (u_df["체결시간"] <= w_end)
                )

                for _, row in u_df[mask].iterrows():
                    # Upbit "정산금액" is Net (Gross - Fee)
                    spot_sell_krw += clean_money_str(row["정산금액"])
                    spot_fee_krw += clean_money_str(row["수수료"])

            elif exch == "BITHUMB" and not t_df.empty and "거래일시" in t_df.columns:
                w_start, w_end = exit_time - timedelta(
                    minutes=10
                ), exit_time + timedelta(minutes=10)
                mask = (t_df["거래일시"] >= w_start) & (t_df["거래일시"] <= w_end)

                for _, row in t_df[mask].iterrows():
                    if "매도" not in str(row.get("거래구분", "")):
                        continue
                    if symbol not in str(row.get("자산", "")) and symbol not in str(
                        row.get("거래수량", "")
                    ):
                        continue

                    gross = clean_money_str(row["거래금액"])
                    fee = clean_money_str(row["수수료"])
                    spot_fee_krw += fee
                    spot_sell_krw += gross - fee

            # 3. FIND BINANCE HEDGE CLOSE
            hedge_pnl_usdt = 0.0
            binance_fee_usdt = 0.0

            if not b_df.empty and "std_time" in b_df.columns:
                # DB is KST, Binance is UTC. Convert DB time to UTC.
                exit_utc = exit_time - timedelta(hours=9)
                w_start, w_end = exit_utc - timedelta(minutes=5), exit_utc + timedelta(
                    minutes=5
                )

                b_sym = f"{symbol}USDT"
                mask = (
                    (b_df["Symbol"] == b_sym)
                    & (b_df["Side"] == "BUY")
                    & (b_df["std_time"] >= w_start)
                    & (b_df["std_time"] <= w_end)
                )

                for _, row in b_df[mask].iterrows():
                    hedge_pnl_usdt += float(row["Realized Profit"])
                    # Binance CSV fees can be mixed assets (BNB/USDT). Simple sum if USDT.
                    if "USDT" in str(row["Fee Coin"]):
                        binance_fee_usdt += float(row["Fee"])

            # 4. UPDATE DB
            if spot_sell_krw > 0:
                spot_sell_usdt = spot_sell_krw / exit_fx
                spot_fee_usdt = spot_fee_krw / exit_fx
                entry_cost = float(pos.entry_cost_usdt or 100.0)

                # NET PNL = (Net Sell Proceeds - Entry Cost) + Hedge PnL - Binance Fee
                net_pnl = (
                    (spot_sell_usdt - entry_cost) + hedge_pnl_usdt - binance_fee_usdt
                )

                conn.execute(
                    text(
                        """
                    UPDATE positions SET 
                        net_pnl_usdt=:net, hedge_pnl_usdt=:hedge, spot_fee_usdt=:sfee, 
                        exit_fx_rate=:fx 
                    WHERE id=:pid
                """
                    ),
                    {
                        "net": net_pnl,
                        "hedge": hedge_pnl_usdt,
                        "sfee": spot_fee_usdt,
                        "fx": exit_fx,
                        "pid": pid,
                    },
                )
                conn.commit()
                print(f"   ✅ Matched {symbol}: Net ${net_pnl:.4f}")
                updates += 1
            else:
                # If we can't find a file match, we leave it alone or log warning
                pass

        print(f"\n✨ Backfill Complete. Updated {updates} positions.")


if __name__ == "__main__":
    backfill()
