import asyncio
import os
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Add parent directory to path to find .env if run from tools/
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

load_dotenv(override=True)


async def fetch_hedging_pnl(days=30):
    print(f"🔍 CCXT Version: {ccxt.__version__}")

    api_key = os.getenv("BINANCE_HEDGE_API_KEY")
    secret = os.getenv("BINANCE_HEDGE_SECRET_KEY")

    if not api_key or not secret:
        print("❌ Error: BINANCE_HEDGE_API_KEY or SECRET not found in .env")
        return

    print(f"🔄 Connecting to Binance Futures (Raw API Mode)...")

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": secret,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,  # [FIX] Prevents -1021 Time Sync Errors
            },
        }
    )

    try:
        # Calculate start time
        start_dt = datetime.now(timezone.utc) - timedelta(days=days)
        start_ts = int(start_dt.timestamp() * 1000)

        print("📥 Fetching income history (Direct /fapi/v1/income)...")

        income_data = await exchange.fapiPrivateGetIncome(
            {"startTime": start_ts, "limit": 1000}
        )

        if not income_data:
            print("⚠️ No hedging data found for this period.")
            return

        df = pd.DataFrame(income_data)

        # Rename and Convert
        df.rename(columns={"income": "amount", "time": "timestamp"}, inplace=True)
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["amount"] = df["amount"].astype(float)

        # --- REPORT 1: SYMBOL SUMMARY (High Level) ---
        print(f"\n📊 SUMMARY REPORT ({days} Days)")
        print("=" * 60)
        summary = (
            df.groupby(["symbol", "incomeType"])["amount"].sum().unstack(fill_value=0)
        )
        for col in ["REALIZED_PNL", "COMMISSION", "FUNDING_FEE"]:
            if col not in summary.columns:
                summary[col] = 0.0
        summary["NET_PROFIT"] = (
            summary["REALIZED_PNL"] + summary["COMMISSION"] + summary["FUNDING_FEE"]
        )
        summary = summary.sort_values(by="NET_PROFIT", ascending=True)

        pd.options.display.float_format = "{:,.4f}".format
        print(summary[["REALIZED_PNL", "COMMISSION", "FUNDING_FEE", "NET_PROFIT"]])
        print("-" * 60)
        print(f"💰 GRAND TOTAL: {summary['NET_PROFIT'].sum():,.4f} USDT")
        print("=" * 60)

        # --- REPORT 2: TRANSACTION BUCKETS (Detailed) ---
        print(f"\n🔬 DETAILED TRANSACTION BUCKETS (Trade-by-Trade)")
        print("=" * 80)

        # Filter for actual trades (PnL + Commission)
        trades_df = df[df["incomeType"].isin(["REALIZED_PNL", "COMMISSION"])].copy()

        if not trades_df.empty:
            trade_buckets = (
                trades_df.groupby(["symbol", "tradeId"])
                .agg(
                    {"amount": "sum", "datetime": "max", "incomeType": lambda x: set(x)}
                )
                .reset_index()
            )

            trade_buckets.rename(columns={"amount": "NET_TRADE_PNL"}, inplace=True)
            trade_buckets = trade_buckets.sort_values(by="datetime", ascending=False)

            print(
                trade_buckets[["datetime", "symbol", "tradeId", "NET_TRADE_PNL"]].head(
                    30
                )
            )

            filename = "hedging_trade_breakdown.csv"
            trade_buckets.to_csv(filename, index=False)
            print(f"\n✅ Detailed Trade Breakdown saved to: {filename}")
        else:
            print("No closed trades found yet.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await exchange.close()


if __name__ == "__main__":
    asyncio.run(fetch_hedging_pnl(days=30))
