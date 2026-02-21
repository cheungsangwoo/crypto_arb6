import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv(override=True)


def check_anomalies():
    # Connect to DB
    db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)

    print("🔍 Scanning for Data Anomalies...")

    # 1. Check for Massive Premiums
    # [FIX] Removed '%' signs from comments to prevent Python string formatting errors
    query = """
        SELECT timestamp, symbol, upbit_bid_price, binance_mark_price, kimchi_premium_pct
        FROM strategy_collector
        WHERE kimchi_premium_pct > 20.0  -- Flag anything gt 20 pct premium
           OR kimchi_premium_pct < -50.0 -- Flag anything lt -50 pct discount
        ORDER BY kimchi_premium_pct DESC
        LIMIT 20
    """

    try:
        df = pd.read_sql(query, engine)

        if not df.empty:
            print("\n🚨 FOUND EXTREME PREMIUM SPIKES:")
            print(df.to_string(index=False))
            print("\n⚠️  DIAGNOSIS: These rows are triggering fake 'Jackpot' exits.")
            print(
                "   Action: These timestamps likely have bad data (e.g. Price=0 or Price=Infinity)."
            )
        else:
            print("\n✅ No extreme premium spikes found (> 20 or < -50).")

    except Exception as e:
        print(f"❌ Error running query: {e}")


if __name__ == "__main__":
    check_anomalies()
