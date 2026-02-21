import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

load_dotenv(override=True)


def cleanup_dust():
    print("🧹 Cleaning Dust from Database...")

    # 1. Connect to DB
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "crypto_arb3")

    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(db_url)

    # 2. Define Dust Threshold (e.g. $5 USDT)
    DUST_THRESHOLD_USDT = 4.0

    with engine.connect() as conn:
        # Check what we are about to delete
        query_check = text(
            f"SELECT symbol, entry_cost_usdt FROM positions WHERE status='OPEN' AND entry_cost_usdt < {DUST_THRESHOLD_USDT}"
        )
        rows = conn.execute(query_check).fetchall()

        if not rows:
            print("   ✅ No dust positions found.")
            return

        print(f"   ⚠️ Found {len(rows)} dust positions (< ${DUST_THRESHOLD_USDT}):")
        for r in rows:
            print(f"      - {r[0]}: ${r[1]:.4f}")

        # Execute Delete
        confirm = input(
            "\n   🗑️  Delete these records from DB? The coins will remain in your wallet. (y/n): "
        )
        if confirm.lower() == "y":
            query_delete = text(
                f"DELETE FROM positions WHERE status='OPEN' AND entry_cost_usdt < {DUST_THRESHOLD_USDT}"
            )
            result = conn.execute(query_delete)
            conn.commit()
            print(f"   ✅ Deleted {result.rowcount} rows. Slots freed up.")
        else:
            print("   ❌ Cancelled.")


if __name__ == "__main__":
    cleanup_dust()
