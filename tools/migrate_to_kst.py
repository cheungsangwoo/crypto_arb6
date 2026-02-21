import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Path hack to load .env
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv(override=True)


def migrate_db_to_kst():
    print("🕰️  STARTING MIGRATION: UTC -> KST (+9 Hours)")

    db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_url)

    # List of tables and their datetime columns to shift
    tasks = [
        ("positions", ["entry_time", "exit_time"]),
        ("strategy_collector", ["timestamp", "next_funding_time"]),
        ("trade_log", ["timestamp"]),
        ("transfer_log", ["timestamp", "completed_at"]),
        ("portfolio_snapshots", ["timestamp"]),
        ("hedging_income", ["timestamp"]),
        ("exchange_rules", ["updated_at"]),
        ("optimization_history", ["timestamp"]),
    ]

    with engine.connect() as conn:
        for table, columns in tasks:
            print(f"   👉 Migrating table: {table}...")
            for col in columns:
                # SQL: UPDATE table SET col = DATE_ADD(col, INTERVAL 9 HOUR)
                try:
                    query = text(
                        f"UPDATE {table} SET {col} = DATE_ADD({col}, INTERVAL 9 HOUR) WHERE {col} IS NOT NULL"
                    )
                    result = conn.execute(query)
                    print(f"      - Shifted '{col}': {result.rowcount} rows updated.")
                except Exception as e:
                    print(f"      ⚠️ Skipped '{col}' (might not exist): {e}")

        conn.commit()

    print("\n✅ MIGRATION COMPLETE. All historical data is now KST.")


if __name__ == "__main__":
    migrate_db_to_kst()
