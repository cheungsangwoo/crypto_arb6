from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to DB
DB_URL = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)


def reset_bot_state():
    with engine.connect() as conn:
        print("🧹 Cleaning Bot State...")

        # 1. WIPE INVENTORY
        # This forces the bot to re-scan your wallets from scratch next time.
        conn.execute(text("DELETE FROM inventory"))
        print("✅ Inventory Table: Wiped.")

        # 2. CLOSE PENDING TRANSFERS
        # Since you are handling the current transfers manually, we mark them as
        # 'MANUAL_FIX' so the bot doesn't try to resume monitoring them.
        conn.execute(
            text(
                "UPDATE transfer_log SET status = 'MANUAL_FIX' WHERE status = 'PENDING'"
            )
        )
        print("✅ Pending Transfers: Marked as MANUAL_FIX.")

        # 3. OPTIONAL: Clear old optimization history to force a fresh strategy search
        # conn.execute(text("DELETE FROM optimization_history"))
        # print("✅ Optimization History: Cleared.")

        conn.commit()
        print("🚀 Database ready for fresh start.")


if __name__ == "__main__":
    reset_bot_state()
