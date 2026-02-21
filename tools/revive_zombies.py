import os
import sys

# Add parent directory to path so we can import database modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.session import SessionLocal
from database.models import Position


def revive_zombies():
    print("🧟 Finding ZOMBIE (LOST) positions to revive...")

    with SessionLocal() as db:
        # Find all positions marked as LOST in the last 24 hours
        zombies = db.query(Position).filter(Position.status == "LOST").all()

        if not zombies:
            print("✅ No zombies found. Your database looks clean.")
            return

        print(f"⚠️ Found {len(zombies)} zombies. Reviving them now...")

        count = 0
        for pos in zombies:
            print(
                f"   💉 Reviving {pos.symbol} ({pos.spot_exchange}) - Qty: {pos.spot_qty}"
            )
            pos.status = "OPEN"
            pos.exit_time = None
            pos.exit_premium = None
            count += 1

        db.commit()
        print(f"✅ Successfully revived {count} positions. Restart the bot now.")


if __name__ == "__main__":
    revive_zombies()
