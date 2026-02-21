import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta, timezone

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from database.session import SessionLocal
from database.models import Position
from clients.upbit_client import UpbitClient
from clients.bithumb_client import BithumbClient

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO)


def get_kst_now():
    return datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None)


async def import_positions():
    print("🔍 Scanning Exchanges for ALL Positions (Debug Mode)...")

    clients = []
    if os.getenv("UPBIT_ACCESS_KEY"):
        clients.append(
            UpbitClient(
                "UPBIT", os.getenv("UPBIT_ACCESS_KEY"), os.getenv("UPBIT_SECRET_KEY")
            )
        )
    if os.getenv("BITHUMB_ACCESS_KEY"):
        clients.append(
            BithumbClient(
                "BITHUMB",
                os.getenv("BITHUMB_ACCESS_KEY"),
                os.getenv("BITHUMB_SECRET_KEY"),
            )
        )

    if not clients:
        print("❌ No clients configured.")
        return

    with SessionLocal() as db:
        # Get active positions to avoid duplicates
        active_db_rows = db.query(Position).filter(Position.status == "OPEN").all()
        tracked_set = {(p.spot_exchange, p.symbol) for p in active_db_rows}

        print(f"   📋 Database currently tracks {len(tracked_set)} positions.")
        new_positions = []

        for client in clients:
            print(f"\n   📡 Scanning {client.name} wallet...")
            try:
                balances = await client.fetch_balance()

                # Filter for ALL non-zero assets (excluding KRW/USDT)
                # We do NOT filter by value yet, just quantity
                held_symbols = [
                    sym
                    for sym, bal in balances.items()
                    if bal["total"] > 0 and sym not in ["KRW", "USDT"]
                ]

                if not held_symbols:
                    print(f"      - No active coin holdings found on {client.name}.")
                    continue

                print(f"      found {len(held_symbols)} assets: {held_symbols}")

                for currency in held_symbols:
                    # [FILTER] Explicitly ignore XRP on Bithumb
                    if currency == "XRP" and client.name == "BITHUMB":
                        print(f"      - Ignoring XRP on Bithumb (Manual Exclusion)")
                        continue

                    qty = balances[currency]["total"]

                    # Fetch Ticker individually to be safe
                    price = 0.0
                    try:
                        ticker = await client.fetch_ticker(currency)
                        if ticker:
                            price = ticker["last"]
                        await asyncio.sleep(0.1)  # Prevent rate limiting
                    except Exception as e:
                        print(f"      ⚠️ Failed to fetch price for {currency}: {e}")

                    value_krw = qty * price

                    # Check if already tracked
                    if (client.name, currency) in tracked_set:
                        print(f"      - {currency}: {qty} (Already tracked)")
                        continue

                    # FORCE IMPORT: Even if price is 0, import it so we don't lose it.
                    # We only skip if quantity is effectively zero.
                    if qty < 0.000001:
                        continue

                    status_msg = "✅ IMPORTING"
                    if value_krw < 2000:
                        status_msg = "⚠️ IMPORTING DUST"

                    print(
                        f"      {status_msg}: {currency} | Qty: {qty} | Price: {price:,.0f} | Val: {value_krw:,.0f} KRW"
                    )

                    pos = Position(
                        symbol=currency,
                        spot_exchange=client.name,
                        status="OPEN",
                        entry_time=get_kst_now(),
                        entry_premium=0.0,
                        spot_qty=qty,
                        binance_qty=0.0,
                        # If price is 0, cost is 0. Bot will fix PnL once price updates.
                        entry_cost_usdt=(value_krw / 1450.0) if price > 0 else 0.0,
                        commission_usdt=0.0,
                        funding_fee_usdt=0.0,
                        net_pnl_usdt=0.0,
                    )
                    new_positions.append(pos)
                    tracked_set.add((client.name, currency))

            except Exception as e:
                print(f"      ❌ Error scanning {client.name}: {e}")

        if new_positions:
            print(f"\n📥 Importing {len(new_positions)} positions into Database...")
            db.add_all(new_positions)
            db.commit()
            print("   ✅ Import Complete!")
        else:
            print("\n✅ No new orphans found.")

    for c in clients:
        await c.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(import_positions())
