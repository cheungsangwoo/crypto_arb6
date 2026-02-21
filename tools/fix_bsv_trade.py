import sys
import os
import asyncio
import logging
import jwt
import uuid
import hashlib
from urllib.parse import urlencode

# Add parent dir to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import ccxt.async_support as ccxt
from database.session import SessionLocal
from database.models import Position
from clients.upbit_client import UpbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Fixer")

load_dotenv()


def generate_upbit_headers(access_key, secret_key, query_string):
    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
    }

    if query_string:
        m = hashlib.sha512()
        m.update(query_string.encode("utf-8"))
        query_hash = m.hexdigest()
        payload["query_hash"] = query_hash
        payload["query_hash_alg"] = "SHA512"

    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    if isinstance(jwt_token, bytes):
        jwt_token = jwt_token.decode("utf-8")

    return {"Authorization": f"Bearer {jwt_token}", "Accept": "application/json"}


async def find_upbit_order_manual(client, symbol, qty_approx):
    market = f"KRW-{symbol}"
    params = {"state": "done", "market": market, "limit": 5, "order_by": "desc"}
    query_string = urlencode(params)
    headers = generate_upbit_headers(client.api_key, client.secret_key, query_string)
    url = f"{client.server_url}/v1/orders?{query_string}"

    logger.info(f"   🔎 [Upbit] Requesting: {url}")

    try:
        resp = client.session.get(url, headers=headers)
        data = resp.json()

        for o in data:
            vol = float(o.get("executed_volume", 0))
            if abs(vol - qty_approx) < 0.01:
                return o
        return None
    except Exception as e:
        logger.error(f"   ❌ [Upbit] Exception: {e}")
        return None


async def get_binance_order_direct(binance, symbol, order_id):
    try:
        # [FIX 1] Force Time Sync
        await binance.load_time_difference()

        logger.info(f"   🔎 [Binance] Fetching Order ID: {order_id}")
        order = await binance.fetch_order(order_id, f"{symbol}/USDT")
        logger.info(
            f"   ✅ FOUND Binance Order: {order['id']} | Avg Price: {order['average']}"
        )
        return order
    except Exception as e:
        logger.error(f"   ❌ [Binance] Fetch Error: {e}")
        return None


async def run_fix():
    logger.info("🔧 STARTING ONE-TIME FIX (V3)...")

    db = SessionLocal()
    pos = db.query(Position).filter(Position.symbol == "BSV").first()

    if not pos:
        logger.error("No BSV Position found!")
        return

    logger.info(f"   📝 Targeted Position ID: {pos.id}")

    upbit = UpbitClient(
        "UPBIT", os.getenv("UPBIT_ACCESS_KEY"), os.getenv("UPBIT_SECRET_KEY")
    )

    # [FIX 2] Relaxed recvWindow for timestamp issues
    binance = ccxt.binance(
        {
            "apiKey": os.getenv("BINANCE_HEDGE_API_KEY"),
            "secret": os.getenv("BINANCE_HEDGE_SECRET_KEY"),
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
                "recvWindow": 60000,  # Allow 60s drift
            },
        }
    )

    try:
        # Fetch Data
        u_order = await find_upbit_order_manual(upbit, "BSV", pos.entry_spot_qty)
        target_binance_id = "1915684175"
        b_order = await get_binance_order_direct(binance, "BSV", target_binance_id)

        changes_made = False

        if u_order:
            real_price = float(u_order.get("avg_price", 0))

            # [FIX 3] Fallback Calculation if executed_funds is 0
            executed_vol = float(u_order.get("executed_volume", 0))
            executed_funds = float(u_order.get("executed_funds", 0))

            if executed_funds == 0 and real_price > 0:
                logger.info("   ⚠️ Upbit API returned 0 funds. Calculating manually...")
                executed_funds = executed_vol * real_price

            pos.entry_spot_order_id = u_order["uuid"]

            if abs(pos.entry_spot_price - real_price) > 1:
                logger.info(
                    f"      🔹 Correcting Spot Price: {pos.entry_spot_price} -> {real_price}"
                )
                pos.entry_spot_price = real_price
                changes_made = True

            # Fix the 0.0 error
            if (
                executed_funds > 0
                and abs(pos.entry_spot_amount_krw - executed_funds) > 100
            ):
                logger.info(
                    f"      🔹 Correcting Spot Amount: {pos.entry_spot_amount_krw} -> {executed_funds:.2f}"
                )
                pos.entry_spot_amount_krw = executed_funds
                changes_made = True

        if b_order:
            real_price = float(b_order.get("average", 0))
            real_cost_usdt = float(b_order.get("cost", 0))

            pos.entry_hedge_order_id = str(b_order["id"])

            if abs(pos.entry_hedge_price - real_price) > 0.01:
                logger.info(
                    f"      🔹 Correcting Hedge Price: {pos.entry_hedge_price} -> {real_price}"
                )
                pos.entry_hedge_price = real_price
                changes_made = True

            if abs(pos.entry_hedge_amount_usdt - real_cost_usdt) > 0.5:
                logger.info(
                    f"      🔹 Correcting Hedge Amount: {pos.entry_hedge_amount_usdt} -> {real_cost_usdt}"
                )
                pos.entry_hedge_amount_usdt = real_cost_usdt
                changes_made = True

        if changes_made:
            db.commit()
            logger.info("✅ Database updated successfully.")
        else:
            # Commit IDs if found
            if u_order or b_order:
                db.commit()
                logger.info("✅ IDs updated. Values confirmed.")
            else:
                logger.warning("⚠️ No updates made.")

    except Exception as e:
        logger.error(f"Critical Error: {e}")
    finally:
        await upbit.close()
        await binance.close()
        db.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_fix())
