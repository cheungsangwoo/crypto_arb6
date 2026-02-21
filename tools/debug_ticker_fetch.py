import asyncio
import ccxt.async_support as ccxt
import os
import json
from dotenv import load_dotenv

load_dotenv(override=True)

# List of coins that are failing in your logs
TEST_COINS = [
    ("UPBIT", ["LSK", "ADA", "A", "ZRX", "RVN", "SAND"]),
    ("BITHUMB", ["P", "ONG", "HEMI", "DEXE", "YB", "KITE"]),
]


async def debug_exchange(name, coins):
    print(f"\n🔬 DIAGNOSING {name}...")

    # Initialize Client
    api_key = os.getenv(f"{name}_ACCESS_KEY")
    secret = os.getenv(f"{name}_SECRET_KEY")

    # Standard CCXT Config
    config = {
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
        "options": {"adjustForTimeDifference": True},
    }

    try:
        if name == "UPBIT":
            exchange = ccxt.upbit(config)
        else:
            exchange = ccxt.bithumb(config)

        print(f"   ✅ Connected to {name}")

        # TEST 1: BULK FETCH (fetch_tickers)
        print(f"   [1] Testing Bulk Fetch (fetch_tickers)...")
        try:
            tickers = await exchange.fetch_tickers()
            print(f"       🎉 Success! Received {len(tickers)} tickers.")

            # Check if our coins exist in the bulk data
            found_count = 0
            for coin in coins:
                # Try common keys
                keys_to_check = [f"{coin}/KRW", f"KRW-{coin}", coin]
                found = False
                for k in keys_to_check:
                    if k in tickers:
                        t = tickers[k]
                        price = (
                            t.get("last")
                            or t.get("close")
                            or t.get("info", {}).get("trade_price")
                        )
                        print(f"       ✅ Found {coin} as '{k}': {price}")
                        found = True
                        found_count += 1
                        break
                if not found:
                    print(
                        f"       ❌ {coin} NOT FOUND in bulk data. (Keys checked: {keys_to_check})"
                    )

            if found_count == len(coins):
                print(f"       ✨ ALL COINS FOUND via Bulk Fetch. This is the fix.")
                await exchange.close()
                return

        except Exception as e:
            print(f"       ❌ Bulk Fetch Failed: {e}")

        # TEST 2: INDIVIDUAL FETCH (If Bulk Failed)
        print(f"   [2] Testing Individual Fetches...")
        for coin in coins:
            print(f"       --- Probing {coin} ---")

            # Formats to try
            formats = [f"{coin}/KRW", f"KRW-{coin}"]
            success = False

            for symbol in formats:
                try:
                    t = await exchange.fetch_ticker(symbol)
                    print(f"          Key: '{symbol}' -> OK")
                    print(
                        f"          Raw Info: {json.dumps(t.get('info', {}))[:100]}..."
                    )  # Print start of raw JSON

                    price = t.get("last")
                    if not price:
                        # Dig into Raw
                        info = t.get("info", {})
                        if "trade_price" in info:
                            price = info["trade_price"]
                        elif "closing_price" in info:
                            price = info["closing_price"]

                    print(f"          Price Extracted: {price}")
                    success = True
                    break  # Stop trying formats if one works

                except Exception as e:
                    print(f"          Key: '{symbol}' -> Failed ({str(e)})")

            if not success:
                print(
                    f"       🚨 IMPOSSIBLE TO PRICE {coin} (Is it delisted? BTC Pair only?)"
                )

    except Exception as e:
        print(f"   🔥 CRITICAL CONNECTION ERROR: {e}")
    finally:
        if exchange:
            await exchange.close()


async def main():
    await debug_exchange(*TEST_COINS[0])  # Test Upbit
    await debug_exchange(*TEST_COINS[1])  # Test Bithumb


if __name__ == "__main__":
    asyncio.run(main())
