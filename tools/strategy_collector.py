import ccxt.async_support as ccxt
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict

from database.models import StrategyCollector
from database.session import SessionLocal
from services.hedger import SYMBOL_MAP
from clients.base_spot_client import BaseSpotClient


class StrategyScanner:
    def __init__(self, spot_clients: List[BaseSpotClient]):
        self.spot_clients = spot_clients
        self.binance = ccxt.binance(
            {"options": {"defaultType": "future", "adjustForTimeDifference": True}}
        )
        self.ENTRY_THRESHOLD = -0.4
        self.EXIT_THRESHOLD = -0.1
        self.BLACKLIST = ["HOLO", "SNT", "WAVES", "NUC", "GXC", "GAS", "SOLVE"]
        self.SANITY_PREMIUM_LIMIT = -30.0

    async def scan(self) -> Dict[str, Dict]:
        # 1. Fetch Binance
        try:
            premium_index = await self.binance.fapiPublicGetPremiumIndex()
            tickers = await self.binance.fetch_tickers()
            bin_data = {}
            for item in premium_index:
                sym = item["symbol"]
                if not sym.endswith("USDT"):
                    continue
                base = sym[:-4]
                ticker = tickers.get(sym, {})
                bid = float(ticker.get("bid", 0) or 0)
                if bid > 0:
                    bin_data[base] = {
                        "mark_price": float(item["markPrice"]),
                        "funding_rate": float(item["lastFundingRate"]),
                        "next_funding_time": datetime.fromtimestamp(
                            int(item["nextFundingTime"]) / 1000
                        ),
                        "bid_price": bid,
                    }
        except Exception as e:
            print(f"   ⚠️ Scanner Error (Binance Fetch): {e}")
            return {}

        # 2. Iterate Spot Exchanges
        market_snapshot = {}
        db_rows = []
        KST = timezone(timedelta(hours=9))
        timestamp = datetime.now(KST).replace(tzinfo=None)
        spot_to_binance = {v: k for k, v in SYMBOL_MAP.items()}

        for client in self.spot_clients:
            try:
                # [FIX] Filter Target List: Only ask for coins this client supports
                # This prevents "Invalid Symbol" errors that cause empty batches
                valid_targets = []

                # Check if client has loaded markets yet
                if not hasattr(client, "valid_markets") or not client.valid_markets:
                    await client.load_markets()

                for spot_coin, bin_future_key in spot_to_binance.items():
                    if spot_coin in self.BLACKLIST:
                        continue
                    if bin_future_key not in bin_data:
                        continue

                    # Verify market exists on this specific exchange
                    market_pair = f"KRW-{spot_coin}"
                    if market_pair in client.valid_markets:
                        valid_targets.append(spot_coin)

                if not valid_targets:
                    print(
                        f"   ⚠️ Debug: {client.name} has no valid targets matching Binance."
                    )
                    continue

                # Get Orderbooks
                orderbooks = await client.fetch_orderbooks(valid_targets)

                # Process
                for coin in valid_targets:
                    ticker_key = f"KRW-{coin}"
                    if ticker_key not in orderbooks:
                        continue

                    u_data = orderbooks[ticker_key]
                    b_key = spot_to_binance.get(coin, coin)
                    b_data = bin_data[b_key]

                    u_bid = u_data["bid"]
                    u_ask = u_data["ask"]
                    b_mark = b_data["mark_price"]

                    if u_bid <= 0 or u_ask <= 0 or b_mark <= 0:
                        continue

                    if b_key.startswith("1000"):
                        b_mark = b_mark / 1000.0

                    # Assume standard FX if not fetched per-loop (optimization)
                    ref_ask = 1450.0

                    implied_fx_bid = u_bid / b_mark
                    entry_premium = ((implied_fx_bid - ref_ask) / ref_ask) * 100

                    if entry_premium < self.SANITY_PREMIUM_LIMIT:
                        continue

                    opp_data = {
                        "spot_exchange": client.name,
                        "symbol": coin,
                        "premium": entry_premium,
                        "spot_bid": u_bid,
                        "binance_mark": b_mark,
                        "binance_symbol_key": b_key,
                        "funding_rate": b_data["funding_rate"],
                        "ref_fx": ref_ask,
                    }

                    if (
                        coin not in market_snapshot
                        or entry_premium < market_snapshot[coin]["premium"]
                    ):
                        market_snapshot[coin] = opp_data

                    db_rows.append(
                        StrategyCollector(
                            timestamp=timestamp,
                            spot_exchange=client.name,
                            symbol=coin,
                            spot_bid_price=u_bid,
                            spot_ask_price=u_ask,
                            spot_bid_size=u_data.get("bid_size", 0),
                            binance_bid_price=b_data["bid_price"],
                            binance_mark_price=b_data["mark_price"],
                            funding_rate=b_data["funding_rate"],
                            next_funding_time=b_data["next_funding_time"],
                            implied_fx=implied_fx_bid,
                            kimchi_premium_pct=entry_premium,
                            annualized_funding_pct=b_data["funding_rate"]
                            * 3
                            * 365
                            * 100,
                            ref_usdt_ask=ref_ask,
                        )
                    )

            except Exception as e:
                print(f"   ⚠️ Scanner Error ({client.name}): {e}")
                continue

        # 3. Save
        if db_rows:
            try:
                with SessionLocal() as db:
                    db.bulk_save_objects(db_rows)
                    db.commit()
                print(
                    f"   💾 Logged {len(db_rows)} records from {len(self.spot_clients)} exchanges."
                )
            except Exception as e:
                print(f"   ⚠️ DB Save Error: {e}")

        return market_snapshot

    async def close(self):
        await self.binance.close()
