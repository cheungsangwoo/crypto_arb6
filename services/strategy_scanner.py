# services/strategy_scanner.py

import ccxt.async_support as ccxt
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import statistics
import logging

from database.models import StrategyCollector, MarketMetric
from database.session import SessionLocal
import services.hedger as hedger
from clients.base_spot_client import BaseSpotClient

logger = logging.getLogger(__name__)


class StrategyScanner:
    def __init__(self, spot_clients: Dict[str, BaseSpotClient]):
        self.spot_clients = spot_clients
        # Adjust for time diff is crucial for signature timestamps
        self.binance = ccxt.binance(
            {"options": {"defaultType": "future", "adjustForTimeDifference": True}}
        )

        # --- CONFIGURATION ---
        self.CONFIG_FILE = "bot_config.json"
        self._last_config_mtime = 0

        # Default Thresholds (conservative maker entry)
        self.THRESHOLDS = {
            "UPBIT": {"ENTRY": -0.5, "EXIT": 1.5},
            "BITHUMB": {"ENTRY": -0.7, "EXIT": 2.1},
            "COINONE": {"ENTRY": -0.3, "EXIT": 0.7},
        }

        self._reload_config()

        self.BLACKLIST = [
            "HOLO",
            "SNT",
            "WAVES",
            "NUC",
            "GXC",
            "GAS",
            "SOLVE",
            "BTC",
            "PAXG",
            "FIDA",
        ]

        self.SANITY_PREMIUM_LIMIT = -30.0

    def _reload_config(self):
        try:
            if not os.path.exists(self.CONFIG_FILE):
                return
            mtime = os.path.getmtime(self.CONFIG_FILE)
            if mtime == self._last_config_mtime:
                return

            with open(self.CONFIG_FILE, "r") as f:
                new_conf = json.load(f)

            for exch, values in new_conf.items():
                if exch not in self.THRESHOLDS:
                    self.THRESHOLDS[exch] = {}
                self.THRESHOLDS[exch].update(values)

            self._last_config_mtime = mtime
            logger.info(f"   🔄 HOT RELOAD: Updated Thresholds from {self.CONFIG_FILE}")
        except Exception as e:
            logger.error(f"   ⚠️ Config Reload Failed: {e}")

    async def _fetch_orderbooks_safe(self, client, symbols):
        try:
            return await client.fetch_orderbooks(symbols)
        except Exception as e:
            if datetime.now().second < 5:
                logger.warning(f"   ⚠️ {client.name} BULK FETCH FAILED: {str(e)}")

            results = {}
            for sym in symbols:
                try:
                    res_dict = await client.fetch_orderbooks([sym])
                    if res_dict:
                        results.update(res_dict)
                except Exception:
                    pass
                await asyncio.sleep(0.01)  # Avoid rate limit spam
            return results

    async def scan(self):
        self._reload_config()

        # --- 0. Fetch Binance Futures Symbols ---
        valid_futures = set()
        try:
            exchange_info = await self.binance.fapiPublicGetExchangeInfo()
            for s in exchange_info["symbols"]:
                if s["status"] == "TRADING":
                    valid_futures.add(s["symbol"])
        except Exception as e:
            logger.error(f"   ⚠️ Scanner Error (ExchangeInfo): {e}")
            return {}, 0.0

        # --- 1. Fetch Binance Data ---
        try:
            premium_index = await self.binance.fapiPublicGetPremiumIndex()
            # Fetch Best Bid/Ask for precise calculation
            book_tickers = await self.binance.fetch_bids_asks()

            book_map = {}
            for symbol, ticker in book_tickers.items():
                if "info" in ticker and "symbol" in ticker["info"]:
                    raw_sym = ticker["info"]["symbol"]
                    book_map[raw_sym] = ticker

            bin_data = {}
            for item in premium_index:
                raw_symbol = item["symbol"]
                if not raw_symbol.endswith("USDT"):
                    continue
                if raw_symbol not in valid_futures:
                    continue

                base = raw_symbol[:-4]
                mark_price = float(item["markPrice"])

                # Default to Mark Price if book is empty
                bid_price = mark_price
                ask_price = mark_price

                if raw_symbol in book_map:
                    bk = book_map[raw_symbol]
                    if bk.get("bid") and bk["bid"] > 0:
                        bid_price = float(bk["bid"])
                    if bk.get("ask") and bk["ask"] > 0:
                        ask_price = float(bk["ask"])

                bin_data[base] = {
                    "mark_price": mark_price,
                    "funding_rate": float(item["lastFundingRate"]),
                    "next_funding_time": datetime.fromtimestamp(
                        int(item["nextFundingTime"]) / 1000
                    ),
                    "bid_price": bid_price,  # We Sell Here (Hedge Entry)
                    "ask_price": ask_price,  # We Buy Here (Hedge Exit)
                }
        except Exception as e:
            logger.error(f"   ⚠️ Scanner Error (Binance): {e}")
            return {}, 0.0

        # --- 2. Iterate Spot Exchanges ---
        market_data = {}
        db_rows = []
        metrics_rows = []
        KST = timezone(timedelta(hours=9))
        timestamp = datetime.now(KST).replace(tzinfo=None)

        ref_ask = 1450.0

        for name, client in self.spot_clients.items():
            exch_entry_premiums = []
            exch_exit_premiums = []
            try:
                markets = await client.load_markets()
                krw_coins = [
                    m["market"].split("-")[1]
                    for m in markets
                    if m["market"].startswith("KRW-")
                ]

                target_coins = []
                upbit_to_binance = {v: k for k, v in hedger.SYMBOL_MAP.items()}

                for c in krw_coins:
                    if c in self.BLACKLIST:
                        continue
                    b_key = upbit_to_binance.get(c, c)
                    if b_key in bin_data:
                        target_coins.append((c, b_key))

                spot_pairs = [f"KRW-{c}" for c, _ in target_coins]
                if "KRW-USDT" not in spot_pairs:
                    spot_pairs.append("KRW-USDT")

                if not spot_pairs:
                    continue

                orderbooks = await self._fetch_orderbooks_safe(client, spot_pairs)
                if not orderbooks:
                    continue

                # Fetch USDT Ref
                usdt_book = orderbooks.get("KRW-USDT") or orderbooks.get("USDT/KRW")
                if usdt_book and usdt_book.get("ask", 0) > 0:
                    ref_ask = usdt_book["ask"]

                for coin, b_key in target_coins:
                    u_data = await client.get_orderbook_from_cache(orderbooks, coin)
                    if not u_data:
                        continue

                    b_data = bin_data[b_key]

                    # SPOT PRICES
                    u_bid = u_data["bid"]
                    u_ask = u_data["ask"]

                    # BINANCE PRICES
                    b_bid = b_data["bid_price"]  # Future Bid (Sell target)
                    b_ask = b_data["ask_price"]  # Future Ask (Buy target)
                    b_mark = b_data["mark_price"]

                    if u_bid <= 0 or u_ask <= 0 or b_bid <= 0 or b_ask <= 0:
                        continue

                    scaling = 1000.0 if b_key.startswith("1000") else 1.0
                    b_bid_scaled = b_bid / scaling
                    b_ask_scaled = b_ask / scaling

                    # --- ACTIVE MAKER PREMIUM CALCULATION ---
                    # Entry: We Buy Spot @ Bid, Sell Future @ Bid
                    # This is conservative. Real execution might be better if spread is tight.
                    implied_fx_entry = u_bid / b_bid_scaled
                    entry_premium = ((implied_fx_entry - ref_ask) / ref_ask) * 100

                    # Exit: We Sell Spot @ Ask, Buy Future @ Ask
                    implied_fx_exit = u_ask / b_ask_scaled
                    exit_premium = ((implied_fx_exit - ref_ask) / ref_ask) * 100

                    if entry_premium < self.SANITY_PREMIUM_LIMIT:
                        continue

                    valid_liquidity = (
                        u_ask * u_data.get("ask_size", 0) / ref_ask
                    ) > 50.0

                    exch_entry_premiums.append(entry_premium)
                    exch_exit_premiums.append(exit_premium)

                    if coin not in market_data:
                        market_data[coin] = {}

                    market_data[coin][name] = {
                        "symbol": coin,
                        "spot_exchange": name,
                        "entry_premium": entry_premium,
                        "exit_premium": exit_premium,
                        "spot_bid": u_bid,
                        "spot_ask": u_ask,
                        "binance_bid": b_bid_scaled,  # Crucial for Active Monitor
                        "binance_ask": b_ask_scaled,
                        "binance_symbol_key": b_key,
                        "funding_rate": b_data["funding_rate"],
                        "ref_fx": ref_ask,
                        "valid_liquidity": valid_liquidity,
                        "scaling": scaling,
                    }

                    db_rows.append(
                        StrategyCollector(
                            timestamp=timestamp,
                            symbol=coin,
                            spot_exchange=name,
                            spot_bid_price=u_bid,
                            spot_ask_price=u_ask,
                            binance_bid_price=b_bid,
                            binance_ask_price=b_ask,
                            binance_mark_price=b_data["mark_price"],
                            funding_rate=b_data["funding_rate"],
                            next_funding_time=b_data["next_funding_time"],
                            implied_fx=implied_fx_entry,
                            kimchi_premium_pct=entry_premium,
                            entry_premium_pct=entry_premium,
                            exit_premium_pct=exit_premium,
                            annualized_funding_pct=b_data["funding_rate"]
                            * 3
                            * 365
                            * 100,
                            ref_usdt_ask=ref_ask,
                        )
                    )

                if exch_entry_premiums:
                    metrics_rows.append(
                        MarketMetric(
                            timestamp=timestamp,
                            exchange=name,
                            median_entry_premium=statistics.median(exch_entry_premiums),
                            median_exit_premium=statistics.median(exch_exit_premiums),
                            opportunity_count=len(exch_entry_premiums),
                        )
                    )

            except Exception as e:
                logger.error(f"   ⚠️ Scanner Error ({client.name}): {e}")
                continue

        # --- SAVE TO DB ---
        if db_rows or metrics_rows:
            try:
                with SessionLocal() as db:
                    if db_rows:
                        db.bulk_save_objects(db_rows)
                    if metrics_rows:
                        db.bulk_save_objects(metrics_rows)
                    db.commit()
            except Exception as e:
                logger.error(f"   ⚠️ DB Save Error: {e}")

        return market_data, ref_ask

    async def close(self):
        await self.binance.close()
