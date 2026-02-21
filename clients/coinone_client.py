import os
import hmac
import hashlib
import base64
import json
import requests
import asyncio
import logging
import time
from typing import Dict, List, Any, Optional
from urllib.parse import quote
from clients.base_spot_client import BaseSpotClient
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)


class CoinoneClient(BaseSpotClient):
    def __init__(self, name: str, access_key: str, secret_key: str):
        super().__init__(name, access_key.strip(), secret_key.strip())
        # Coinone public API endpoint (different from trading API)
        self.server_url = "https://api.coinone.co.kr"
        self.public_url = (
            "https://public.coinone.co.kr"  # Public endpoints use different URL
        )
        self.session = requests.Session()
        self.valid_markets = set()
        adapter = HTTPAdapter(
            pool_connections=20, pool_maxsize=50
        )  # Allow 50 parallel connections
        self.session.mount("https://", adapter)

    async def get_orderbook_from_cache(
        self, orderbooks: Dict, symbol: str
    ) -> Optional[Dict]:
        """Extract orderbook data for a symbol from cache."""
        market_key = f"KRW-{symbol}"
        return orderbooks.get(market_key)

    def _generate_payload(
        self, method: str, path: str, params: Dict = None
    ) -> Dict[str, Any]:
        """Generate authenticated request payload for Coinone API."""
        if params is None:
            params = {}

        # Build nonce and request body
        nonce = str(int(time.time() * 1000))
        params_json = json.dumps(params)

        # Create signature
        message = method + path + params_json + nonce
        signature = hmac.new(
            self.secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        return {
            "access_token": self.api_key,
            "nonce": nonce,
            "signature": signature,
            "params": params,
        }

    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for Coinone API."""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def load_markets(self):
        """Load available markets from Coinone."""
        # Coinone API: only api.coinone.co.kr works, and requires /KRW parameter
        endpoints = [
            f"{self.server_url}/public/v2/markets/KRW",  # Correct endpoint with KRW market
            f"{self.server_url}/public/v2/markets",  # Fallback without KRW (less specific)
        ]

        def _get():
            for url in endpoints:
                try:
                    logger.info(f"   📍 Trying Coinone endpoint: {url}")
                    res = requests.get(url, headers=self._get_headers(), timeout=5)
                    logger.info(
                        f"      Response status: {res.status_code} (len: {len(res.text)} bytes)"
                    )

                    if res.status_code == 200:
                        # Check if response is actually JSON
                        if not res.text or res.text.strip() == "":
                            logger.info(f"      ⚠️ Empty response - skipping")
                            continue

                        if res.text.startswith("<"):
                            logger.info(f"      ⚠️ Got HTML instead of JSON - skipping")
                            continue

                        try:
                            data = res.json()
                        except ValueError as je:
                            logger.info(f"      ⚠️ Failed to parse JSON: {je}")
                            continue

                        logger.debug(
                            f"Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}"
                        )

                        markets = []

                        # Extract markets from Coinone's response format
                        market_list = data.get("markets", [])

                        for market in market_list:
                            if isinstance(market, dict):
                                # Coinone format: quote_currency and target_currency fields
                                quote = market.get("quote_currency", "").upper()
                                target = market.get("target_currency", "").upper()

                                if quote == "KRW" and target:
                                    market_name = f"KRW-{target}"
                                    markets.append({"market": market_name})

                        if markets:
                            logger.info(f"   ✅ Coinone loaded {len(markets)} markets")
                            return markets

                        logger.info(f"      ⚠️ Got 200 but no markets found")
                        continue
                    else:
                        logger.info(f"      ⚠️ Status {res.status_code}")
                        continue

                except Exception as e:
                    logger.info(f"      ⚠️ Exception: {str(e)[:100]}")
                    continue

            # All endpoints failed
            logger.warning("   ⚠️ All Coinone market endpoints failed")
            return None

        markets = await asyncio.to_thread(_get)

        # If API fails, use a fallback list of major coins commonly traded on Coinone
        if not markets:
            logger.warning("   ⚠️ Coinone API unavailable, using fallback market list")
            fallback_coins = [
                "BTC",
                "ETH",
                "XRP",
                "BCH",
                "LTC",
                "ETC",
                "DOGE",
                "ADA",
                "SOL",
                "AVAX",
                "MATIC",
                "ATOM",
                "LINK",
                "USDT",
                "USDC",
            ]
            markets = [{"market": f"KRW-{coin}"} for coin in fallback_coins]

        # Cache valid markets
        self.valid_markets = {m["market"] for m in markets} if markets else set()
        logger.debug(f"Coinone has {len(self.valid_markets)} valid markets")
        return markets

    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        """Fetch ticker price for a symbol."""
        # Handle multiple format inputs: "BTC", "BTC/KRW", "KRW-BTC", etc.
        if "/" in symbol:
            symbol = symbol.split("/")[0]  # "BTC/KRW" -> "BTC"
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        symbol_clean = market.replace("KRW-", "").lower()

        url = f"{self.public_url}/public/v2/ticker?currency={symbol_clean}&quote_currency=krw"

        def _get():
            try:
                res = requests.get(url, headers=self._get_headers(), timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data and "tickers" in data and len(data["tickers"]) > 0:
                        ticker = data["tickers"][0]
                        return {"last": float(ticker.get("last", 0))}
                return {"last": 0.0}
            except Exception as e:
                logger.debug(f"Coinone fetch_ticker error: {e}")
                return {"last": 0.0}

        return await asyncio.to_thread(_get)

    async def fetch_orderbooks(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        """Fetch orderbooks for multiple symbols."""
        if not symbols:
            return {}

        formatted_symbols = []
        for s in symbols:
            if not s.startswith("KRW-"):
                s = f"KRW-{s}"
            formatted_symbols.append(s)

        if not formatted_symbols:
            return {}

        results = {}

        # Fetch them one by one to avoid rate limits
        for market in formatted_symbols:
            try:
                symbol_clean = market.replace("KRW-", "").lower()
                url = f"{self.public_url}/public/v2/orderbook?currency={symbol_clean}&quote_currency=krw"

                def _get_ob():
                    try:
                        res = requests.get(url, headers=self._get_headers(), timeout=5)
                        if res.status_code == 200:
                            data = res.json()

                            # Coinone response format
                            if isinstance(data, dict) and "orderbook" in data:
                                ob = data["orderbook"]
                                if ob.get("asks") and ob.get("bids"):
                                    # asks/bids are lists of [price, qty] or dicts
                                    asks = ob["asks"]
                                    bids = ob["bids"]

                                    # Handle both list and dict formats
                                    ask_price = float(
                                        asks[0]["price"]
                                        if isinstance(asks[0], dict)
                                        else asks[0][0]
                                    )
                                    ask_qty = float(
                                        asks[0]["qty"]
                                        if isinstance(asks[0], dict)
                                        else asks[0][1]
                                    )
                                    bid_price = float(
                                        bids[0]["price"]
                                        if isinstance(bids[0], dict)
                                        else bids[0][0]
                                    )
                                    bid_qty = float(
                                        bids[0]["qty"]
                                        if isinstance(bids[0], dict)
                                        else bids[0][1]
                                    )

                                    return {
                                        "bid": bid_price,
                                        "ask": ask_price,
                                        "bid_size": bid_qty,
                                        "ask_size": ask_qty,
                                        "timestamp": data.get(
                                            "timestamp", int(time.time() * 1000)
                                        ),
                                    }
                        return None
                    except Exception as e:
                        logger.debug(f"Coinone orderbook error for {market}: {e}")
                        return None

                ob_data = await asyncio.to_thread(_get_ob)
                if ob_data:
                    results[market] = ob_data

                # Small delay to avoid rate limits
                await asyncio.sleep(0.05)

            except Exception as e:
                logger.debug(f"Coinone orderbook exception for {market}: {e}")
                continue

        return results

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, float]]:
        """Fetch single orderbook."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        results = await self.fetch_orderbooks([market])
        return results.get(market)

    async def fetch_tickers(self) -> Dict[str, Dict[str, float]]:
        """
        Fetch all available tickers in CCXT format.
        Returns dict with market symbols as keys and ticker data as values.
        """
        # Get all available markets first
        await self.load_markets()

        # Fetch orderbooks for all markets to generate ticker data
        if not self.valid_markets:
            return {}

        orderbooks = await self.fetch_orderbooks(list(self.valid_markets))

        tickers = {}
        for market, ob in orderbooks.items():
            if ob and "bid" in ob and "ask" in ob:
                # Use mid price as last price
                last_price = (ob["bid"] + ob["ask"]) / 2
                tickers[market] = {
                    "symbol": market,
                    "timestamp": ob.get("timestamp", int(time.time() * 1000)),
                    "last": last_price,
                    "bid": ob["bid"],
                    "ask": ob["ask"],
                    "info": ob,
                }

        return tickers

    async def fetch_balance(self) -> Dict[str, Dict[str, float]]:
        """Fetch account balance."""

        def _get():
            try:
                # Try the standard Coinone v2 API endpoint
                nonce = str(int(time.time() * 1000))

                # Create request signature for private endpoint
                message = "GET" + "/v2/account/balance" + nonce
                signature = hmac.new(
                    self.secret_key.encode("utf-8"),
                    message.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()

                # Try with Authorization header
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                    "X-COINONE-SIGNATURE": signature,
                    "X-COINONE-NONCE": nonce,
                }

                url = f"{self.server_url}/v2/account/balance"
                res = self.session.get(url, headers=headers, timeout=5)

                logger.debug(
                    f"Coinone balance API status: {res.status_code}, len: {len(res.text)}"
                )

                # DEBUG: Log raw API response
                logger.info(f"   🔍 DEBUG COINONE fetch_balance: API URL: {url}")
                logger.info(
                    f"   🔍 DEBUG COINONE fetch_balance: Status: {res.status_code}, Response length: {len(res.text)}"
                )
                if len(res.text) > 0:
                    logger.info(
                        f"   🔍 DEBUG COINONE fetch_balance: First 500 chars: {res.text[:500]}"
                    )

                if res.status_code == 200:
                    # Check if response is actually JSON
                    if not res.text or res.text.strip() == "":
                        logger.warning("Coinone balance API returned empty response")
                        logger.info(f"   🔍 DEBUG COINONE: Empty text response")
                        return []

                    try:
                        data = res.json()
                    except ValueError as je:
                        logger.error(f"Coinone balance API returned non-JSON: {je}")
                        logger.error(
                            f"Response text (first 200 chars): {res.text[:200]}"
                        )
                        logger.debug(f"Full response: {res.text}")
                        return []

                    logger.debug(f"Coinone balance response type: {type(data)}")
                    logger.info(
                        f"   🔍 DEBUG COINONE: JSON parsed type: {type(data)}, value: {data if isinstance(data, dict) else f'list with {len(data)} items'}"
                    )

                    if isinstance(data, dict):
                        if data.get("success"):
                            result = data.get("balances", [])
                            logger.info(
                                f"   🔍 DEBUG COINONE: Dict response 'success'=True, balances: {result}"
                            )
                            return result
                        elif "balances" in data:
                            result = data.get("balances", [])
                            logger.info(
                                f"   🔍 DEBUG COINONE: Dict response has 'balances', returning: {result}"
                            )
                            return result
                        else:
                            logger.info(
                                f"   🔍 DEBUG COINONE: Dict response but no expected fields. Keys: {list(data.keys())}"
                            )
                            return []
                    elif isinstance(data, list):
                        logger.debug(
                            f"Coinone balance returned list with {len(data)} items"
                        )
                        logger.info(
                            f"   🔍 DEBUG COINONE: Returnning list directly: {len(data)} items"
                        )
                        return data

                    logger.debug(f"Coinone balance response structure not recognized")
                    return []
                else:
                    logger.debug(
                        f"Coinone balance returned {res.status_code}: {res.text[:200]}"
                    )
                    logger.info(
                        f"   🔍 DEBUG COINONE: Non-200 status: {res.status_code}"
                    )
                    return []
            except Exception as e:
                logger.error(f"Coinone fetch_balance exception: {e}")
                logger.info(
                    f"   🔍 DEBUG COINONE: Exception in _get(): {type(e).__name__}: {str(e)}"
                )
                import traceback

                logger.debug(traceback.format_exc())
                return []

        try:
            balances_list = await asyncio.to_thread(_get)

            # DEBUG: Log the raw response from the balance API
            logger.info(
                f"   🔍 DEBUG COINONE: fetch_balance raw response type: {type(balances_list)}, length: {len(balances_list) if isinstance(balances_list, (list, dict)) else 'N/A'}"
            )
            if isinstance(balances_list, list) and balances_list:
                logger.info(
                    f"   🔍 DEBUG COINONE: First balance item: {balances_list[0]}"
                )

            result = {}

            if not isinstance(balances_list, list):
                logger.debug(f"Coinone balances response type: {type(balances_list)}")
                logger.info(f"   🔍 DEBUG COINONE: Not a list, returning empty dict")
                return {}

            if not balances_list:
                logger.debug("Coinone balance list is empty")
                logger.info(f"   🔍 DEBUG COINONE: Balance list is empty!")
                return {}

            for item in balances_list:
                if not isinstance(item, dict):
                    continue

                try:
                    # Handle both possible field names for currency
                    curr = (item.get("currency") or item.get("coin_code") or "").upper()

                    if not curr:
                        continue

                    # Handle various possible field names for balance amounts
                    free = float(
                        item.get("avail_balance")
                        or item.get("available_balance")
                        or item.get("balance")
                        or 0
                    )
                    locked = float(item.get("hold_balance") or item.get("onOrder") or 0)

                    result[curr] = {
                        "free": free,
                        "used": locked,
                        "total": free + locked,
                    }

                    # DEBUG: Log each parsed item
                    if curr == "KRW":
                        logger.info(
                            f"   🔍 DEBUG COINONE: Parsed KRW - free: {free}, locked: {locked}, total: {free + locked}"
                        )

                except (ValueError, TypeError) as e:
                    logger.debug(f"Failed to parse balance for {curr}: {e}")
                    continue

            if result:
                bal_str = ", ".join(
                    f"{k}:{v['total']:.2f}" for k, v in list(result.items())[:5]
                )
                logger.info(f"   ✅ Coinone balance fetched: {bal_str}")
            else:
                logger.info(f"   🔍 DEBUG COINONE: Result dict is empty after parsing")

            return result
        except Exception as e:
            logger.error(f"Coinone Balance Exception: {e}")
            logger.info(
                f"   🔍 DEBUG COINONE: Exception occurred: {type(e).__name__}: {str(e)}"
            )
            return {}

    async def create_limit_buy_order(
        self, symbol: str, quantity: float, price: float
    ) -> Dict[str, Any]:
        """Create a limit buy order."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        symbol_clean = market.replace("KRW-", "").lower()

        params = {
            "currency": symbol_clean,
            "quote_currency": "krw",
            "price": str(int(price)),
            "qty": str(quantity),
            "side": "buy",
        }

        return await asyncio.to_thread(self._post_order, params)

    async def create_limit_sell_order(
        self, symbol: str, price: float, quantity: float
    ) -> Dict[str, Any]:
        """Create a limit sell order."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        symbol_clean = market.replace("KRW-", "").lower()

        params = {
            "currency": symbol_clean,
            "quote_currency": "krw",
            "price": str(int(price)),
            "qty": str(quantity),
            "side": "sell",
        }

        return await asyncio.to_thread(self._post_order, params)

    async def create_market_sell_order(
        self, symbol: str, quantity: float
    ) -> Dict[str, Any]:
        """Create a market sell order."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        symbol_clean = market.replace("KRW-", "").lower()

        params = {
            "currency": symbol_clean,
            "quote_currency": "krw",
            "qty": str(quantity),
            "side": "sell",
        }

        return await asyncio.to_thread(self._post_order, params)

    def _post_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method to post an order to Coinone."""
        try:
            path = "/v2/order/create"
            payload = self._generate_payload("POST", path, params)

            url = f"{self.server_url}{path}"
            headers = self._get_headers()

            res = self.session.post(url, headers=headers, json=payload)

            if res.status_code >= 500:
                logger.warning(
                    f"Coinone Server Error ({res.status_code}). Order state unknown."
                )
                return {"id": "UNKNOWN_RECONCILE", "filled": 0.0, "raw": res.text}

            if res.status_code not in [200, 201]:
                logger.error(f"Coinone Order Failed: {res.text}")
                return {"id": "ERROR", "filled": 0.0, "raw": res.text}

            data = res.json()
            if data.get("success"):
                return {"id": data.get("order_id"), "filled": 0.0, "raw": data}
            else:
                logger.error(f"Coinone Order Error: {data.get('errorCode', 'Unknown')}")
                return {"id": "ERROR", "filled": 0.0, "raw": data}

        except Exception as e:
            logger.error(f"Coinone Order Exception: {e}")
            return {"id": "ERROR", "filled": 0.0, "raw": str(e)}

    async def fetch_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        """Fetch order status."""

        def _get():
            try:
                path = "/v2/order/complete"
                params = {"order_id": order_id}
                payload = self._generate_payload("GET", path, params)

                url = f"{self.server_url}{path}"
                headers = self._get_headers()

                res = self.session.get(url, headers=headers, params=payload)

                if res.status_code == 200:
                    data = res.json()
                    if data.get("success"):
                        return data
                return None
            except Exception as e:
                logger.debug(f"Coinone fetch_order error: {e}")
                return None

        try:
            order_data = await asyncio.to_thread(_get)

            if not order_data:
                return {"id": order_id, "status": "closed", "filled": 0, "remaining": 0}

            # Map Coinone status to standard format
            status_map = {
                "PENDING": "open",
                "COMPLETED": "closed",
                "CANCELED": "closed",
                "PARTIALLY_COMPLETED": "open",
            }

            coinone_status = order_data.get("order_status", "PENDING")
            filled = float(order_data.get("filled", 0))

            return {
                "id": order_id,
                "status": status_map.get(coinone_status, coinone_status),
                "filled": filled,
                "remaining": float(order_data.get("remaining", 0)),
            }
        except Exception as e:
            logger.error(f"Coinone Order Parse Error: {e}")
            return {"id": order_id, "status": "unknown", "filled": 0, "remaining": 0}

    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an order."""

        def _delete():
            try:
                path = "/v2/order/cancel"
                params = {"order_id": order_id}
                payload = self._generate_payload("POST", path, params)

                url = f"{self.server_url}{path}"
                headers = self._get_headers()

                res = self.session.post(url, headers=headers, json=payload)

                return res.status_code in [200, 404]
            except Exception as e:
                logger.debug(f"Coinone cancel_order error: {e}")
                return False

        return await asyncio.to_thread(_delete)

    async def fetch_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Fetch open orders."""

        def _get():
            try:
                path = "/v2/order/pending"
                params = {}

                if symbol:
                    symbol_clean = symbol.replace("KRW-", "").lower()
                    params["currency"] = symbol_clean
                    params["quote_currency"] = "krw"

                payload = self._generate_payload("GET", path, params)

                url = f"{self.server_url}{path}"
                headers = self._get_headers()

                res = self.session.get(url, headers=headers, params=payload)

                if res.status_code == 200:
                    data = res.json()
                    if data.get("success"):
                        return data.get("orders", [])
                return []
            except Exception as e:
                logger.debug(f"Coinone fetch_open_orders error: {e}")
                return []

        try:
            orders = await asyncio.to_thread(_get)
            result = []

            for o in orders:
                result.append(
                    {
                        "id": o.get("order_id"),
                        "symbol": f"KRW-{o.get('currency', '').upper()}",
                        "status": o.get("order_status", "PENDING"),
                        "price": float(o.get("price", 0)),
                        "filled": float(o.get("filled", 0)),
                        "remaining": float(o.get("remaining", 0)),
                        "side": o.get("side", "buy"),
                    }
                )

            return result
        except Exception as e:
            logger.error(f"Coinone Open Orders Parse Error: {e}")
            return []

    async def cancel_all_orders(self):
        """Cancel all open orders."""
        try:
            orders = await self.fetch_open_orders()
            if not orders:
                return
            for o in orders:
                await self.cancel_order(o["id"])
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"   ❌ Coinone Cancel All Error: {e}")

    async def create_ioc_buy_order(
        self, symbol: str, price: float, quantity: float
    ) -> Dict[str, Any]:
        """
        Immediate-or-Cancel buy order.
        Places a limit order, waits briefly, then cancels any unfilled portion.
        """
        order_id = None
        try:
            # 1. Place Limit Buy
            res = await self.create_limit_buy_order(symbol, quantity, price)
            order_id = res.get("id")
            if not order_id or order_id == "ERROR":
                logger.error(f"Failed to place base limit order: {res}")
                return {}

            # 2. Wait for Matching
            await asyncio.sleep(1.5)

        except asyncio.CancelledError:
            # Graceful shutdown
            if order_id:
                logger.warning(
                    f"⚠️ Shutdown detected! Cancelling pending Coinone order: {order_id}"
                )
                await self.cancel_order(order_id)
            raise

        except Exception as e:
            logger.error(f"Coinone Limit Order Exception: {e}")
            return {}

        # 3. Check & Cancel unfilled portion
        try:
            order_info = await self.fetch_order(order_id)
            remaining = float(order_info.get("remaining", 0.0))

            if remaining > 0:
                await self.cancel_order(order_id)
                await asyncio.sleep(0.2)
                order_info = await self.fetch_order(order_id)

            filled = float(order_info.get("filled", 0.0))
            avg_price = price  # Assume limit price if filled

            return {"id": order_id, "filled": filled, "avg_price": avg_price}

        except Exception as e:
            logger.error(f"Coinone IOC Check/Cancel Error: {e}")
            return {}

    async def close(self):
        """Close session."""
        self.session.close()
