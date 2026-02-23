import os
import hmac
import hashlib
import base64
import json
import uuid
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
            "https://api.coinone.co.kr"  # Public endpoints also served here (public.coinone.co.kr doesn't resolve)
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

    def _build_v2_auth(self, params: Dict = None):
        """
        Build Coinone V2 authenticated request headers + body.
        Returns (headers, body_str) ready for session.post(url, headers=h, data=body).
        Auth: base64(JSON payload) in X-COINONE-PAYLOAD, HMAC-SHA512 in X-COINONE-SIGNATURE.
        Params are merged at the TOP LEVEL of the payload (not nested under "params").
        """
        payload = {
            "access_token": self.api_key,
            "nonce": str(uuid.uuid4()),
        }
        if params:
            payload.update(params)  # Merge directly — Coinone V2 does NOT use a "params" wrapper
        body_str = json.dumps(payload)
        encoded_payload = base64.b64encode(body_str.encode("utf-8"))
        signature = hmac.new(
            self.secret_key.encode("utf-8"), encoded_payload, hashlib.sha512
        ).hexdigest()
        headers = self._get_headers()
        headers["X-COINONE-PAYLOAD"] = encoded_payload.decode("utf-8")
        headers["X-COINONE-SIGNATURE"] = signature
        return headers, body_str

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
                    # logger.info(f"   📍 Trying Coinone endpoint: {url}")
                    res = requests.get(url, headers=self._get_headers(), timeout=5)
                    # logger.info(
                    #     f"      Response status: {res.status_code} (len: {len(res.text)} bytes)"
                    # )

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
                            # logger.info(f"   ✅ Coinone loaded {len(markets)} markets")
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

        url = f"{self.server_url}/ticker?currency={symbol_clean}&quote_currency=KRW"

        def _get():
            try:
                res = requests.get(url, headers=self._get_headers(), timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data and data.get("result") == "success":
                        return {"last": float(data.get("last", 0))}
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

        # One bulk call to ticker_new/KRW returns best_bids/best_asks for all coins
        def _get_bulk():
            try:
                url = f"{self.server_url}/public/v2/ticker_new/KRW"
                res = requests.get(url, headers=self._get_headers(), timeout=10)
                if res.status_code == 200:
                    return res.json()
                return None
            except Exception as e:
                logger.debug(f"Coinone bulk orderbook error: {e}")
                return None

        data = await asyncio.to_thread(_get_bulk)
        results = {}

        if not data or data.get("result") != "success":
            return results

        # Build lookup: UPPER(target_currency) -> ticker entry
        ticker_map = {
            t.get("target_currency", "").upper(): t
            for t in data.get("tickers", [])
        }

        # Filter to only the requested symbols
        requested = {s.replace("KRW-", "").upper() for s in formatted_symbols}

        for coin in requested:
            ticker = ticker_map.get(coin)
            if not ticker:
                continue
            best_asks = ticker.get("best_asks", [])
            best_bids = ticker.get("best_bids", [])
            if not best_asks or not best_bids:
                continue
            results[f"KRW-{coin}"] = {
                "bid": float(best_bids[0]["price"]),
                "ask": float(best_asks[0]["price"]),
                "bid_size": float(best_bids[0]["qty"]),
                "ask_size": float(best_asks[0]["qty"]),
                "timestamp": int(ticker.get("timestamp", int(time.time() * 1000))),
            }

        return results

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, float]]:
        """Fetch single orderbook."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        results = await self.fetch_orderbooks([market])
        return results.get(market)

    async def fetch_tickers(self) -> Dict[str, Dict[str, float]]:
        """
        Fetch all available tickers instantly using Coinone bulk API.
        """

        def _get():
            try:
                # Coinone bulk ticker endpoint (currency=all returns all coins)
                url = f"{self.server_url}/ticker?currency=all&quote_currency=KRW"
                res = requests.get(url, headers=self._get_headers(), timeout=5)
                if res.status_code == 200:
                    return res.json()
                return None
            except Exception as e:
                logger.debug(f"Coinone bulk ticker error: {e}")
                return None

        data = await asyncio.to_thread(_get)
        tickers = {}

        if data and data.get("result") == "success":
            # Response is a flat dict of currency -> ticker data
            skip_keys = {"result", "errorCode", "timestamp"}
            for sym, ticker in data.items():
                if sym in skip_keys or not isinstance(ticker, dict):
                    continue
                market = f"KRW-{sym.upper()}"
                last_price = float(ticker.get("last", 0))
                tickers[market] = {
                    "symbol": market,
                    "timestamp": int(time.time() * 1000),
                    "last": last_price,
                    "bid": last_price,
                    "ask": last_price,
                    "info": ticker,
                }

        return tickers

    async def fetch_balance(self) -> Dict[str, Dict[str, float]]:
        """Fetch account balance using V2.1 API."""

        def _get():
            try:
                import uuid
                import base64
                import json
                import hmac
                import hashlib

                path = "/v2.1/account/balance/all"
                url = f"{self.server_url}{path}"

                # 1. Prepare Payload (V2.1 uses UUID for nonce)
                payload = {"access_token": self.api_key, "nonce": str(uuid.uuid4())}

                # 2. Encode Payload to Base64
                dumped_json = json.dumps(payload)
                encoded_payload = base64.b64encode(dumped_json.encode("utf-8"))

                # 3. Generate Signature using SHA512 (V2.1 Requirement)
                signature = hmac.new(
                    self.secret_key.encode("utf-8"), encoded_payload, hashlib.sha512
                ).hexdigest()

                # 4. Set Headers
                headers = self._get_headers()
                headers["X-COINONE-PAYLOAD"] = encoded_payload.decode("utf-8")
                headers["X-COINONE-SIGNATURE"] = signature

                # 5. Send POST Request
                # (Use data=dumped_json to ensure exact byte match with signature)
                res = self.session.post(
                    url, headers=headers, data=dumped_json, timeout=5
                )

                if res.status_code == 200:
                    data = res.json()
                    if isinstance(data, dict) and data.get("result") == "success":
                        return data.get("balances", [])
                    else:
                        logger.error(f"Coinone balance API error: {data}")
                        return []
                else:
                    logger.debug(f"Coinone balance error {res.status_code}: {res.text}")
                    return []
            except Exception as e:
                logger.error(f"Coinone fetch_balance exception: {e}")
                return []

        try:
            balances_list = await asyncio.to_thread(_get)
            result = {}

            for item in balances_list:
                if not isinstance(item, dict):
                    continue

                curr = item.get("currency", "").upper()
                if not curr:
                    continue

                # Parse the v2.1 response structure
                free = float(item.get("available") or 0.0)
                locked = float(item.get("limit") or 0.0)

                result[curr] = {
                    "free": free,
                    "used": locked,
                    "total": free + locked,
                }

            if result:
                bal_str = ", ".join(
                    f"{k}:{v['total']:.2f}" for k, v in list(result.items())[:5]
                )
                # logger.info(f"   ✅ Coinone balance fetched: {bal_str}")

            return result
        except Exception as e:
            logger.error(f"Coinone Balance Exception: {e}")
            return {}

    @staticmethod
    def _fmt_price(price: float) -> str:
        """Format a KRW price for Coinone's API.
        For prices >= 1 KRW, send as integer (e.g. '423').
        For prices < 1 KRW, preserve decimal places (e.g. '0.423').
        Using int() on sub-1 prices would send '0', causing error 308.
        """
        if price >= 1:
            return str(int(price))
        return str(price)

    async def create_limit_buy_order(
        self, symbol: str, quantity: float, price: float
    ) -> Dict[str, Any]:
        """Create a limit buy order."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        target = market.replace("KRW-", "").upper()

        params = {
            "target_currency": target,
            "quote_currency": "KRW",
            "type": "LIMIT",
            "side": "BUY",
            "price": self._fmt_price(price),
            "qty": str(quantity),
            "post_only": False,
        }

        return await asyncio.to_thread(self._post_order, params)

    async def create_limit_sell_order(
        self, symbol: str, price: float, quantity: float
    ) -> Dict[str, Any]:
        """Create a limit sell order."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        target = market.replace("KRW-", "").upper()

        params = {
            "target_currency": target,
            "quote_currency": "KRW",
            "type": "LIMIT",
            "side": "SELL",
            "price": self._fmt_price(price),
            "qty": str(quantity),
            "post_only": False,
        }

        return await asyncio.to_thread(self._post_order, params)

    async def create_market_sell_order(
        self, symbol: str, quantity: float
    ) -> Dict[str, Any]:
        """Create a market sell order."""
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        target = market.replace("KRW-", "").upper()

        params = {
            "target_currency": target,
            "quote_currency": "KRW",
            "type": "MARKET",
            "side": "SELL",
            "qty": str(quantity),
        }

        return await asyncio.to_thread(self._post_order, params)

    def _post_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method to post an order to Coinone using V2 auth."""
        try:
            path = "/v2.1/order"
            url = f"{self.server_url}{path}"
            headers, body = self._build_v2_auth(params)

            res = self.session.post(url, headers=headers, data=body, timeout=5)

            if res.status_code >= 500:
                logger.warning(
                    f"Coinone Server Error ({res.status_code}). Order state unknown."
                )
                return {"id": "UNKNOWN_RECONCILE", "filled": 0.0, "raw": res.text}

            if res.status_code not in [200, 201]:
                logger.error(f"Coinone Order Failed: {res.text}")
                return {"id": "ERROR", "filled": 0.0, "raw": res.text}

            data = res.json()
            if data.get("result") == "success":
                order_id = data.get("order_id")
                return {"id": order_id, "filled": 0.0, "raw": data}
            else:
                err_code = data.get("error_code", "Unknown")
                symbol_hint = params.get("target_currency", "?")
                price_hint = params.get("price", "?")
                logger.error(
                    f"Coinone Order Error: {err_code} | {data.get('error_msg')} | "
                    f"symbol={symbol_hint} price={price_hint} | {data}"
                )
                return {"id": "ERROR", "filled": 0.0, "raw": data}

        except Exception as e:
            logger.error(f"Coinone Order Exception: {e}")
            return {"id": "ERROR", "filled": 0.0, "raw": str(e)}

    async def fetch_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        """
        Fetch order status using V2 auth (POST).
        Checks pending orders first; if not found there, checks completed orders.
        """

        def _check_open():
            """Check if order is still live in open_orders."""
            try:
                if symbol:
                    params = {
                        "target_currency": symbol.replace("KRW-", "").upper(),
                        "quote_currency": "KRW",
                    }
                    url = f"{self.server_url}/v2.1/order/open_orders"
                else:
                    params = {"quote_currency": "KRW"}
                    url = f"{self.server_url}/v2.1/order/open_orders/all"
                headers, body = self._build_v2_auth(params)
                res = self.session.post(url, headers=headers, data=body, timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("result") == "success":
                        orders = data.get("open_orders", data.get("orders", []))
                        for o in orders:
                            if o.get("order_id") == order_id:
                                return o
                return None
            except Exception as e:
                logger.debug(f"Coinone fetch_order (open) error: {e}")
                return None

        def _check_info():
            """Get order details via order/info — works for open, filled, and cancelled orders."""
            try:
                if not symbol:
                    return None  # order/info requires target_currency
                params = {
                    "order_id": order_id,
                    "target_currency": symbol.replace("KRW-", "").upper(),
                    "quote_currency": "KRW",
                }
                headers, body = self._build_v2_auth(params)
                url = f"{self.server_url}/v2.1/order/info"
                res = self.session.post(url, headers=headers, data=body, timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("result") == "success":
                        return data.get("order")
                return None
            except Exception as e:
                logger.debug(f"Coinone fetch_order (info) error: {e}")
                return None

        try:
            # 1. Check live open orders first (fast path)
            open_order = await asyncio.to_thread(_check_open)
            if open_order:
                filled = float(open_order.get("executed_qty", 0))
                remaining = float(open_order.get("remain_qty", 0))
                return {"id": order_id, "status": "open", "filled": filled, "remaining": remaining}

            # 2. Order not in open list — use order/info to get actual fill amount
            info = await asyncio.to_thread(_check_info)
            if info:
                filled = float(info.get("executed_qty", 0))
                remaining = float(info.get("remain_qty", 0))
                status = "open" if remaining > 0 else "closed"
                return {"id": order_id, "status": status, "filled": filled, "remaining": remaining}

            # 3. Fallback: treat as closed with 0 fill
            return {"id": order_id, "status": "closed", "filled": 0.0, "remaining": 0.0}

        except Exception as e:
            logger.error(f"Coinone Order Parse Error: {e}")
            return {"id": order_id, "status": "open", "filled": 0.0, "remaining": 1.0}

    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        """Cancel an order using V2 auth (POST)."""

        def _cancel():
            try:
                # cancel requires order_id + target_currency + quote_currency
                target = symbol.replace("KRW-", "").upper() if symbol else None
                if not target:
                    logger.warning(f"Coinone cancel_order: symbol required but not provided for order {order_id}")
                    return False
                params = {
                    "order_id": order_id,
                    "target_currency": target,
                    "quote_currency": "KRW",
                }
                headers, body = self._build_v2_auth(params)
                url = f"{self.server_url}/v2.1/order/cancel"
                res = self.session.post(url, headers=headers, data=body, timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("result") == "success":
                        return True
                    # 117 = Already Canceled — order is gone, treat as success
                    if data.get("error_code") == "117":
                        return True
                    logger.debug(f"Coinone cancel_order failed: {data.get('error_code')} {data.get('error_msg')}")
                return False
            except Exception as e:
                logger.debug(f"Coinone cancel_order error: {e}")
                return False

        return await asyncio.to_thread(_cancel)

    async def fetch_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Fetch open (pending) orders using V2 auth (POST)."""

        def _post():
            try:
                if symbol:
                    # Fetch for specific symbol
                    params = {
                        "target_currency": symbol.replace("KRW-", "").upper(),
                        "quote_currency": "KRW",
                    }
                    url = f"{self.server_url}/v2.1/order/open_orders"
                else:
                    # Fetch all open orders across all symbols
                    params = {"quote_currency": "KRW"}
                    url = f"{self.server_url}/v2.1/order/open_orders/all"
                headers, body = self._build_v2_auth(params)
                res = self.session.post(url, headers=headers, data=body, timeout=5)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("result") == "success":
                        # API returns key "open_orders" (not "orders")
                        return data.get("open_orders", data.get("orders", []))
                return []
            except Exception as e:
                logger.debug(f"Coinone fetch_open_orders error: {e}")
                return []

        try:
            orders = await asyncio.to_thread(_post)
            return [
                {
                    "id": o.get("order_id"),
                    "symbol": f"KRW-{o.get('target_currency', '').upper()}",
                    "status": "open",
                    "price": float(o.get("price", 0)),
                    "filled": float(o.get("executed_qty", 0)),
                    "remaining": float(o.get("remain_qty", 0)),
                    "side": o.get("side", "BUY"),
                }
                for o in orders
            ]
        except Exception as e:
            logger.error(f"Coinone Open Orders Parse Error: {e}")
            return []

    async def cancel_all_orders(self):
        """Cancel all open orders using the cancel/all endpoint (one call per trading pair)."""
        try:
            orders = await self.fetch_open_orders()
            if not orders:
                return

            # Group orders by target_currency and cancel the entire pair at once
            pairs_cancelled = set()
            for o in orders:
                sym = o.get("symbol", "")  # e.g. "KRW-XRP"
                target = sym.replace("KRW-", "").upper() if sym else None
                if not target or target in pairs_cancelled:
                    continue
                pairs_cancelled.add(target)

                def _cancel_pair(t=target):
                    try:
                        params = {"target_currency": t, "quote_currency": "KRW"}
                        headers, body = self._build_v2_auth(params)
                        url = f"{self.server_url}/v2.1/order/cancel/all"
                        res = self.session.post(url, headers=headers, data=body, timeout=5)
                        if res.status_code == 200:
                            data = res.json()
                            if data.get("result") == "success":
                                n = data.get("canceled_count", 0)
                                if n:
                                    logger.info(f"   Coinone: cancelled {n} {t}/KRW orders")
                                return True
                        return False
                    except Exception as e:
                        logger.debug(f"Coinone cancel_pair error ({t}): {e}")
                        return False

                await asyncio.to_thread(_cancel_pair)
                await asyncio.sleep(0.05)

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
                await self.cancel_order(order_id, symbol)
            raise

        except Exception as e:
            logger.error(f"Coinone Limit Order Exception: {e}")
            return {}

        # 3. Check & Cancel unfilled portion
        try:
            order_info = await self.fetch_order(order_id, symbol)
            remaining = float(order_info.get("remaining", 0.0))

            if remaining > 0:
                await self.cancel_order(order_id, symbol)
                await asyncio.sleep(0.2)
                order_info = await self.fetch_order(order_id, symbol)

            filled = float(order_info.get("filled", 0.0))
            avg_price = price  # Assume limit price if filled

            return {"id": order_id, "filled": filled, "avg_price": avg_price}

        except Exception as e:
            logger.error(f"Coinone IOC Check/Cancel Error: {e}")
            return {}

    async def close(self):
        """Close session."""
        self.session.close()
