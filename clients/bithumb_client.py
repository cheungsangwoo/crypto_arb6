import os
import jwt
import uuid
import hashlib
import requests
import asyncio
import logging
import math
import time
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode, unquote
from clients.base_spot_client import BaseSpotClient
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)


class BithumbClient(BaseSpotClient):
    def __init__(self, name: str, access_key: str, secret_key: str):
        super().__init__(name, access_key.strip(), secret_key.strip())
        self.server_url = "https://api.bithumb.com"
        self.session = requests.Session()
        self.valid_markets = set()
        adapter = HTTPAdapter(
            pool_connections=20, pool_maxsize=50
        )  # Allow 50 parallel connections
        self.session.mount("https://", adapter)

    async def get_orderbook_from_cache(
        self, orderbooks: Dict, symbol: str
    ) -> Optional[Dict]:
        market_key = f"KRW-{symbol}"
        return orderbooks.get(market_key)

    def _get_headers(self, query_string=None):
        payload = {
            "access_key": self.api_key,
            "nonce": str(uuid.uuid4()),
            "timestamp": round(time.time() * 1000),
        }

        if query_string:
            m = hashlib.sha512()
            m.update(query_string.encode("utf-8"))
            query_hash = m.hexdigest()
            payload["query_hash"] = query_hash
            payload["query_hash_alg"] = "SHA512"

        jwt_token = jwt.encode(payload, self.secret_key, algorithm="HS256")

        # [NOTE] Requests usually handles bytes vs str automatically,
        # but jwt.encode returns str in newer PyJWT versions.
        if isinstance(jwt_token, bytes):
            jwt_token = jwt_token.decode("utf-8")

        return {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # --- MARKET DATA ---
    async def load_markets(self):
        url = f"{self.server_url}/v1/market/all?isDetails=false"

        def _get():
            try:
                res = requests.get(
                    url, headers={"Accept": "application/json"}, timeout=5
                )
                return res.json() if res.status_code == 200 else []
            except:
                return []

        markets = await asyncio.to_thread(_get)
        # Cache valid markets
        self.valid_markets = {
            m["market"] for m in markets if m["market"].startswith("KRW-")
        }
        return markets

    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        url = f"{self.server_url}/v1/ticker?markets={market}"

        def _get():
            try:
                res = requests.get(
                    url, headers={"Accept": "application/json"}, timeout=5
                )
                if res.status_code == 200:
                    data = res.json()
                    if data:
                        return {"last": float(data[0]["trade_price"])}
                return {"last": 0.0}
            except:
                return {"last": 0.0}

        return await asyncio.to_thread(_get)

    async def fetch_orderbooks(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        # This matches the working optimized fetcher from arb3
        if not symbols:
            return {}

        formatted = []
        for s in symbols:
            m = f"KRW-{s}" if not s.startswith("KRW-") else s
            if not self.valid_markets or m in self.valid_markets:
                formatted.append(m)

        if not formatted:
            return {}

        BATCH = 50
        results = {}
        chunks = [formatted[i : i + BATCH] for i in range(0, len(formatted), BATCH)]

        def _get_batch(batch_markets):
            markets_str = ",".join(batch_markets)
            url = f"{self.server_url}/v1/orderbook?markets={markets_str}"
            try:
                res = requests.get(
                    url, headers={"Accept": "application/json"}, timeout=5
                )
                return res.json() if res.status_code == 200 else []
            except:
                return []

        for chunk in chunks:
            data = await asyncio.to_thread(_get_batch, chunk)
            if isinstance(data, list):
                for item in data:
                    code = item["market"]
                    if not item.get("orderbook_units"):
                        continue
                    units = item["orderbook_units"][0]
                    results[code] = {
                        "bid": float(units["bid_price"]),
                        "ask": float(units["ask_price"]),
                        "bid_size": float(units["bid_size"]),
                        "ask_size": float(units["ask_size"]),
                        "timestamp": item["timestamp"],
                    }
        return results

    # --- MANAGEMENT ---
    async def fetch_balance(self) -> Dict[str, Dict[str, float]]:
        def _get():
            headers = self._get_headers()
            res = self.session.get(f"{self.server_url}/v1/accounts", headers=headers)

            # Check for HTTP errors (like 504 or 500)
            if res.status_code != 200:
                logger.error(f"Bithumb API Server Error {res.status_code}: {res.text}")
                return None

            return res.json()

        try:
            accounts = await asyncio.to_thread(_get)

            # VALIDATION: Ensure the response is actually a list of accounts
            if not isinstance(accounts, list):
                # If it's a dict, it's likely an error message from Bithumb
                error_msg = (
                    accounts.get("message")
                    if isinstance(accounts, dict)
                    else "Unknown Format"
                )
                logger.error(f"Bithumb Balance Error (Invalid Data): {error_msg}")
                return {}

            result = {}
            for acc in accounts:
                curr = acc["currency"]
                free = float(acc["balance"])
                locked = float(acc["locked"])
                result[curr] = {"free": free, "used": locked, "total": free + locked}
            return result
        except Exception as e:
            logger.error(f"Bithumb Balance Exception: {e}")
            return {}  # Return empty dict instead of crashing

    # --- ORDER EXECUTION ---
    async def create_limit_buy_order(
        self, symbol: str, quantity: float, price: float
    ) -> Dict[str, Any]:
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        params = {
            "market": market,
            "side": "bid",
            "ord_type": "limit",
            "price": str(price),
            "volume": str(quantity),
        }
        return await asyncio.to_thread(self._post_order, params)

    async def create_limit_sell_order(
        self, symbol: str, price: float, quantity: float
    ) -> Dict[str, Any]:
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        params = {
            "market": market,
            "side": "ask",
            "ord_type": "limit",
            "price": str(price),
            "volume": str(quantity),
        }
        return await asyncio.to_thread(self._post_order, params)

    async def create_market_sell_order(
        self, symbol: str, quantity: float
    ) -> Dict[str, Any]:
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        params = {
            "market": market,
            "side": "ask",
            "ord_type": "market",
            "volume": str(quantity),
        }
        return await asyncio.to_thread(self._post_order, params)

    def _post_order(self, params):
        # [CRITICAL] This matches the working arb3 logic: encode manually, then verify
        try:
            sorted_params = dict(sorted(params.items()))
            query_string = urlencode(sorted_params)
            headers = self._get_headers(query_string=query_string)

            res = self.session.post(
                f"{self.server_url}/v1/orders", json=sorted_params, headers=headers
            )

            if res.status_code >= 500:
                logger.warning(
                    f"Bithumb Server Error ({res.status_code}). Order state unknown."
                )
                return {"id": "UNKNOWN_RECONCILE", "filled": 0.0, "raw": res.text}

            if res.status_code not in [200, 201]:
                logger.error(f"Bithumb Order Failed: {res.text}")
                res.raise_for_status()

            data = res.json()
            return {"id": data.get("uuid"), "filled": 0.0, "raw": data}
        except Exception as e:
            logger.error(f"Bithumb Order Exception: {e}")
            raise e

    async def fetch_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        params = {"uuid": order_id}
        query_string = urlencode(params)
        headers = self._get_headers(query_string=query_string)

        def _get():
            url = f"{self.server_url}/v1/order?{query_string}"
            return self.session.get(url, headers=headers)

        res = await asyncio.to_thread(_get)
        if res.status_code == 404:
            return {"id": order_id, "status": "closed", "filled": 0, "remaining": 0}

        data = res.json()
        return {
            "id": data["uuid"],
            "status": data["state"],
            "filled": float(data.get("executed_volume", 0)),
            "remaining": float(data.get("remaining_volume", 0)),
        }

    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        params = {"uuid": order_id}
        query_string = urlencode(params)
        headers = self._get_headers(query_string=query_string)

        def _del():
            url = f"{self.server_url}/v1/order?{query_string}"
            return self.session.delete(url, headers=headers)

        res = await asyncio.to_thread(_del)
        return res.status_code in [200, 404]

    async def fetch_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        params = {"state": "wait"}
        if symbol:
            params["market"] = f"KRW-{symbol}" if "-" not in symbol else symbol

        query_string = urlencode(params)
        headers = self._get_headers(query_string=query_string)

        def _get():
            return self.session.get(
                f"{self.server_url}/v1/orders?{query_string}", headers=headers
            )

        res = await asyncio.to_thread(_get)
        if res.status_code != 200:
            return []

        orders = res.json()
        return [
            {
                "id": o["uuid"],
                "symbol": o["market"].replace("KRW-", ""),
                "status": o["state"],
                "price": float(o.get("price", 0)),
                "filled": float(o.get("executed_volume", 0)),
                "remaining": float(o.get("remaining_volume", 0)),
                "side": o["side"],
            }
            for o in orders
        ]

    async def cancel_all_orders(self):
        try:
            orders = await self.fetch_open_orders()
            if not orders:
                return
            for o in orders:
                await self.cancel_order(o["id"])
                # Bithumb rate limit safe-guard
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"   ❌ Bithumb Cancel All Error: {e}")

    # [FIX] Graceful Exit Version (Updated for JWT)
    async def create_ioc_buy_order(self, symbol, price, quantity):
        try:
            # 1. Place Limit Buy
            res = await self.create_limit_buy_order(symbol, quantity, price)
            order_id = res.get("id") or res.get("uuid")
            if not order_id:
                return {}
        except Exception:
            return {}

        try:
            # 2. Wait
            await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            # Safety Cancel on Shutdown
            logger.warning(f"⚠️ IOC Interrupted! Cancelling {symbol}...")
            await self.cancel_order(order_id, symbol)
            raise

        # 3. Check & Cancel
        try:
            order_info = await self.fetch_order(order_id)
            remaining = float(order_info.get("remaining", 0.0))

            if remaining > 0:
                await self.cancel_order(order_id)
                await asyncio.sleep(0.2)
                order_info = await self.fetch_order(order_id)

            filled = float(order_info.get("filled", 0.0))

            # Since this is the "New API", trades list might be available for avg_price
            # but for speed, we assume limit price if filled.
            avg_price = price

            return {"id": order_id, "filled": filled, "avg_price": avg_price}
        except:
            return {}

    async def close(self):
        self.session.close()
