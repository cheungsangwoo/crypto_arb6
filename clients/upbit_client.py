import jwt
import uuid
import hashlib
import os
import requests
from urllib.parse import urlencode, unquote
import logging
import asyncio
from typing import Dict, List, Any, Optional
from clients.base_spot_client import BaseSpotClient
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)


class UpbitClient(BaseSpotClient):
    def __init__(self, name: str, access_key: str, secret_key: str):
        super().__init__(name, access_key.strip(), secret_key.strip())
        self.server_url = "https://api.upbit.com"
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

    def _get_headers(self, query=None):
        payload = {
            "access_key": self.api_key,
            "nonce": str(uuid.uuid4()),
        }
        if query:
            if isinstance(query, dict):
                sorted_items = sorted(query.items())
                q_str = unquote(urlencode(sorted_items, doseq=True)).encode("utf-8")
            else:
                q_str = query.encode("utf-8")

            m = hashlib.sha512()
            m.update(q_str)
            query_hash = m.hexdigest()
            payload["query_hash"] = query_hash
            payload["query_hash_alg"] = "SHA512"

        token = jwt.encode(payload, self.secret_key, algorithm="HS256")
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        return {"Authorization": f"Bearer {token}"}

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

        return await asyncio.to_thread(_get)

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
                    if data and isinstance(data, list):
                        return {"last": float(data[0]["trade_price"])}
                return {"last": 0.0}
            except:
                return {"last": 0.0}

        return await asyncio.to_thread(_get)

    async def fetch_orderbooks(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        if not symbols:
            return {}
        BATCH_SIZE = 100
        results = {}
        formatted_symbols = []
        for s in symbols:
            if not s.startswith("KRW-"):
                s = f"KRW-{s}"
            formatted_symbols.append(s)
        chunks = [
            formatted_symbols[i : i + BATCH_SIZE]
            for i in range(0, len(formatted_symbols), BATCH_SIZE)
        ]

        def _get_batch(batch_symbols):
            markets_str = ",".join(batch_symbols)
            url = f"{self.server_url}/v1/orderbook?markets={markets_str}"
            try:
                res = requests.get(
                    url, headers={"Accept": "application/json"}, timeout=5
                )
                return res.json() if res.status_code == 200 else []
            except Exception:
                return []

        for chunk in chunks:
            data = await asyncio.to_thread(_get_batch, chunk)
            if isinstance(data, list):
                for item in data:
                    code = item["market"]
                    units = item["orderbook_units"][0]
                    results[code] = {
                        "bid": float(units["bid_price"]),
                        "ask": float(units["ask_price"]),
                        "bid_size": float(units["bid_size"]),
                        "ask_size": float(units["ask_size"]),
                        "timestamp": item["timestamp"],
                    }
        return results

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, float]]:
        market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
        results = await self.fetch_orderbooks([market])
        return results.get(market)

    async def fetch_balance(self) -> Dict[str, Dict[str, float]]:
        def _get():
            headers = self._get_headers()
            res = self.session.get(f"{self.server_url}/v1/accounts", headers=headers)
            res.raise_for_status()
            return res.json()

        try:
            accounts = await asyncio.to_thread(_get)
            result = {}
            for acc in accounts:
                curr = acc["currency"]
                free = float(acc["balance"])
                locked = float(acc["locked"])
                result[curr] = {"free": free, "used": locked, "total": free + locked}
            return result
        except Exception as e:
            logger.error(f"Upbit Balance Error: {e}")
            raise e

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
        try:
            sorted_params = dict(sorted(params.items()))
            headers = self._get_headers(sorted_params)
            res = self.session.post(
                f"{self.server_url}/v1/orders", json=sorted_params, headers=headers
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Order Exception: {e}")
            raise e

    async def fetch_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        params = {"uuid": order_id}
        headers = self._get_headers(params)

        def _get():
            return self.session.get(
                f"{self.server_url}/v1/order", params=params, headers=headers
            )

        res = await asyncio.to_thread(_get)
        if res.status_code == 404:
            return {
                "id": order_id,
                "status": "closed",
                "filled": 0,
                "remaining": 0,
                "trades": [],
            }
        data = res.json()
        trades = data.get("trades", [])
        return {
            "id": data["uuid"],
            "status": data["state"],
            "filled": float(data.get("executed_volume", 0)),
            "remaining": float(data.get("remaining_volume", 0)),
            "trades": trades,
        }

    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        params = {"uuid": order_id}
        headers = self._get_headers(params)
        res = await asyncio.to_thread(
            self.session.delete,
            f"{self.server_url}/v1/order",
            params=params,
            headers=headers,
        )
        return res.status_code in [200, 404]

    # [FIXED] Now filters by symbol to find specific stuck orders
    async def fetch_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        url = f"{self.server_url}/v1/orders"
        params = {"state": "wait", "page": 1}

        if symbol:
            market = f"KRW-{symbol}" if not symbol.startswith("KRW-") else symbol
            params["market"] = market

        headers = self._get_headers(params)
        res = await asyncio.to_thread(
            self.session.get, url, params=params, headers=headers
        )
        if res.status_code != 200:
            return []
        return [
            {"id": o["uuid"], "symbol": o["market"], "status": "open"}
            for o in res.json()
        ]

    async def cancel_all_orders(self):
        try:
            orders = await self.fetch_open_orders()
            if not orders:
                return
            for o in orders:
                await self.cancel_order(o["id"])
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"   ❌ Upbit Cancel All Error: {e}")

    async def close(self):
        self.session.close()

    async def create_ioc_buy_order(self, symbol, price, quantity):
        # 1. Place Standard Limit Order
        order_id = None
        try:
            res = await self.create_limit_buy_order(symbol, quantity, price)
            order_id = res.get("uuid")
            if not order_id:
                logger.error(f"   ❌ Failed to place base limit order: {res}")
                return {}

            # 2. Wait for Matching (Critical Window)
            await asyncio.sleep(1.5)

        except asyncio.CancelledError:
            # 🛑 INTERRUPT CAUGHT (Graceful Exit)
            if order_id:
                logger.warning(
                    f"   ⚠️ Shutdown detected! Cancelling pending Upbit order: {order_id}"
                )
                await self.cancel_order(order_id)
            raise  # Re-raise to allow shutdown to continue

        except Exception as e:
            logger.error(f"   ❌ Limit Order Exception: {e}")
            return {}

        # 3. Check & Cancel (Normal flow)
        try:
            # Fetch status first
            order_info = await self.fetch_order(order_id)
            remaining = float(order_info.get("remaining", 0.0))

            # If not fully filled, cancel the remainder
            if remaining > 0:
                await self.cancel_order(order_id)
                await asyncio.sleep(0.2)
                order_info = await self.fetch_order(order_id)

            filled = float(order_info.get("filled", 0.0))

            # 4. Calculate Weighted Average Price from Trades
            trades = order_info.get("trades", [])
            avg_price = price

            if trades:
                total_vol = sum(float(t.get("volume", 0)) for t in trades)
                total_val = sum(
                    float(t.get("volume", 0)) * float(t.get("price", 0)) for t in trades
                )
                if total_vol > 0:
                    avg_price = total_val / total_vol

            return {"id": order_id, "filled": filled, "avg_price": avg_price}

        except Exception as e:
            logger.error(f"   ⚠️ Simulated IOC Error (Check/Cancel): {e}")
            return {}
