from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any


class BaseSpotClient(ABC):
    """Abstract Base Class for all Spot Exchange Clients."""

    def __init__(self, name: str, api_key: str, secret_key: str):
        self.name = name.upper()
        self.api_key = api_key
        self.secret_key = secret_key

    # --- MARKET DATA ---
    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, float]:
        pass

    @abstractmethod
    async def fetch_orderbooks(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        pass

    async def fetch_orderbook(self, symbol: str) -> Optional[Dict[str, float]]:
        market = f"KRW-{symbol}"
        results = await self.fetch_orderbooks([market])
        return results.get(market)

    @abstractmethod
    async def fetch_balance(self) -> Dict[str, Dict[str, float]]:
        pass

    # --- ORDER EXECUTION ---
    @abstractmethod
    async def create_limit_buy_order(
        self, symbol: str, quantity: float, price: float
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def create_limit_sell_order(
        self, symbol: str, price: float, quantity: float
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def create_market_sell_order(
        self, symbol: str, quantity: float
    ) -> Dict[str, Any]:
        pass

    # --- MANAGEMENT ---
    @abstractmethod
    async def fetch_order(self, order_id: str, symbol: str = None) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str = None) -> bool:
        pass

    @abstractmethod
    async def fetch_open_orders(self, symbol: str = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def cancel_all_orders(self):
        """Cancels all open orders for this exchange."""
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def create_ioc_buy_order(
        self, symbol: str, price: float, quantity: float
    ) -> Dict[str, Any]:
        """
        Attempts to buy immediately at the given price/qty.
        Any portion not filled immediately is cancelled.
        Returns dict with 'filled' quantity and 'avg_price'.
        """
        pass
