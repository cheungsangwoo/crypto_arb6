import ccxt.async_support as ccxt
import asyncio
from datetime import datetime
from database.session import SessionLocal
from database.models import ExchangeRules
from services.hedger import SYMBOL_MAP
import logging

logger = logging.getLogger(__name__)


class RuleManager:
    def __init__(self, spot_clients=None):
        self.binance = ccxt.binance({"options": {"defaultType": "future"}})
        self.spot_exchanges = {
            "UPBIT": ccxt.upbit(),
            "BITHUMB": ccxt.bithumb(),
        }
        self.auth_clients = spot_clients or {}

    # Inside rule_manager.py
    async def close(self):
        await self.binance.close()
        for ex in self.spot_exchanges.values():
            await ex.close()

    async def sync_rules(self):
        """Robustly sync actual Binance and Spot exchange rules to the database."""
        logger.info("   ⏳ Syncing Exchange Rules (CCXT Unified + Spot 8-Decimal)...")
        try:
            # 1. Load Binance Markets
            await self.binance.load_markets()

            # 2. Iterate and Extract Actual Limits
            for symbol, market in self.binance.markets.items():
                if not market.get("active"):
                    continue

                # Use CCXT's pre-parsed unified 'limits' dictionary
                # This automatically extracts LOT_SIZE, MARKET_LOT_SIZE, etc.
                bin_step = market["precision"]["amount"]  # Unified precision
                bin_tick = market["precision"]["price"]  # Unified price tick

                # Fetch minimum requirements
                limits = market.get("limits", {})
                bin_min_qty = limits.get("amount", {}).get("min", 1.0)
                bin_min_notional = limits.get("cost", {}).get("min", 10.0)

                # Fallback to defaults if values are missing or None
                bin_step = float(bin_step) if bin_step is not None else 1.0
                bin_tick = float(bin_tick) if bin_tick is not None else 0.01
                bin_min_qty = float(bin_min_qty) if bin_min_qty is not None else 1.0
                bin_min_notional = (
                    float(bin_min_notional) if bin_min_notional is not None else 10.0
                )

                # 3. Save to Database for all spot exchanges
                for exch_name in ["UPBIT", "BITHUMB", "COINONE"]:
                    with SessionLocal() as db:
                        # Use an 'upsert' pattern (update if exists, otherwise insert)
                        rule = (
                            db.query(ExchangeRules)
                            .filter_by(symbol=symbol, spot_exchange=exch_name)
                            .first()
                        )

                        if not rule:
                            rule = ExchangeRules(symbol=symbol, spot_exchange=exch_name)
                            db.add(rule)

                        rule.binance_step_size = bin_step
                        rule.binance_tick_size = bin_tick
                        rule.binance_min_qty = bin_min_qty
                        rule.binance_min_notional = bin_min_notional

                        # Keep standard spot 8-decimal rule
                        rule.spot_step_size = (
                            0.00000001 if exch_name == "UPBIT" else 0.001
                        )
                        rule.spot_min_notional = 5000.0
                        rule.updated_at = datetime.now()

                        db.commit()

            logger.info("   ✅ Rule Sync Complete with actual exchange limits.")

        except Exception as e:
            logger.error(f"   ❌ Rule Sync Error: {e}")

    def get_rules_map(self):
        with SessionLocal() as db:
            all_rules = db.query(ExchangeRules).all()
            return {(r.symbol, r.spot_exchange): r for r in all_rules}
