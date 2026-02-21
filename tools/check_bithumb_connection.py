import asyncio
import ccxt.async_support as ccxt
import os
import math
from datetime import datetime, timedelta, timezone
from typing import Dict

# DB Imports
from database.models import Position, PortfolioSnapshot
from database.session import SessionLocal

# Services
from services.rule_manager import RuleManager
from services.hedger import SYMBOL_MAP
from clients.base_spot_client import BaseSpotClient

import logging

logger = logging.getLogger(__name__)


def truncate(f, n=8):
    if f == 0:
        return 0.0
    factor = 10**n
    return math.floor(f * factor) / factor


# Config
DRY_RUN = False  # Set to True to collect data without trading
MIN_TRADE_VALUE_KRW = 5000.0


class PositionManager:
    def __init__(
        self,
        spot_clients: Dict[str, BaseSpotClient],
        shutdown_event: asyncio.Event = None,
    ):
        self.spot_clients = spot_clients
        self.binance = ccxt.binance(
            {
                "apiKey": os.getenv("BINANCE_HEDGE_API_KEY"),
                "secret": os.getenv("BINANCE_HEDGE_SECRET_KEY"),
                "options": {"defaultType": "future", "adjustForTimeDifference": True},
            }
        )
        self.BATCH_SIZE_USDT = 100.0
        self.MAX_POSITIONS = 1
        self.rule_manager = RuleManager(spot_clients=self.spot_clients)
        self.rules = {}
        self.shutdown_event = shutdown_event
        self.exchange_slots = {"UPBIT": 0, "BITHUMB": 0}
        self.locks = {}
        self.MIN_TRADE_VALUE_KRW = MIN_TRADE_VALUE_KRW

        asyncio.create_task(self._ensure_binance_hedge_mode())

    async def _ensure_binance_hedge_mode(self):
        try:
            await self.binance.fapiPrivatePostPositionSideDual(
                {"dualSidePosition": "true"}
            )
        except Exception:
            pass

    def get_symbol_lock(self, symbol: str):
        if symbol not in self.locks:
            self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    def get_kst_now(self):
        return datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None)

    async def close(self):
        if self.binance:
            await self.binance.close()
        if self.rule_manager:
            await self.rule_manager.close()

    async def sync_capacity(self):
        self.rules = self.rule_manager.get_rules_map()
        try:
            global_ref_fx = 1450.0
            upbit_client = self.spot_clients.get("UPBIT")
            if upbit_client:
                t = await upbit_client.fetch_ticker("USDT")
                global_ref_fx = t.get("last", 1450.0)

            for name, client in self.spot_clients.items():
                bal = await client.fetch_balance()
                total_krw = bal.get("KRW", {}).get("total", 0.0)
                self.exchange_slots[name] = int(
                    total_krw / (self.BATCH_SIZE_USDT * global_ref_fx)
                )
                logger.info(f"   💰 {name} Capacity: {self.exchange_slots[name]} slots")

            b_bal = await self.binance.fetch_balance()
            usdt_total = float(b_bal["USDT"]["total"])
            self.MAX_POSITIONS = int(usdt_total / self.BATCH_SIZE_USDT)
            logger.info(f"   🔒 Global Active Cap: {self.MAX_POSITIONS} positions")
        except Exception as e:
            logger.error(f"   ❌ Error syncing capacity: {e}")

    def get_active_positions(self):
        with SessionLocal() as db:
            return db.query(Position).filter(Position.status == "OPEN").all()

    def apply_precision(self, symbol, quantity, exchange_name, is_binance=False):
        if is_binance:
            rule = self.rules.get((symbol, exchange_name))
            if rule and rule.binance_step_size:
                step = float(rule.binance_step_size)
                return float(math.floor(quantity / step) * step)
            return float(math.floor(quantity * 100) / 100)

        return truncate(quantity, 8)

    async def parallel_issue_orders(self, pending_tasks):
        issued_info = []
        BATCH_SIZE = 4

        for i in range(0, len(pending_tasks), BATCH_SIZE):
            batch = pending_tasks[i : i + BATCH_SIZE]
            tasks = []
            task_metadata = []

            for task_type, opp, pos in batch:
                client = self.spot_clients[opp["spot_exchange"]]

                if task_type == "BUY":
                    price = opp["spot_bid"]
                    raw_qty = (self.BATCH_SIZE_USDT * opp["ref_fx"]) / price
                    qty = self.apply_precision(
                        opp["symbol"], raw_qty, opp["spot_exchange"]
                    )

                    if (price * qty) >= 5000:
                        # [DRY RUN CHECK]
                        if DRY_RUN:
                            logger.info(
                                f"   🚧 DRY RUN: Would BUY {opp['symbol']} ({opp['spot_exchange']}) | Qty: {qty} @ {price}"
                            )
                            continue

                        tasks.append(
                            client.create_limit_buy_order(opp["symbol"], qty, price)
                        )
                        task_metadata.append((task_type, opp, pos))
                else:
                    # SELL LOGIC
                    price = opp["spot_ask"]

                    # [DRY RUN BYPASS] Skip wallet check in Dry Run to avoid "No wallet balance" logs
                    if DRY_RUN:
                        # Assume we have enough for simulation logging
                        logger.info(
                            f"   🚧 DRY RUN: Would SELL {opp['symbol']} ({opp['spot_exchange']}) | Price: {price}"
                        )
                        continue

                    bal = await client.fetch_balance()
                    raw_wallet_qty = bal.get(opp["symbol"], {}).get("total", 0.0)

                    # Apply buffer BEFORE truncation
                    safe_qty = raw_wallet_qty - 0.00000002
                    qty = truncate(safe_qty, 8)

                    if (price * qty) >= 5000:

                        async def try_sell_safe(c, sym, p, q):
                            try:
                                return await c.create_limit_sell_order(sym, p, q)
                            except Exception as e:
                                err_msg = str(e).lower()
                                if hasattr(e, "response") and e.response is not None:
                                    err_msg += str(e.response.text).lower()
                                logger.warning(
                                    f"   ⚠️ Sell error {sym}: {err_msg[:100]}"
                                )
                                raise e

                        tasks.append(try_sell_safe(client, opp["symbol"], price, qty))
                        task_metadata.append((task_type, opp, pos))
                    else:
                        logger.warning(
                            f"   ⚠️ Skipping SELL for {opp['symbol']}: Wallet value below 5000 KRW threshold."
                        )

            if not tasks:
                continue

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for j, res in enumerate(results):
                t_type, t_opp, t_pos = task_metadata[j]

                if isinstance(res, Exception):
                    logger.error(f"   ❌ Batch Order Error ({t_opp['symbol']}): {res}")
                    continue

                issued_info.append(
                    {
                        "type": t_type,
                        "order_id": res["id"],
                        "symbol": t_opp["symbol"],
                        "exchange": t_opp["spot_exchange"],
                        "pos_obj": t_pos,
                        "opp_data": t_opp,
                    }
                )
        return issued_info

    async def finalize_cycle(self, issued_orders):
        # 1. Process Synchronized Orders
        for order in issued_orders:
            client = self.spot_clients[order["exchange"]]
            symbol = order["symbol"]
            order_id = order["order_id"]

            try:
                await client.cancel_order(order_id, symbol=symbol)
                await asyncio.sleep(1.0)
            except Exception:
                pass

            status = await client.fetch_order(order_id, symbol=symbol)
            filled = float(status.get("filled", 0.0))

            if filled > 0:
                side = "SHORT" if order["type"] == "BUY" else "BUY"
                if await self._force_hedge_binance(symbol, filled, side=side):
                    if order["type"] == "BUY":
                        self._record_new_position(order, filled)
                    else:
                        await self._finalize_closed_position(order, filled)

        # 2. Orphan Reconciliation
        # [DRY RUN BYPASS] Skip orphan check to avoid false alarms when positions aren't actually created
        if DRY_RUN:
            return

        active_db_positions = self.get_active_positions()
        for pos in active_db_positions:
            client = self.spot_clients.get(pos.spot_exchange)
            if not client:
                continue

            bal = await client.fetch_balance()
            real_spot_qty = bal.get(pos.symbol, {}).get("total", 0.0)
            ticker = await client.fetch_ticker(pos.symbol)
            price = float(ticker.get("last", 0.0))

            if (real_spot_qty * price) < self.MIN_TRADE_VALUE_KRW:
                logger.warning(
                    f"   🧹 Orphan Found: {pos.symbol} on {pos.spot_exchange} filled unobserved."
                )
                if pos.binance_qty > 0:
                    logger.info(
                        f"   📉 Closing orphaned hedge for {pos.symbol}: {pos.binance_qty} Qty"
                    )
                    success = await self._force_hedge_binance(
                        pos.symbol, pos.binance_qty, side="BUY"
                    )
                    if success:
                        with SessionLocal() as db:
                            db_pos = db.query(Position).get(pos.id)
                            if db_pos:
                                db_pos.status = "CLOSED"
                                db_pos.exit_time = self.get_kst_now()
                                db.commit()
                        logger.info(f"   ✅ Orphan {pos.symbol} reconciled and CLOSED.")

    async def _force_hedge_binance(self, symbol, quantity, side="SHORT"):
        if quantity <= 0:
            return True

        # [DRY RUN CHECK]
        if DRY_RUN:
            logger.info(f"   🚧 DRY RUN: Would Hedge {side} {symbol} Qty: {quantity}")
            return True

        from services.hedger import SYMBOL_MAP

        b_key = next(
            (
                k.split(":")[0].replace("/USDT", "")
                for k, v in SYMBOL_MAP.items()
                if v == symbol
            ),
            symbol,
        )
        binance_symbol = f"{b_key}/USDT"

        logger.info(f"   📉 Binance Hedge Action: {side} {quantity} {binance_symbol}")
        try:
            params = {"positionSide": "SHORT"}
            if side == "SHORT":
                await self.binance.create_market_sell_order(
                    symbol=binance_symbol, amount=quantity, params=params
                )
            else:
                # Do NOT send reduceOnly when using positionSide="SHORT" for closing
                await self.binance.create_market_buy_order(
                    symbol=binance_symbol, amount=quantity, params=params
                )
            return True
        except Exception as e:
            logger.error(
                f"   🚨 CRITICAL: Binance {side} failed for {binance_symbol}: {e}"
            )
            return False

    def _record_new_position(self, order_info, filled_qty):
        with SessionLocal() as db:
            pos = Position(
                symbol=order_info["symbol"],
                spot_exchange=order_info["exchange"],
                entry_premium=order_info["opp_data"]["entry_premium"],
                spot_qty=filled_qty,
                binance_qty=filled_qty,
                status="OPEN",
                entry_time=self.get_kst_now(),
                entry_cost_usdt=self.BATCH_SIZE_USDT,
            )
            db.add(pos)
            db.commit()
            logger.info(f"   💾 Recorded NEW Position: {order_info['symbol']}")

    async def _finalize_closed_position(self, order_info, filled_qty):
        pos = order_info["pos_obj"]
        ticker = order_info["opp_data"]

        bin_comm, bin_funding = await self._fetch_actual_fees(
            pos.symbol, pos.entry_time
        )

        with SessionLocal() as db:
            db_pos = db.query(Position).get(pos.id)
            if not db_pos:
                return

            db_pos.spot_qty -= filled_qty
            db_pos.binance_qty -= filled_qty

            # Check dust status
            current_val = db_pos.spot_qty * ticker["spot_ask"]

            if current_val < self.MIN_TRADE_VALUE_KRW:
                db_pos.status = "CLOSED"
                db_pos.exit_time = self.get_kst_now()

                # --- PnL Logic ---
                exit_prem = ticker.get("exit_premium", ticker.get("premium", 0.0))
                entry_prem = float(db_pos.entry_premium or 0.0)
                entry_cost = float(db_pos.entry_cost_usdt or 0.0)

                # Standard Formula
                pnl_pct = exit_prem - entry_prem
                gross_pnl = entry_cost * (pnl_pct / 100.0)

                spot_fee = entry_cost * 0.0005 * 2
                total_comm = spot_fee + bin_comm

                db_pos.exit_premium = exit_prem
                db_pos.realized_pnl = gross_pnl
                db_pos.net_pnl_usdt = gross_pnl - total_comm + bin_funding
                db_pos.commission_usdt = total_comm
                db_pos.funding_fee_usdt = bin_funding

                logger.info(
                    f"   🏁 Position CLOSED: {pos.symbol} | Net: ${db_pos.net_pnl_usdt:.4f}"
                )
            else:
                logger.info(f"   📉 Position REDUCED (Partial fill): {pos.symbol}")
            db.commit()

    async def _fetch_actual_fees(self, symbol, start_time):
        try:
            start_ts = int((start_time - timedelta(hours=24)).timestamp() * 1000)
            from services.hedger import SYMBOL_MAP

            inv_map = {v: k for k, v in SYMBOL_MAP.items()}
            b_key = inv_map.get(symbol, symbol)
            binance_symbol = f"{b_key}USDT"

            income_data = await self.binance.fapiPrivateGetIncome(
                {"symbol": binance_symbol, "startTime": start_ts, "limit": 100}
            )
            commission = 0.0
            funding = 0.0
            entry_ts_ms = int(start_time.timestamp() * 1000)
            for item in income_data:
                time = int(item["time"])
                if time >= entry_ts_ms:
                    amt = float(item["income"])
                    if item["incomeType"] == "COMMISSION":
                        commission += abs(amt)
                    elif item["incomeType"] == "FUNDING_FEE":
                        funding += amt
            return commission, funding
        except:
            return 0.0, 0.0

    async def sync_positions(self):
        """[SAFEGUARD] Reactive syncing with wallet as source of truth."""
        try:
            with SessionLocal() as db:
                active_positions = (
                    db.query(Position).filter(Position.status == "OPEN").all()
                )
                if not active_positions:
                    return

                spot_balances = {}
                for name, client in self.spot_clients.items():
                    spot_balances[name] = await client.fetch_balance()

                try:
                    b_positions = await self.binance.fetch_positions()
                except Exception as e:
                    logger.warning(f"   ⚠️ Binance Sync Warning (Transient): {e}")
                    return

                b_pos_map = {p["symbol"]: float(p["contracts"]) for p in b_positions}
                from services.hedger import SYMBOL_MAP

                inv_map = {v: k for k, v in SYMBOL_MAP.items()}

                for pos in active_positions:
                    exch = pos.spot_exchange
                    real_spot = 0.0
                    if exch in spot_balances and pos.symbol in spot_balances[exch]:
                        real_spot = spot_balances[exch][pos.symbol].get("total", 0.0)

                    if real_spot <= 0.00000001:
                        logger.warning(
                            f"   🧹 Auditor: {pos.symbol} not found on {exch}. Closing Ghost Position."
                        )
                        pos.status = "CLOSED"
                        pos.exit_time = self.get_kst_now()
                        continue

                    if abs(pos.spot_qty - real_spot) > 0:
                        pos.spot_qty = real_spot

                    b_key = inv_map.get(pos.symbol, pos.symbol)
                    b_sym = f"{b_key}/USDT"
                    real_hedge = b_pos_map.get(b_sym, 0.0)
                    if real_hedge == 0 and f"{b_key}USDT" in b_pos_map:
                        real_hedge = b_pos_map[f"{b_key}USDT"]

                    if abs(pos.binance_qty - real_hedge) > 0:
                        pos.binance_qty = real_hedge

                    db.commit()
        except Exception as e:
            logger.error(f"   ❌ Sync Logic Error: {e}")

    async def save_portfolio_snapshot(self):
        # (Keep your existing snapshot logic here)
        pass
