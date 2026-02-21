import asyncio
import ccxt.async_support as ccxt
import os
import math
import logging
import decimal
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# DB Imports
from database.models import Position, PortfolioSnapshot
from database.session import SessionLocal

# Services
from services.rule_manager import RuleManager
from services.hedger import SYMBOL_MAP
from clients.base_spot_client import BaseSpotClient
from services.valuation_service import ValuationService

logger = logging.getLogger(__name__)


def truncate(f, n=8):
    if f == 0:
        return 0.0
    factor = 10**n
    return math.floor(f * factor) / factor


class PositionManager:
    def __init__(
        self,
        spot_clients: Dict[str, BaseSpotClient],
        shutdown_event: asyncio.Event = None,
        thresholds: Dict = None,
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
        self.exchange_slots = {"UPBIT": 0, "BITHUMB": 0, "COINONE": 0}
        self.locks = {}
        self.thresholds = thresholds or {}
        self.shared_fx_rate = 0.0
        self.MIN_TRADE_VALUE_KRW = 5000.0
        self.leverage_cache = set()

        asyncio.create_task(self._ensure_binance_hedge_mode())

    def adjust_price_to_tick(self, price: float) -> float:
        if price >= 2_000_000:
            tick = 1000
        elif price >= 1_000_000:
            tick = 1000
        elif price >= 500_000:
            tick = 500
        elif price >= 100_000:
            tick = 100
        elif price >= 50_000:
            tick = 50
        elif price >= 10_000:
            tick = 10
        elif price >= 5_000:
            tick = 5
        elif price >= 1_000:
            tick = 1
        elif price >= 100:
            tick = 1
        elif price >= 10:
            tick = 0.1
        elif price >= 1:
            tick = 0.01
        elif price >= 0.1:
            tick = 0.001
        else:
            tick = 0.0001
        adjusted = math.floor(price / tick) * tick
        if tick >= 1:
            return int(adjusted)
        else:
            d = decimal.Decimal(str(tick))
            precision = abs(d.as_tuple().exponent)
            return float(round(adjusted, precision))

    async def _ensure_binance_hedge_mode(self):
        try:
            await self.binance.load_time_difference()  # [FIX] Ensure time is synced before this private call
            await self.binance.fapiPrivatePostPositionSideDual(
                {"dualSidePosition": "true"}
            )
        except:
            pass

    async def _safe_close_hedge(self, symbol_slash: str, qty: float):
        """
        Safely closes a SHORT position in Hedge Mode.
        Swallows errors if the position is already closed/zero.
        """
        try:
            # [FIX] Do NOT send reduceOnly in Hedge Mode. Just use positionSide.
            await self.binance.create_market_buy_order(
                symbol=symbol_slash, amount=qty, params={"positionSide": "SHORT"}
            )
            return True, None, None
        except Exception as e:
            e_str = str(e)
            # If rejected because position is zero/closed, treat as success
            if "-2022" in e_str or "ReduceOnly" in e_str or "Order's notional" in e_str:
                logger.warning(f"   ⚠️ Hedge Close Skipped (Likely already 0): {e_str}")
                return True, 0.0, "already_closed"
            return False, 0.0, str(e)

    # --- [FIX] NEW LEVERAGE HELPER ---
    async def _set_binance_leverage(self, symbol: str, leverage: int = 10):
        """Sets leverage for a symbol to ensure 'max position' limits are initialized."""
        if symbol in self.leverage_cache:
            return

        try:
            # Symbol must be formatted for Binance (e.g., BTCUSDT)
            # This API call handles the mapping internally or we pass the raw binance symbol
            await self.binance.fapiPrivatePostLeverage(
                {"symbol": symbol, "leverage": leverage}
            )
            self.leverage_cache.add(symbol)
            # logger.info(f"   ⚙️ Set Leverage for {symbol} to {leverage}x")
        except Exception as e:
            logger.warning(f"   ⚠️ Failed to set leverage for {symbol}: {e}")

    # --- MISSING HELPER METHOD ---
    async def _get_current_short_pos(self, symbol: str) -> float:
        """Helper to check ACTUAL short position on Binance."""
        try:
            # We must search for the specific symbol (e.g. BSVUSDT)
            positions = await self.binance.fetch_positions([symbol])
            for p in positions:
                # In Hedge Mode, we look for side='short' or positionSide='SHORT'
                # CCXT normalizes symbols to 'BTC/USDT:USDT' for linear perps
                if (
                    p["symbol"] == f"{symbol}/USDT:USDT"
                    or p["symbol"] == f"{symbol}/USDT"
                ):
                    if p["side"] == "short":
                        return float(p["contracts"])
            return 0.0
        except Exception:
            return 0.0

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

    # --- [LEG 1] ACTIVE MAKER STRATEGY ---
    async def execute_maker_strategy(self, market_data: Dict):
        # 1. Filter & Sort (Same as before)
        all_opps = []
        for coin, exchanges in market_data.items():
            for exch, data in exchanges.items():
                threshold = self.thresholds.get(exch, {}).get("ENTRY", -0.5)
                if data["entry_premium"] > threshold:
                    continue
                if not data.get("valid_liquidity", False):
                    continue
                all_opps.append(data)

        all_opps.sort(key=lambda x: x["entry_premium"])

        # 2. Capacity Check
        active_positions = self.get_active_positions()
        current_global_count = len(active_positions)

        active_symbols = {p.symbol for p in active_positions}

        # 3. Batch Firing Loop
        while current_global_count < self.MAX_POSITIONS:
            batch = []
            # Strict per-batch limits to ensure 1s spacing isn't wasted
            # Default limits per exchange (can be customized)
            limits = {
                "UPBIT": 4,
                "BITHUMB": 10,
                "COINONE": 4,  # Conservative limit for Coinone
            }
            batch_counts = {exch: 0 for exch in self.exchange_slots.keys()}

            # Fill Batch
            for opp in all_opps[:]:
                symbol = opp["symbol"]
                exch = opp["spot_exchange"]

                if symbol in active_symbols:
                    continue
                if self.exchange_slots.get(exch, 0) <= 0:
                    continue
                if self.get_symbol_lock(symbol).locked():
                    continue
                if batch_counts[exch] >= limits.get(exch, 4):
                    continue

                batch.append(opp)
                active_symbols.add(symbol)
                self.exchange_slots[exch] -= 1
                batch_counts[exch] += 1
                current_global_count += 1
                all_opps.remove(opp)

                if len(batch) >= 10:
                    break  # Cap total simultaneous fire

            if not batch:
                break

            # 4. Execute & Wait
            await self._process_batch(batch)

            # [CHANGE] Sleep 1.0s to spread orders out (User Request)
            # This creates the "Place orders every second" behavior.
            await asyncio.sleep(1.0)

    async def _process_batch(self, batch_opps):
        orders_info = []  # Stores {id, symbol, exch, opp_data}

        # A. Place Limit Orders (Async)
        # logger.info(f"   🚀 Placing Batch of {len(batch_opps)} Active Maker Orders...")

        async def place_wrapper(opp):
            try:
                client = self.spot_clients[opp["spot_exchange"]]
                price = self.adjust_price_to_tick(opp["spot_bid"])  # BUY AT BID
                ref_fx = opp["ref_fx"]

                # Calculate Size
                target_usdt = self.BATCH_SIZE_USDT
                raw_qty = (target_usdt * ref_fx) / price
                qty = self.apply_precision(opp["symbol"], raw_qty, opp["spot_exchange"])

                if (qty * price) < self.MIN_TRADE_VALUE_KRW:
                    return None

                # DEBUG: Log order placement for all exchanges, especially Coinone
                if opp["spot_exchange"] == "COINONE":
                    logger.info(
                        f"   🔍 DEBUG COINONE: Placing {opp['symbol']} order | Client: {client.__class__.__name__} | Qty: {qty} @ {price}"
                    )

                res = await client.create_limit_buy_order(opp["symbol"], qty, price)

                # DEBUG: Log successful order creation
                if opp["spot_exchange"] == "COINONE":
                    logger.info(
                        f"   ✅ DEBUG COINONE: Order placed successfully. ID: {res.get('id') or res.get('uuid')}"
                    )

                return {
                    "id": res.get("id") or res.get("uuid"),
                    "symbol": opp["symbol"],
                    "exch": opp["spot_exchange"],
                    "price": price,
                    "qty": qty,
                    "opp": opp,
                    "created_at": asyncio.get_event_loop().time(),
                }
            except Exception as e:
                logger.error(
                    f"   ❌ Order Fail {opp['symbol']} ({opp['spot_exchange']}): {e}"
                )
                self.exchange_slots[opp["spot_exchange"]] += 1  # Refund slot
                if opp["spot_exchange"] == "COINONE":
                    logger.error(
                        f"   🔍 DEBUG COINONE ERROR: {type(e).__name__}: {str(e)}"
                    )
                return None

        tasks = [place_wrapper(o) for o in batch_opps]
        results = await asyncio.gather(*tasks)
        orders_info = [r for r in results if r is not None]

        if not orders_info:
            return

        # B. Enter Active Monitoring Loop
        await self._monitor_batch_orders(orders_info)

    async def _monitor_batch_orders(self, orders):
        """
        High-Frequency Loop:
        1. Fetch Binance Bids (Hedge Entry Price)
        2. Re-calculate Premium
        3. Cancel if invalid
        4. Detect Fill -> Hedge
        """
        active_orders = {o["id"]: o for o in orders}
        monitor_start = asyncio.get_event_loop().time()
        MAX_LIFE = 10  # seconds

        try:
            while active_orders:
                # 1. Fetch Latest Binance Data (Batch)
                # We need the BID price to calculate entry premium (Buying Spot, Selling Future)
                try:
                    tickers = await self.binance.fetch_bids_asks()
                except Exception:
                    await asyncio.sleep(0.5)
                    continue

                ids_to_remove = []

                for oid, info in active_orders.items():
                    symbol = info["symbol"]
                    b_key = info["opp"]["binance_symbol_key"]
                    b_ticker = tickers.get(b_key + "USDT") or tickers.get(
                        b_key + "/USDT"
                    )

                    # --- SAFETY CHECK 1: TIMEOUT ---
                    if (
                        asyncio.get_event_loop().time() - info["created_at"]
                    ) > MAX_LIFE:
                        await self._cancel_and_refund(info, "TIMEOUT")
                        ids_to_remove.append(oid)
                        continue

                    # --- SAFETY CHECK 2: PREMIUM DEGRADATION ---
                    if b_ticker:
                        b_bid = float(b_ticker["bid"])
                        scaling = info["opp"]["scaling"]
                        b_bid_scaled = b_bid / scaling

                        # Recalculate Entry Premium: (My_Order_Price / New_Future_Bid)
                        current_prem = (
                            (info["price"] / (b_bid_scaled * info["opp"]["ref_fx"])) - 1
                        ) * 100
                        threshold = self.thresholds.get(info["exch"], {}).get(
                            "ENTRY", -0.5
                        )

                        # If Premium rose above threshold (e.g. -0.2 > -0.5), it's bad.
                        if current_prem > threshold:
                            logger.info(
                                f"   🛑 Kill {symbol}: Prem degraded to {current_prem:.2f}%"
                            )
                            await self._cancel_and_refund(info, "PREM_DEGRADED")
                            ids_to_remove.append(oid)
                            continue

                    # --- CHECK FILL STATUS ---
                    try:
                        client = self.spot_clients[info["exch"]]
                        status = await client.fetch_order(oid, symbol)
                        filled = float(status.get("filled", 0.0))

                        if filled > 0:
                            # PARTIAL or FULL FILL -> HEDGE IMMEDIATELY
                            logger.info(f"   ⚡ FILL DETECTED: {symbol} Qty: {filled}")

                            # Lock to prevent double processing
                            async with self.get_symbol_lock(symbol):
                                success = await self._execute_hedge_leg(
                                    info, filled, status.get("average", info["price"])
                                )
                                if success:
                                    # If fully filled, remove. If partial, we could keep monitoring,
                                    # but for simplicity in "Active Maker", we treat partial as done and cancel remainder.
                                    remaining = float(status.get("remaining", 0.0))
                                    if remaining > 0:
                                        await client.cancel_order(oid, symbol)
                                        # Refund partial slot? No, we used the slot.
                                    ids_to_remove.append(oid)
                                else:
                                    # Hedge Failed -> Rollback Spot
                                    ids_to_remove.append(oid)

                        elif status.get("status") == "closed":
                            # Canceled externally
                            ids_to_remove.append(oid)
                            self.exchange_slots[info["exch"]] += 1

                    except Exception as e:
                        logger.error(f"   ⚠️ Monitor Error {symbol}: {e}")

                # Cleanup processed orders
                for oid in ids_to_remove:
                    active_orders.pop(oid, None)

                if not active_orders:
                    break
                await asyncio.sleep(0.5)  # High freq poll

        except Exception as e:
            logger.error(f"   ⚠️ Critical Monitor Loop Error: {e}")
        finally:
            # Ensure any dangling orders are cancelled
            for info in active_orders.values():
                await self._cancel_and_refund(info, "LOOP_EXIT")

    async def _cancel_and_refund(self, info, reason):
        """
        [FIXED] Safely cancels an order AND checks if it was filled during the process.
        Prevents 'Zombie' positions where we cancel but fail to hedge a race-condition fill.
        """
        try:
            client = self.spot_clients[info["exch"]]
            symbol = info["symbol"]
            oid = info["id"]

            # 1. Attempt Cancel
            try:
                await client.cancel_order(oid, symbol)
            except Exception:
                # Ignore errors (e.g., "Order already closed"), we verify status next.
                pass

            await asyncio.sleep(0.5)

            # 2. [CRITICAL] Verify Final Status
            # We must check if it filled *before* or *during* cancel
            try:
                final_status = await client.fetch_order(oid, symbol)
                filled = float(final_status.get("filled", 0.0))

                if filled > 0:
                    # Check if we already hedged this amount?
                    # Note: logic in _execute_hedge_leg uses idempotency checks (Binance position size)
                    # so it is safe to call again.
                    logger.warning(
                        f"   🧟 ZOMBIE PREVENTION: {symbol} filled {filled} during {reason} cancel!"
                    )

                    avg_price = float(
                        final_status.get("average")
                        or final_status.get("price")
                        or info["price"]
                    )

                    # Execute Hedge for the filled amount
                    async with self.get_symbol_lock(symbol):
                        await self._execute_hedge_leg(info, filled, avg_price)
            except Exception as e:
                logger.error(f"   ⚠️ Failed to verify cancel status for {symbol}: {e}")

            # 3. Always Refund Slot
            self.exchange_slots[info["exch"]] += 1

        except Exception as e:
            logger.error(f"   ❌ Critical Cancel Error: {e}")
            self.exchange_slots[info["exch"]] += 1

    async def _handle_hedge_failure_and_rollback(
        self,
        symbol: str,
        exch: str,
        spot_qty: float,
        spot_price: float,
        bin_order_id: str,
        bin_symbol: str,
        current_hedge_res: Optional[dict],
        fallback_price: str,
        prior_fills: float = 0.0,  # <--- [NEW ARGUMENT]
    ):
        """
        Shared logic to handle hedge failures with cumulative fill tracking.
        """

        # 1. Determine exactly how much was hedged
        current_order_fill = 0.0
        updated_hedge_res = current_hedge_res

        try:
            if bin_order_id:
                # Fetch latest status from Binance to be sure
                status = await self.binance.fetch_order(bin_order_id, bin_symbol)
                current_order_fill = float(status.get("filled", 0.0))
                updated_hedge_res = status
        except Exception as fetch_err:
            logger.warning(f"   ⚠️ Could not verify hedge fill: {fetch_err}")
            # If verification fails, we rely on what we know (prior_fills)
            pass

        # [CRITICAL FIX] Total = Prior Fills (from loop) + Current Active Order Fill
        hedge_filled = prior_fills + current_order_fill

        logger.info(
            f"   🔍 DEBUG ROLLBACK: Prior={prior_fills}, Current={current_order_fill}, Total={hedge_filled}"
        )

        # 2. Calculate Mismatch
        unhedged_qty = spot_qty - hedge_filled
        unhedged_val_krw = unhedged_qty * spot_price
        MIN_TRADE_KRW = 6000.0  # Safe buffer above 5000

        # --- SCENARIO A: Total Failure (Zero Hedge) ---
        if hedge_filled <= 0:
            logger.critical(f"   ☠️ ZERO HEDGE. Rolling back FULL Spot Position.")
            try:
                client = self.spot_clients[exch]
                await client.create_market_sell_order(symbol, spot_qty)
                return False, 0.0, 0.0, None
            except Exception as rb_err:
                logger.critical(f"   ☠️ ROLLBACK FAILED: {rb_err}")
                return False, 0.0, 0.0, None

        # --- SCENARIO B: Partial Hedge (Dust Mismatch) ---
        elif unhedged_val_krw < MIN_TRADE_KRW:
            logger.warning(
                f"   ⚠️ Unhedged Dust ({unhedged_val_krw:.0f} KRW). Too small to sell. Keeping Position."
            )
            # We ACCEPT the mismatch. We record the hedge we actually got.
            # We keep the full spot_qty (because we can't sell the dust).

            if not updated_hedge_res:
                updated_hedge_res = {"average": fallback_price}

            return True, spot_qty, hedge_filled, updated_hedge_res

        # --- SCENARIO C: Partial Hedge (Significant Mismatch) ---
        else:
            logger.warning(
                f"   ⚖️ Partial Hedge. Selling {unhedged_qty} unhedged spot to rebalance."
            )
            try:
                client = self.spot_clients[exch]
                # Sell ONLY the difference
                await client.create_market_sell_order(symbol, unhedged_qty)

                # Success: We now hold spot equal to our hedge
                if not updated_hedge_res:
                    updated_hedge_res = {"average": fallback_price}

                return True, hedge_filled, hedge_filled, updated_hedge_res

            except Exception as rb_err:
                logger.error(
                    f"   ❌ Partial Rollback Failed: {rb_err}. Saving mismatched state to DB."
                )
                # If we fail to sell spot, we SAVE the mismatched state.
                # This ensures the bot knows we still hold the Spot, so the Unwinder can fix it later.
                if not updated_hedge_res:
                    updated_hedge_res = {"average": fallback_price}

                return True, spot_qty, hedge_filled, updated_hedge_res

    # --- [LEG 2] SMART HEDGE (IDEMPOTENT FIX) ---
    async def _execute_hedge_leg(self, order_info, filled_qty, avg_price):
        symbol = order_info["symbol"]
        exch = order_info["exch"]
        opp = order_info["opp"]
        b_key = opp["binance_symbol_key"]
        bin_sym = f"{b_key}/USDT"
        cumulative_hedge_filled = 0.0

        # 1. Calc Target Qty
        target_hedge_qty = self.apply_precision(
            symbol, filled_qty, exch, is_binance=True
        )
        scaling = opp["scaling"]
        ref_fx = opp["ref_fx"]

        await self._set_binance_leverage(f"{b_key}USDT", leverage=10)

        # 2. Check Existing Position (IDEMPOTENCY CHECK)
        # This prevents "Double Shorting" even if the API times out
        current_short = await self._get_current_short_pos(b_key)

        # If we are already short enough, skip!
        if current_short >= (target_hedge_qty * 0.99):
            logger.info(
                f"   ⚠️ Already Hedged {symbol} (Qty: {current_short}). Skipping."
            )
            return True  # Treat as success

        # Calculate what's left to hedge
        needed_qty = target_hedge_qty - current_short
        needed_qty = self.apply_precision(symbol, needed_qty, exch, is_binance=True)

        if needed_qty <= 0:
            return True

        # 3. Execution Loop
        target_price_raw = (
            avg_price / (ref_fx * (1 + opp["entry_premium"] / 100.0))
        ) * scaling
        price_str = self.binance.price_to_precision(bin_sym, target_price_raw)

        current_order_id = None
        final_hedge_res = None
        is_filled = False

        for attempt in range(1, 5):
            try:
                if not current_order_id:
                    logger.info(f"   🛡️ Hedging {symbol}: {needed_qty} @ {price_str}")
                    res = await self.binance.create_limit_sell_order(
                        bin_sym, needed_qty, price_str, params={"positionSide": "SHORT"}
                    )
                    current_order_id = res["id"]

                for _ in range(4):
                    await asyncio.sleep(0.5)
                    try:
                        status = await self.binance.fetch_order(
                            current_order_id, bin_sym
                        )
                        if float(status.get("filled", 0)) >= needed_qty:
                            final_hedge_res = status
                            is_filled = True
                            break
                    except Exception as e:
                        # If lost, checking position again is safest,
                        # but for speed, we just swallow error and let chase logic retry
                        pass

                if is_filled:
                    break

                # Chase Logic
                book = await self.binance.fetch_order_book(bin_sym, limit=5)
                if not book["bids"]:
                    continue
                fresh_bid = float(book["bids"][0][0])
                fresh_bid_scaled = fresh_bid / scaling
                spot_usd = avg_price / ref_fx
                current_prem = ((spot_usd / fresh_bid_scaled) - 1) * 100

                if current_prem < -0.1:
                    logger.info(
                        f"   🔄 Chasing! Market moved (Prem: {current_prem:.2f}%). Repricing..."
                    )

                    if current_order_id:
                        try:
                            await self.binance.cancel_order(current_order_id, bin_sym)
                        except Exception as e:
                            logger.warning(
                                f"      Cancel failed (might be filled): {e}"
                            )

                        # Wait a split second for Binance to settle the final filled state
                        await asyncio.sleep(0.5)

                        try:
                            # Fetch status AFTER cancel to get the absolute final filled amount
                            cancel_status = await self.binance.fetch_order(
                                current_order_id, bin_sym
                            )
                            filled_amount = float(cancel_status.get("filled", 0.0))

                            if filled_amount > 0:
                                logger.info(
                                    f"      Partially/Fully filled {filled_amount}. Reducing next order."
                                )
                                needed_qty -= filled_amount
                                cumulative_hedge_filled += filled_amount
                                needed_qty = self.apply_precision(
                                    symbol, needed_qty, exch, is_binance=True
                                )
                        except Exception as e:
                            pass

                    # 4. Check if we are done before placing new order
                    if needed_qty <= 0:
                        is_filled = True
                        cumulative_hedge_filled = target_hedge_qty
                        break

                    # 5. Set new price and reset ID to trigger new order creation next loop
                    price_str = self.binance.price_to_precision(bin_sym, fresh_bid)
                    current_order_id = None
                else:
                    break

            except Exception as e:
                if (
                    "precision" in str(e).lower()
                    or "notional" in str(e).lower()
                    or "min_amount" in str(e).lower()
                ):
                    logger.warning(
                        f"      Stopping chase, remaining qty is too small to hedge (Dust)."
                    )
                    is_filled = True
                    break
                logger.error(f"   ⚠️ Hedge Chase Error: {e}")
                await asyncio.sleep(1)

        # Finalize
        if not is_filled:
            try:
                if current_order_id:
                    try:
                        await self.binance.cancel_order(current_order_id, bin_sym)
                    except:
                        pass
                await self.binance.create_market_sell_order(
                    bin_sym, needed_qty, params={"positionSide": "SHORT"}
                )
                is_filled = True
            except Exception as e:
                logger.error(f"   🚨 HEDGE EXECUTION ERROR {symbol}: {e}")

                # [FIX] Pass 'cumulative_hedge_filled' as 'prior_fills'
                success, final_spot_qty, final_hedge_qty, final_res = (
                    await self._handle_hedge_failure_and_rollback(
                        symbol=symbol,
                        exch=exch,
                        spot_qty=filled_qty,
                        spot_price=avg_price,
                        bin_order_id=current_order_id,
                        bin_symbol=bin_sym,
                        current_hedge_res=final_hedge_res,
                        fallback_price=price_str,
                        prior_fills=cumulative_hedge_filled,  # <--- PASSING THE TRACKER
                    )
                )

                if not success:
                    return False

                # Update local variables for the DB Save section below
                filled_qty = final_spot_qty
                target_hedge_qty = final_hedge_qty
                final_hedge_res = final_res

                # Recalculate KRW Cost based on adjusted spot qty
                cost_krw = filled_qty * avg_price

        # DB Logic
        hedge_price = (
            float(
                final_hedge_res.get("average", 0)
                or final_hedge_res.get("price", 0)
                or price_str
            )
            if final_hedge_res
            else float(price_str)
        )
        hedge_val_usdt = target_hedge_qty * hedge_price
        cost_krw = filled_qty * avg_price

        with SessionLocal() as db:
            pos = Position(
                symbol=symbol,
                exchange=exch,
                status="OPEN",
                current_spot_qty=filled_qty,
                current_hedge_qty=target_hedge_qty,
                entry_time=self.get_kst_now(),
                entry_spot_qty=filled_qty,
                entry_spot_price=avg_price,
                entry_spot_amount_krw=cost_krw,
                entry_hedge_qty=target_hedge_qty,
                entry_hedge_price=hedge_price,
                entry_hedge_amount_usdt=hedge_val_usdt,
                entry_usdt_rate=ref_fx,
                config_entry_threshold=self.thresholds.get(exch, {}).get("ENTRY", 0),
                calc_entry_premium=opp["entry_premium"],
                entry_spot_order_id=str(order_info["id"]),
            )
            db.add(pos)
            db.commit()

        logger.info(
            f"   ✅ Position Opened: {symbol} | Spot Cost: {cost_krw:,.0f} KRW | Hedge: ${hedge_val_usdt:.2f}"
        )
        return True

    # --- [EXIT] ACTIVE TRAILING EXIT (SAFE BATCHING) ---
    async def run_active_exit(self):
        active = self.get_active_positions()
        if not active:
            return

        # 1. Fetch Binance Data Globally (One request for all)
        try:
            tickers_map = await self.binance.fetch_bids_asks()
            bin_asks = {}
            for ticker in tickers_map.values():
                raw = ticker.get("info", {})
                sym = raw.get("symbol")
                if sym:
                    bin_asks[sym] = float(ticker.get("ask", 0) or 0)
        except Exception as e:
            logger.error(f"   ⚠️ Exit Logic Skipped (Binance Data): {e}")
            return

        # [CHANGE] Spawn background tasks with stagger
        # This prevents blocking the main loop while still respecting API limits via staggered start
        BATCH_SIZE = 5
        for i in range(0, len(active), BATCH_SIZE):
            chunk = active[i : i + BATCH_SIZE]
            for pos in chunk:
                # Fire and forget (background task)
                asyncio.create_task(self._manage_single_active_exit(pos, bin_asks))

            # Sleep 1.0s between spawning batches to protect Upbit Limit (8/s)
            # This consumes time in the main loop but allows for cleaner API usage
            if (i + BATCH_SIZE) < len(active):
                await asyncio.sleep(1.0)

    async def _manage_single_active_exit(self, pos: Position, bin_map: Dict):
        # Lock to ensure we don't conflict with Entry/Sync logic
        if self.get_symbol_lock(pos.symbol).locked():
            return

        async with self.get_symbol_lock(pos.symbol):
            client = self.spot_clients.get(pos.exchange)
            bal = await client.fetch_balance()
            free = bal.get(pos.symbol, {}).get("free", 0.0)
            if free < (pos.current_spot_qty * 0.9):
                return

            from services.hedger import SYMBOL_MAP

            inv_map = {v: k for k, v in SYMBOL_MAP.items()}
            b_key = inv_map.get(pos.symbol, pos.symbol)
            bin_sym_raw = f"{b_key}USDT"

            # This line uses the new argument
            bin_ask = bin_map.get(bin_sym_raw)
            if not bin_ask:
                return

            target_pct = self.thresholds.get(pos.exchange, {}).get("EXIT", 1.5)
            scaling = 1000.0 if b_key.startswith("1000") else 1.0

            floor_price = (
                (bin_ask / scaling) * self.shared_fx_rate * (1 + target_pct / 100.0)
            )

            try:
                s_book = await client.fetch_orderbook(pos.symbol)
                s_best_ask = s_book["ask"]
            except:
                return

            final_price = max(floor_price, s_best_ask)
            final_price = self.adjust_price_to_tick(final_price)

            if final_price <= s_best_ask:
                qty = self.apply_precision(
                    pos.symbol, min(pos.current_spot_qty, free), pos.exchange
                )
                if (qty * final_price) < 5000:
                    return

                try:
                    logger.info(f"   💰 Exit Attempt {pos.symbol}: Limit {final_price}")
                    res = await client.create_limit_sell_order(
                        pos.symbol, final_price, qty
                    )
                    order_id = res.get("id") or res.get("uuid")

                    await asyncio.sleep(5.0)

                    await client.cancel_order(order_id, pos.symbol)
                    final_status = await client.fetch_order(order_id, pos.symbol)
                    filled = float(final_status.get("filled", 0.0))

                    if filled > 0:
                        ratio = (
                            filled / pos.current_spot_qty
                            if pos.current_spot_qty > 0
                            else 0
                        )
                        h_qty = self.apply_precision(
                            pos.symbol,
                            pos.current_hedge_qty * ratio,
                            pos.exchange,
                            is_binance=True,
                        )
                        success, ex_price, _ = await self._safe_close_hedge(
                            f"{b_key}/USDT", h_qty
                        )
                        self._finalize_db_exit(
                            pos,
                            filled,
                            final_price,
                            h_qty,
                            ex_price or (bin_ask / scaling),
                        )
                except Exception as e:
                    logger.error(f"   ⚠️ Exit Error {pos.symbol}: {e}")

    def _finalize_db_exit(self, pos, s_qty, s_price, h_qty, h_price):
        """
        Updates the Position in DB with exit details, calculating PnL for this specific batch
        and accumulating it into the total position stats. Handles partial exits correctly.
        """
        try:
            with SessionLocal() as db:
                db_pos = db.query(Position).get(pos.id)
                if not db_pos:
                    logger.error(
                        f"   ❌ DB Error: Position {pos.id} not found for finalization."
                    )
                    return

                # --- 1. State Updates ---
                # Reduce current open quantities
                db_pos.current_spot_qty = max(
                    0.0, (db_pos.current_spot_qty or 0.0) - s_qty
                )
                db_pos.current_hedge_qty = max(
                    0.0, (db_pos.current_hedge_qty or 0.0) - h_qty
                )

                # --- 2. Batch PnL Calculation ---
                fx = self.shared_fx_rate

                # Spot Leg (KRW): (Exit - Entry) * Qty
                entry_spot_price = db_pos.entry_spot_price or 0.0
                batch_spot_pnl_krw = (s_price - entry_spot_price) * s_qty

                # Hedge Leg (USDT): (Entry - Exit) * Qty  <-- Short Logic
                entry_hedge_price = db_pos.entry_hedge_price or 0.0
                batch_hedge_pnl_usdt = (entry_hedge_price - h_price) * h_qty

                # --- 3. Fee Estimation (Pro-Rata) ---
                # We calculate fees for THIS batch's share of the entry + this exit
                # Assumption: ~0.05% (0.0005) per leg per side.

                # Spot Fees (KRW -> USDT)
                spot_entry_val_krw = s_qty * entry_spot_price
                spot_exit_val_krw = s_qty * s_price
                spot_fees_usdt = (
                    (spot_entry_val_krw + spot_exit_val_krw) * 0.0005
                ) / fx

                # Hedge Fees (USDT)
                hedge_entry_val_usdt = h_qty * entry_hedge_price
                hedge_exit_val_usdt = h_qty * h_price
                hedge_fees_usdt = (hedge_entry_val_usdt + hedge_exit_val_usdt) * 0.0005

                batch_total_fees = spot_fees_usdt + hedge_fees_usdt

                # --- 4. Net PnL (USDT) ---
                batch_net_pnl = (
                    (batch_spot_pnl_krw / fx) + batch_hedge_pnl_usdt - batch_total_fees
                )

                # --- 5. DB Accumulation ---
                # Accumulate Exit Totals
                db_pos.exit_time = self.get_kst_now()
                db_pos.exit_spot_qty = (db_pos.exit_spot_qty or 0.0) + s_qty
                db_pos.exit_hedge_qty = (db_pos.exit_hedge_qty or 0.0) + h_qty
                db_pos.exit_spot_amount_krw = (
                    db_pos.exit_spot_amount_krw or 0.0
                ) + spot_exit_val_krw
                db_pos.exit_hedge_amount_usdt = (
                    db_pos.exit_hedge_amount_usdt or 0.0
                ) + hedge_exit_val_usdt

                # Update Weighted Average Exit Prices
                if db_pos.exit_spot_qty > 0:
                    db_pos.exit_spot_price = (
                        db_pos.exit_spot_amount_krw / db_pos.exit_spot_qty
                    )
                if db_pos.exit_hedge_qty > 0:
                    db_pos.exit_hedge_price = (
                        db_pos.exit_hedge_amount_usdt / db_pos.exit_hedge_qty
                    )

                # Accumulate Performance Metrics
                db_pos.gross_spot_pnl_krw = (
                    db_pos.gross_spot_pnl_krw or 0.0
                ) + batch_spot_pnl_krw
                db_pos.gross_hedge_pnl_usdt = (
                    db_pos.gross_hedge_pnl_usdt or 0.0
                ) + batch_hedge_pnl_usdt
                db_pos.total_fees_usdt = (
                    db_pos.total_fees_usdt or 0.0
                ) + batch_total_fees
                db_pos.net_pnl_usdt = (db_pos.net_pnl_usdt or 0.0) + batch_net_pnl

                # Update Audit Fields
                db_pos.exit_usdt_rate = fx

                # Check for Closure
                # If remaining qty is negligible (< 1% of entry), mark CLOSED
                if db_pos.current_spot_qty < (db_pos.entry_spot_qty * 0.01):
                    db_pos.status = "CLOSED"
                    db_pos.current_spot_qty = 0
                    db_pos.current_hedge_qty = 0

                    # Final Log
                    logger.info(
                        f"   🏁 TRADE CLOSED: {pos.symbol} | 💰 NET PNL: ${db_pos.net_pnl_usdt:.2f}"
                    )
                else:
                    logger.info(
                        f"   📉 PARTIAL EXIT: {pos.symbol} | Batch Net: ${batch_net_pnl:.2f} | Rem Spot: {db_pos.current_spot_qty:.4f}"
                    )

                db.commit()

        except Exception as e:
            logger.error(f"   ❌ Finalize DB Error: {e}")

    async def sync_capacity(self):
        """
        Updates order slots based on BATCH_SIZE_USDT.
        Now relies on shared_fx_rate being updated by the main loop.
        """
        self.rules = self.rule_manager.get_rules_map()

        if self.shared_fx_rate <= 0:
            # logger.warning("   ⚠️ sync_capacity: No FX rate yet. Skipping slot calculation.")
            return

        try:
            for name, client in self.spot_clients.items():
                try:
                    bal = await client.fetch_balance()
                    free_krw = bal.get("KRW", {}).get("free", 0.0)

                    # DEBUG: Log balance fetch for all exchanges, especially Coinone
                    logger.debug(
                        f"   🔍 DEBUG {name}: Balance fetch - Free KRW: {free_krw}, Full balance: {bal}"
                    )
                    if name == "COINONE":
                        logger.info(
                            f"   🔍 DEBUG COINONE: fetch_balance() returned: {bal}"
                        )

                    # Calculate slots using the shared rate updated from the scanner
                    self.exchange_slots[name] = int(
                        free_krw / (self.BATCH_SIZE_USDT * self.shared_fx_rate)
                    )

                    # DEBUG: Log allocated slots
                    logger.debug(
                        f"   💰 {name} Capacity: {self.exchange_slots[name]} slots ({free_krw:,.0f} KRW Free)"
                    )
                    if name == "COINONE":
                        logger.info(
                            f"   🔍 DEBUG COINONE: Allocated {self.exchange_slots[name]} slots"
                        )

                except Exception as e:
                    logger.error(f"   ❌ Balance Fetch Error for {name}: {e}")
                    self.exchange_slots[name] = 0

        except Exception as e:
            logger.error(f"   ⚠️ Capacity Sync Error: {e}")
        try:
            b_bal = await self.binance.fetch_balance()
            usdt_total = float(b_bal["USDT"]["total"])
            self.MAX_POSITIONS = int(usdt_total / self.BATCH_SIZE_USDT)
        except:
            self.MAX_POSITIONS = 0

    async def sync_positions(self):
        """Reconciles DB state with actual wallet balances (Thread-Safe)."""
        try:
            with SessionLocal() as db:
                active_db_positions = (
                    db.query(Position).filter(Position.status == "OPEN").all()
                )
                if not active_db_positions:
                    return

                # Fetch all data first to minimize locking time
                b_positions = await self.binance.fetch_positions()
                b_position_map = {
                    p["symbol"]: float(p["contracts"]) for p in b_positions
                }

                spot_balances = {}
                for name, client in self.spot_clients.items():
                    bal = await client.fetch_balance()
                    if bal:  # [FIX] Only sync if API succeeded
                        spot_balances[name] = bal

                from services.hedger import SYMBOL_MAP

                inv_map = {v: k for k, v in SYMBOL_MAP.items()}

                for pos in active_db_positions:
                    # [CRITICAL FIX] Lock symbol so we don't race with Exit Logic
                    if self.get_symbol_lock(pos.symbol).locked():
                        continue  # Skip this cycle if main bot is busy with this symbol

                    async with self.get_symbol_lock(pos.symbol):
                        # Re-verify DB state inside lock
                        db.refresh(pos)
                        if pos.status != "OPEN":
                            continue

                        if pos.exchange not in spot_balances:
                            continue  # [FIX] Safely skip this coin if its exchange API timed out

                        real_spot_qty = float(
                            spot_balances[pos.exchange]
                            .get(pos.symbol, {})
                            .get("total", 0.0)
                        )

                        b_key = inv_map.get(pos.symbol, pos.symbol)
                        target_symbols = [f"{b_key}/USDT", f"{b_key}/USDT:USDT"]

                        real_binance_qty = 0.0
                        for ts in target_symbols:
                            if ts in b_position_map:
                                real_binance_qty = b_position_map[ts]
                                break

                        # 1. GHOST SELL CHECK (Panic Close)
                        # Only trigger if we have SIGNIFICANT hedge open but NO spot
                        if real_spot_qty < (
                            pos.current_spot_qty * 0.05
                        ) and real_binance_qty > (pos.current_hedge_qty * 0.5):
                            logger.critical(
                                f"🚨 GHOST SELL DETECTED: {pos.symbol}. Closing Hedge!"
                            )

                            bin_sym = f"{b_key}/USDT"
                            # Use new safe helper
                            success, _, _ = await self._safe_close_hedge(
                                bin_sym, real_binance_qty
                            )

                            if success:
                                pos.status = "CLOSED"
                                pos.exit_time = self.get_kst_now()
                                pos.current_spot_qty = 0
                                pos.current_hedge_qty = 0
                                db.commit()
                                logger.info(
                                    f"✅ Emergency Hedge Close Complete for {pos.symbol}"
                                )
                            continue

                        # 2. NORMAL SYNC (Update DB to match Reality)
                        changed = False
                        if abs(pos.current_spot_qty - real_spot_qty) > (
                            pos.current_spot_qty * 0.05
                        ):
                            pos.current_spot_qty = real_spot_qty
                            changed = True

                        if abs(pos.current_hedge_qty - real_binance_qty) > (
                            pos.current_hedge_qty * 0.05
                        ):
                            pos.current_hedge_qty = real_binance_qty
                            changed = True

                        if changed:
                            db.commit()

        except Exception as e:
            logger.error(f"💥 Failed to sync positions: {e}")

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

    async def execute_sniper_logic(self, market_data: Dict):
        all_opps = []
        for coin, exchanges in market_data.items():
            for exch, data in exchanges.items():
                threshold = self.thresholds.get(exch, {}).get("ENTRY", -0.5)
                if data["entry_premium"] > threshold:
                    continue
                if not data.get("valid_liquidity", False):
                    continue
                all_opps.append(data)

        all_opps.sort(key=lambda x: x["entry_premium"])
        active_positions = self.get_active_positions()
        current_global_count = len(active_positions)
        active_symbols = {p.symbol for p in active_positions}

        for opp in all_opps:
            if current_global_count >= self.MAX_POSITIONS:
                break
            symbol = opp["symbol"]
            if symbol in active_symbols:
                continue

            exch = opp["spot_exchange"]
            spot_price = opp["spot_ask"]

            async with self.get_symbol_lock(symbol):
                has_orphan = await self._check_orphaned_holdings(
                    symbol, exch, current_price=spot_price
                )
                if has_orphan:
                    logger.warning(
                        f"   🛑 Safety Block: {symbol} exists in wallet but not DB. Skipping Buy."
                    )
                    continue

                success = await self._attempt_snipe(opp)
                if success:
                    current_global_count += 1
                    active_symbols.add(symbol)
                    self.exchange_slots[exch] -= 1

    async def _check_orphaned_holdings(
        self, symbol: str, exchange: str, current_price: float = None
    ) -> bool:
        try:
            client = self.spot_clients.get(exchange)
            if not client:
                return False
            bal = await client.fetch_balance()
            total_qty = bal.get(symbol, {}).get("total", 0.0)
            DUST_THRESHOLD_KRW = 6000.0

            if total_qty > 0:
                market_valuation = (
                    (total_qty * current_price)
                    if current_price
                    else (DUST_THRESHOLD_KRW + 1)
                )
                if market_valuation <= DUST_THRESHOLD_KRW:
                    return False
                with SessionLocal() as db:
                    exists = (
                        db.query(Position)
                        .filter(
                            Position.symbol == symbol,
                            Position.spot_exchange == exchange,
                            Position.status == "OPEN",
                        )
                        .first()
                    )
                    if not exists:
                        return True
            return False
        except:
            return True

    async def _attempt_snipe(self, opp) -> bool:
        symbol = opp["symbol"]
        exch = opp["spot_exchange"]
        client = self.spot_clients.get(exch)
        price = self.adjust_price_to_tick(opp["spot_ask"])
        ref_fx = opp["ref_fx"]
        SPOT_FEE_RATE = 0.0004 if exch == "BITHUMB" else 0.0005
        HEDGE_FEE_RATE = 0.0005
        target_usdt = self.BATCH_SIZE_USDT

        liq_factor = 0.5 if exch == "BITHUMB" else 1.0
        available_spot_usdt = (opp["spot_ask_size"] * liq_factor * price) / ref_fx
        trade_size_usdt = min(target_usdt, available_spot_usdt)
        raw_qty = (trade_size_usdt * ref_fx) / price
        quantity = self.apply_precision(symbol, raw_qty, exch)

        if (quantity * price) < self.MIN_TRADE_VALUE_KRW:
            return False

        logger.info(
            f"🔫 SNIPING: {symbol} on {exch} | Prem: {opp['entry_premium']:.2f}% | Price: {price}"
        )

        # --- LEG 1: SPOT ENTRY ---
        try:
            order_res = await client.create_ioc_buy_order(symbol, price, quantity)
            filled_qty = float(order_res.get("filled", 0.0))
            avg_price = float(order_res.get("avg_price", price))

            if filled_qty <= 0:
                return False

            cost_krw = filled_qty * avg_price
            cost_usdt = cost_krw / ref_fx

            if cost_krw < (self.MIN_TRADE_VALUE_KRW * 0.5):
                logger.warning(f"   ⚠️ Dust fill ({cost_krw:.0f} KRW). Skipping hedge.")
                return False

        except Exception as e:
            logger.error(f"   ❌ Spot Entry Failed ({symbol}): {e}")
            return False

        # --- LEG 2: SMART HEDGE CHASE ---
        b_key = opp["binance_symbol_key"]
        bin_sym = f"{b_key}/USDT"
        hedge_qty = self.apply_precision(symbol, filled_qty, exch, is_binance=True)
        scaling = 1000.0 if b_key.startswith("1000") else 1.0

        # [NEW] Initialize Tracker
        cumulative_hedge_filled = 0.0

        await self._set_binance_leverage(f"{b_key}USDT", leverage=10)

        # Initial Target Price
        target_price_raw = (
            avg_price / (ref_fx * (1 + opp["entry_premium"] / 100.0))
        ) * scaling
        price_str = self.binance.price_to_precision(bin_sym, target_price_raw)

        current_order_id = None
        final_hedge_res = None
        is_filled = False

        # Chase Loop: 4 attempts x 15 seconds
        for attempt in range(1, 5):
            try:
                # 1. Place Limit Order (if not already active)
                if not current_order_id:
                    logger.info(
                        f"   ⏳ ({attempt}/4) Hedging {bin_sym} @ {price_str}..."
                    )
                    hedge_order = await self.binance.create_limit_sell_order(
                        symbol=bin_sym,
                        amount=hedge_qty,
                        price=price_str,
                        params={"positionSide": "SHORT"},
                    )
                    current_order_id = hedge_order["id"]
                    final_hedge_res = hedge_order

                # 2. Wait 15s (polling status)
                wait_start = asyncio.get_event_loop().time()
                while (asyncio.get_event_loop().time() - wait_start) < 15:
                    await asyncio.sleep(2.0)
                    order_status = await self.binance.fetch_order(
                        current_order_id, bin_sym
                    )
                    filled = float(order_status.get("filled", 0.0))

                    if order_status.get("status") == "closed" or filled >= hedge_qty:
                        final_hedge_res = order_status
                        is_filled = True
                        break

                if is_filled:
                    # [NEW] Sync tracker on success
                    cumulative_hedge_filled = hedge_qty
                    break

                # 3. Handle Timeout (Chase Logic)
                logger.info(f"   ⚠️ Attempt {attempt} Timed Out. Checking Market...")

                # Chase Logic
                book = await self.binance.fetch_order_book(bin_sym, limit=5)
                if not book["bids"]:
                    continue
                fresh_bid = float(book["bids"][0][0])
                fresh_bid_scaled = fresh_bid / scaling
                spot_usd = avg_price / ref_fx
                current_prem = ((spot_usd / fresh_bid_scaled) - 1) * 100

                if current_prem < -0.1:
                    logger.info(
                        f"   🔄 Chasing! Market moved (Prem: {current_prem:.2f}%). Repricing..."
                    )
                    if current_order_id:
                        try:
                            await self.binance.cancel_order(current_order_id, bin_sym)
                        except:
                            pass

                        await asyncio.sleep(0.5)

                        try:
                            cancel_status = await self.binance.fetch_order(
                                current_order_id, bin_sym
                            )
                            fill_amt = float(cancel_status.get("filled", 0.0))

                            if fill_amt > 0:
                                hedge_qty -= fill_amt
                                cumulative_hedge_filled += fill_amt  # Update tracker
                                hedge_qty = self.apply_precision(
                                    symbol, hedge_qty, exch, is_binance=True
                                )
                        except:
                            pass

                    if hedge_qty <= 0:
                        is_filled = True
                        break

                    price_str = self.binance.price_to_precision(bin_sym, fresh_bid)
                    current_order_id = None
                else:
                    break

            except Exception as e:
                logger.error(f"   ⚠️ Hedge Loop Error: {e}")
                await asyncio.sleep(1.0)

        # --- FINALIZATION ---
        try:
            # If still not filled after 4 loops or abort
            if not is_filled:
                # Same logic as before but using cumulative tracker inside the helper
                pass  # The exception handler below will catch the state

            # [REMOVED OLD PANIC LOGIC HERE, NOW HANDLED BY HELPER OR SUCCESS]

            # Success Calculation
            if is_filled:
                final_hedge_res = (
                    await self.binance.fetch_order(current_order_id, bin_sym)
                    if current_order_id
                    else final_hedge_res
                )

            hedge_price = float(
                final_hedge_res.get("average", 0) or final_hedge_res.get("price", 0)
            ) or float(price_str)

            # Recalculate totals based on actuals
            # Note: We use cumulative_hedge_filled if we looped, or hedge_qty if instant fill
            final_qty = (
                cumulative_hedge_filled if cumulative_hedge_filled > 0 else hedge_qty
            )

            hedge_val_usdt = final_qty * hedge_price
            spot_fee_val = (filled_qty * avg_price / ref_fx) * SPOT_FEE_RATE
            hedge_fee_val = hedge_val_usdt * HEDGE_FEE_RATE

            with SessionLocal() as db:
                conf_entry = self.thresholds.get(exch, {}).get("ENTRY", 0.0)
                pos = Position(
                    symbol=symbol,
                    exchange=exch,
                    status="OPEN",
                    current_spot_qty=filled_qty,
                    current_hedge_qty=final_qty,
                    entry_time=self.get_kst_now(),
                    entry_spot_qty=filled_qty,
                    entry_spot_price=avg_price,
                    entry_spot_amount_krw=filled_qty * avg_price,
                    entry_hedge_qty=final_qty,
                    entry_hedge_price=hedge_price,
                    entry_hedge_amount_usdt=hedge_val_usdt,
                    entry_usdt_rate=ref_fx,
                    config_entry_threshold=conf_entry,
                    calc_entry_premium=opp["entry_premium"],
                    entry_spot_order_id=str(order_res.get("id", "")),
                    entry_hedge_order_id=str(final_hedge_res.get("id", "")),
                )
                db.add(pos)
                db.commit()

            logger.info(f"   ✅ Position Opened: {symbol}")
            return True

        except Exception as e:
            logger.error(f"   🚨 SNIPE HEDGE FAILED {symbol}: {e}")

            # [FIX] Use shared rollback logic with cumulative tracker
            success, final_spot_qty, final_hedge_qty, final_res = (
                await self._handle_hedge_failure_and_rollback(
                    symbol=symbol,
                    exch=exch,
                    spot_qty=filled_qty,
                    spot_price=avg_price,
                    bin_order_id=current_order_id,
                    bin_symbol=bin_sym,
                    current_hedge_res=final_hedge_res,
                    fallback_price=price_str,
                    prior_fills=cumulative_hedge_filled,  # <--- PASSING THE TRACKER
                )
            )

            if not success:
                return False

            # Update local variables for the DB Save section (if we decided to keep it)
            # The code flow here assumes we might want to record a partial position.
            # However, _attempt_snipe usually returns False on error.
            # If _handle_hedge_failure_and_rollback returns True, it means we have a viable (though possibly partial) position.

            # Since we are inside the Exception block, we can't easily jump back to the DB block above without duplication.
            # Simple solution: Duplicate DB save here for the partial success case.

            filled_qty = final_spot_qty
            final_qty = final_hedge_qty
            hedge_price = float(final_res.get("average", 0) or price_str)

            with SessionLocal() as db:
                pos = Position(
                    symbol=symbol,
                    exchange=exch,
                    status="OPEN",
                    current_spot_qty=filled_qty,
                    current_hedge_qty=final_qty,
                    entry_time=self.get_kst_now(),
                    entry_spot_qty=filled_qty,
                    entry_spot_price=avg_price,
                    entry_spot_amount_krw=filled_qty * avg_price,
                    entry_hedge_qty=final_qty,
                    entry_hedge_price=hedge_price,
                    entry_hedge_amount_usdt=final_qty * hedge_price,
                    entry_usdt_rate=ref_fx,
                    config_entry_threshold=0.0,
                    calc_entry_premium=opp["entry_premium"],
                    entry_spot_order_id=str(order_res.get("id", "")),
                    entry_hedge_order_id="partial_rescue",
                )
                db.add(pos)
                db.commit()

            logger.info(f"   ✅ Position Saved (Partial/Rescue): {symbol}")
            return True

    async def run_exit_logic(self):
        active = self.get_active_positions()
        if not active:
            return

        # [PATCH] Fetch ALL Book Tickers instead of a specific list.
        # This fixes "No Price Found" errors caused by symbol format mismatches (e.g. FLUID/USDT vs FLUIDUSDT).
        try:
            tickers_map = await self.binance.fetch_bids_asks()
            bin_asks = {}
            for ticker in tickers_map.values():
                # We use the raw symbol (e.g., 'BTCUSDT') because that's what _manage_single_exit expects
                raw = ticker.get("info", {})
                sym = raw.get("symbol")
                if sym:
                    bin_asks[sym] = float(ticker.get("ask", 0) or 0)

        except Exception as e:
            logger.error(f"   ⚠️ Exit Logic Skipped (Binance Data): {e}")
            return

        # Execute Exits
        tasks = [self._manage_single_exit(pos, bin_asks) for pos in active]
        await asyncio.gather(*tasks)

    async def _check_position_health(self, pos: Position) -> Optional[float]:
        try:
            # [FIX 1] Use 'exchange' instead of 'spot_exchange'
            client = self.spot_clients.get(pos.exchange)
            if not client:
                return None
            bal = await client.fetch_balance()
            if not bal:
                return None
            coin_data = bal.get(pos.symbol, {})
            free_qty = float(coin_data.get("free", 0.0))
            total_qty = float(coin_data.get("total", 0.0))

            # [FIX 2] Use 'current_spot_qty' instead of 'spot_qty'
            required_qty = pos.current_spot_qty * 0.99

            if free_qty >= required_qty:
                return free_qty
            if total_qty >= required_qty and free_qty < required_qty:
                logger.warning(
                    f"   🔒 LOCKED FUNDS: {pos.symbol}. Cancelling open orders..."
                )
                await self._release_locked_funds(client, pos.symbol)
                return None
            if total_qty < required_qty:
                # [FIX] Do NOT close in DB here! Let sync_positions Ghost Sell handle it safely.
                logger.warning(
                    f"   🧟 ZOMBIE DETECTED: {pos.symbol}. Deferring to Ghost Sell Sync."
                )
                return None
            return None
        except Exception as e:
            logger.error(f"   ⚠️ Health Check Error: {e}")
            return None

    async def _release_locked_funds(self, client, symbol):
        try:
            open_orders = await client.fetch_open_orders(symbol)
            if not open_orders:
                return

            # [IMPROVEMENT] Log the action
            logger.info(
                f"   🔓 Releasing {len(open_orders)} locked orders for {symbol}..."
            )

            for order in open_orders:
                await client.cancel_order(order["id"], symbol)
                await asyncio.sleep(0.1)
        except Exception as e:
            # [IMPROVEMENT] Log the error instead of 'pass'
            logger.error(f"   ⚠️ Failed to release funds for {symbol}: {e}")

    async def _manage_single_exit(self, pos: Position, bin_map: Dict):
        symbol = pos.symbol
        exch = pos.exchange  # updated from spot_exchange

        lock = self.get_symbol_lock(symbol)
        if lock.locked():
            return

        async with lock:
            wallet_free_qty = await self._check_position_health(pos)
            if wallet_free_qty is None:
                return

            threshold = self.thresholds.get(exch, {}).get("EXIT", 0.0)
            from services.hedger import SYMBOL_MAP

            inv_map = {v: k for k, v in SYMBOL_MAP.items()}
            b_key = inv_map.get(symbol, symbol)
            scaling = 1000.0 if b_key.startswith("1000") else 1.0

            bin_sym_raw = f"{b_key}USDT"
            bin_ask = bin_map.get(bin_sym_raw)
            if not bin_ask:
                return

            bin_ask_unit = bin_ask / scaling
            ref_fx = self.shared_fx_rate
            raw_target = bin_ask_unit * ref_fx * (1 + threshold / 100.0)
            target_spot_price_krw = self.adjust_price_to_tick(raw_target)

            client = self.spot_clients.get(exch)

            # Use 'current_spot_qty' from DB for logic
            safe_qty = wallet_free_qty
            if exch == "BITHUMB":
                safe_qty -= 0.00000002
            qty = self.apply_precision(symbol, safe_qty, exch)

            trade_val = qty * target_spot_price_krw
            if trade_val < self.MIN_TRADE_VALUE_KRW:
                return

            try:
                res = await client.create_limit_sell_order(
                    symbol, target_spot_price_krw, qty
                )
                order_id = res.get("id") or res.get("uuid")

                await asyncio.sleep(5.0)

                await client.cancel_order(order_id, symbol)
                final_status = await client.fetch_order(order_id, symbol)
                filled = float(final_status.get("filled", 0.0))

                if filled > 0:
                    logger.info(f"   🎉 EXIT FILLED: {symbol} Qty: {filled}")

                    # Ratio based on ENTRY snapshot for hedge calculation
                    denom = pos.entry_spot_qty if pos.entry_spot_qty > 0 else 1.0
                    ratio = min(1.0, filled / denom)

                    # [CRITICAL FIX] Use current_hedge_qty so that synced ghost shorts are fully closed
                    hedge_close_qty = pos.current_hedge_qty * ratio
                    hedge_close_qty = self.apply_precision(
                        symbol, hedge_close_qty, exch, is_binance=True
                    )

                    hedge_exit_price = 0.0
                    hedge_exit_id = ""

                    if hedge_close_qty > 0:
                        b_sym_slash = f"{b_key}/USDT"

                        # [FIX] Use Safe Close Helper
                        success, ex_price, ex_id = await self._safe_close_hedge(
                            b_sym_slash, hedge_close_qty
                        )

                        if success:
                            # If price is 0 (already closed elsewhere), use last known or Estimate
                            hedge_exit_price = ex_price or bin_ask_unit
                            hedge_exit_id = ex_id or "safe_close"

                    # --- [NEW] UPDATE SINGLE POSITIONS TABLE ---
                    with SessionLocal() as db:
                        db_pos = db.query(Position).get(pos.id)

                        # Calculate PnL
                        exit_spot_krw = filled * target_spot_price_krw
                        gross_spot_pnl = exit_spot_krw - (
                            filled * db_pos.entry_spot_price
                        )
                        gross_hedge_pnl = (
                            db_pos.entry_hedge_price - hedge_exit_price
                        ) * hedge_close_qty

                        exit_val_usdt = exit_spot_krw / ref_fx
                        total_fees = (
                            db_pos.entry_hedge_amount_usdt + exit_val_usdt
                        ) * 0.0015
                        net_pnl = (
                            (gross_spot_pnl / ref_fx) + gross_hedge_pnl - total_fees
                        )

                        calc_exit_prem = 0.0
                        if hedge_exit_price > 0:
                            calc_exit_prem = (
                                (target_spot_price_krw / ref_fx) / hedge_exit_price - 1
                            ) * 100

                        # Update Exit Columns
                        db_pos.exit_time = self.get_kst_now()
                        db_pos.exit_spot_qty = (
                            filled  # Assuming full exit for simplicity of logging
                        )
                        db_pos.exit_spot_price = target_spot_price_krw
                        db_pos.exit_spot_amount_krw = exit_spot_krw
                        db_pos.exit_hedge_qty = hedge_close_qty
                        db_pos.exit_hedge_price = hedge_exit_price
                        db_pos.exit_hedge_amount_usdt = (
                            hedge_close_qty * hedge_exit_price
                        )

                        db_pos.gross_spot_pnl_krw = gross_spot_pnl
                        db_pos.gross_hedge_pnl_usdt = gross_hedge_pnl
                        db_pos.total_fees_usdt = total_fees
                        db_pos.net_pnl_usdt = net_pnl

                        db_pos.exit_usdt_rate = ref_fx
                        db_pos.config_exit_threshold = threshold
                        db_pos.calc_exit_premium = calc_exit_prem

                        db_pos.exit_spot_order_id = str(order_id)
                        db_pos.exit_hedge_order_id = hedge_exit_id

                        # Update Status & State
                        if filled >= (safe_qty * 0.98):
                            db_pos.status = "CLOSED"
                            db_pos.current_spot_qty = 0
                            db_pos.current_hedge_qty = 0
                            db.commit()
                            logger.info(
                                f"   ✅ Trade Closed: {symbol} | Net PnL: ${net_pnl:.2f}"
                            )
                        else:
                            db_pos.current_spot_qty -= filled
                            db_pos.current_hedge_qty -= hedge_close_qty
                            db.commit()
                            logger.info(
                                f"   📉 PARTIAL Exit: {symbol} | Batch Net: ${net_pnl:.2f}"
                            )

            except Exception as e:
                logger.error(f"   ⚠️ Exit Error ({symbol}): {e}")

    async def save_portfolio_snapshot(self):
        logger.info("📸 Capturing Portfolio Snapshot...")
        try:
            ref_fx = self.shared_fx_rate
            if ref_fx <= 0:
                logger.error("❌ Snapshot aborted: No valid FX rate available.")
                return

            await self.binance.load_time_difference()
            if ref_fx <= 100:
                ref_fx = 1450.0
            b_bal = await self.binance.fetch_balance()
            bin_free = float(b_bal["USDT"]["free"])
            bin_total = float(b_bal["USDT"]["total"])
            b_positions = await self.binance.fetch_positions()
            total_unrealized_pnl = sum(
                float(p.get("unrealizedPnl", 0)) for p in b_positions
            )

            details = {}
            total_spot_krw_value = 0.0
            total_spot_free_krw = 0.0

            for name, client in self.spot_clients.items():
                bal = await client.fetch_balance()
                coins_holding = [
                    c
                    for c, data in bal.items()
                    if c not in ["KRW", "USDT", "free", "used", "total", "info"]
                    and data.get("total", 0) > 0
                ]
                details[name] = {
                    "coins": {},
                    "free_krw": bal.get("KRW", {}).get("total", 0.0),
                }
                total_spot_free_krw += details[name]["free_krw"]
                price_map = await ValuationService.get_prices(
                    client, name, coins_to_price=coins_holding
                )

                for coin in coins_holding:
                    qty = bal[coin].get("total", 0.0)
                    price = price_map.get(coin, 0.0)
                    if price == 0:
                        continue
                    val_krw = qty * price
                    details[name]["coins"][coin] = {"qty": qty, "val_krw": val_krw}
                    total_spot_krw_value += val_krw

            total_equity_usdt = (
                bin_total
                + (total_spot_krw_value / ref_fx)
                + (total_spot_free_krw / ref_fx)
            )
            inv_val_usdt = total_spot_krw_value / ref_fx

            with SessionLocal() as db:
                snap = PortfolioSnapshot(
                    timestamp=self.get_kst_now(),
                    total_usdt_value=total_equity_usdt,
                    total_krw_value=total_equity_usdt * ref_fx,
                    binance_usdt_free=bin_free,
                    binance_unrealized_pnl=total_unrealized_pnl,
                    spot_krw_free=total_spot_free_krw,
                    fx_rate=ref_fx,
                    # [FIX] Dump dict to JSON string for DB
                    details=json.dumps(details),
                    inventory_val_usdt=inv_val_usdt,
                )
                db.add(snap)
                db.commit()
            logger.info(
                f"   ✅ Snapshot Saved. Total Equity: ${total_equity_usdt:,.2f}"
            )
        except Exception as e:
            logger.error(f"   ❌ Snapshot Error: {e}")
