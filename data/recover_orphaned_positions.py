"""
Recovery script: Creates DB entries + Binance SHORTs for 6 orphaned Bithumb positions
that have no DB records due to the Windows Update shutdown.
Also tags ghost-closed positions (110, 123, 127) with exit_reason=GHOST_SELL.
Run once with: .venv/Scripts/python.exe data/recover_orphaned_positions.py
"""
import asyncio
import io
import os
import sys
import math
from datetime import datetime, timezone, timedelta

# Windows UTF-8 fix for emoji output
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

import ccxt.async_support as ccxt
from database.session import SessionLocal
from database.models import Position
from clients.bithumb_client import BithumbClient


def get_kst_now():
    return datetime.now(timezone(timedelta(hours=9))).replace(tzinfo=None)


async def main():
    bithumb = BithumbClient(
        "BITHUMB",
        os.getenv("BITHUMB_ACCESS_KEY"),
        os.getenv("BITHUMB_SECRET_KEY"),
    )

    binance = ccxt.binance(
        {
            "apiKey": os.getenv("BINANCE_HEDGE_API_KEY"),
            "secret": os.getenv("BINANCE_HEDGE_SECRET_KEY"),
            "options": {"defaultType": "future", "adjustForTimeDifference": True},
        }
    )

    try:
        await binance.load_time_difference()
        await binance.load_markets()

        # Ensure hedge (dual-side) mode
        try:
            await binance.fapiPrivatePostPositionSideDual({"dualSidePosition": "true"})
        except Exception:
            pass

        # ── FX rate ────────────────────────────────────────────────────────────
        btc_usdt_t = await binance.fetch_ticker("BTC/USDT:USDT")
        btc_usdt = btc_usdt_t.get("last", 0.0)
        btc_krw_t = await bithumb.fetch_ticker("BTC")
        btc_krw = btc_krw_t.get("last", 0.0)
        fx = btc_krw / btc_usdt if btc_usdt > 0 else 1450.0
        print(f"FX rate: {fx:.2f} KRW/USDT\n")

        # ── Bithumb wallet ─────────────────────────────────────────────────────
        balance = await bithumb.fetch_balance()
        print("=== BITHUMB WALLET (total, incl. locked in open orders) ===")

        # Symbols to recover — query balance to get live quantities
        # (user screenshot showed: A, KSM, APE, SKY, KERNEL, YB)
        candidates = ["A", "KSM", "APE", "SKY", "KERNEL", "YB"]

        results = []
        for sym in candidates:
            qty = float(balance.get(sym, {}).get("total", 0.0))
            if qty <= 0:
                print(f"  ⚠️  {sym}: 0 balance — may already be sold or ticker differs")
                continue

            ticker = await bithumb.fetch_ticker(sym)
            spot_price = ticker.get("last", 0.0)
            val_krw = qty * spot_price

            # Check Binance USDT-M perpetual
            bin_sym = f"{sym}/USDT:USDT"
            has_futures = bin_sym in binance.markets
            bin_price = 0.0
            if has_futures:
                try:
                    ft = await binance.fetch_ticker(bin_sym)
                    bin_price = ft.get("bid", 0.0) or ft.get("last", 0.0)
                except Exception as e:
                    print(f"  ⚠️  Binance ticker error for {sym}: {e}")
                    has_futures = False

            results.append(
                {
                    "symbol": sym,
                    "qty": qty,
                    "spot_price": spot_price,
                    "val_krw": val_krw,
                    "has_futures": has_futures,
                    "bin_sym": bin_sym,
                    "bin_price": bin_price,
                }
            )
            fut_info = f"Binance futures @ {bin_price:.6f} USDT" if has_futures else "NO Binance futures"
            print(
                f"  {'✅' if has_futures else '⚠️ '} {sym}: {qty:.4f} "
                f"@ {spot_price:.2f} KRW = {val_krw:,.0f} KRW | {fut_info}"
            )

        print()

        # ── Create DB entries + open Binance SHORTs ────────────────────────────
        print("=== CREATING DB ENTRIES + BINANCE SHORTS ===")
        with SessionLocal() as db:
            for r in results:
                sym = r["symbol"]
                qty = r["qty"]
                spot_price = r["spot_price"]
                bin_price = r["bin_price"]

                existing = (
                    db.query(Position)
                    .filter(
                        Position.symbol == sym,
                        Position.exchange == "BITHUMB",
                        Position.status == "OPEN",
                    )
                    .first()
                )
                if existing:
                    print(f"  ⏭️  {sym}: OPEN position already in DB (ID={existing.id}), skipping")
                    continue

                hedge_qty = round(qty, 2) if r["has_futures"] else 0.0

                pos = Position(
                    symbol=sym,
                    exchange="BITHUMB",
                    status="OPEN",
                    entry_time=get_kst_now(),
                    entry_spot_price=spot_price,
                    entry_spot_qty=qty,
                    current_spot_qty=qty,
                    entry_spot_amount_krw=qty * spot_price,
                    entry_hedge_price=bin_price if r["has_futures"] else 0.0,
                    entry_hedge_qty=hedge_qty,
                    current_hedge_qty=hedge_qty,
                    entry_hedge_amount_usdt=(hedge_qty * bin_price) if r["has_futures"] else 0.0,
                    entry_usdt_rate=fx,
                    config_entry_threshold=0,
                    calc_entry_premium=0,
                )
                db.add(pos)
                db.flush()
                print(f"  📝 DB entry created: {sym} ID={pos.id} | {qty:.4f} @ {spot_price:.2f} KRW")

                # Open Binance SHORT
                if r["has_futures"] and hedge_qty > 0:
                    # Determine qty precision from Binance market spec
                    try:
                        market = binance.markets[r["bin_sym"]]
                        amt_prec = market.get("precision", {}).get("amount", 0.01)
                        if amt_prec >= 1:
                            hedge_qty_precise = float(int(hedge_qty))
                        else:
                            decimals = int(round(-math.log10(amt_prec)))
                            hedge_qty_precise = round(hedge_qty, decimals)
                    except Exception:
                        hedge_qty_precise = round(hedge_qty, 2)

                    # Set 10x leverage
                    try:
                        await binance.fapiPrivatePostLeverage(
                            {"symbol": sym + "USDT", "leverage": 10}
                        )
                    except Exception as lev_err:
                        print(f"    ⚠️  Leverage set failed for {sym}: {lev_err}")

                    # Open SHORT (market order at best available price)
                    try:
                        order = await binance.create_market_sell_order(
                            r["bin_sym"],
                            hedge_qty_precise,
                            params={"positionSide": "SHORT"},
                        )
                        fill_price = float(order.get("average", 0) or bin_price)
                        pos.entry_hedge_price = fill_price
                        pos.entry_hedge_amount_usdt = hedge_qty * fill_price
                        pos.current_hedge_qty = hedge_qty_precise
                        pos.entry_hedge_qty = hedge_qty_precise
                        print(
                            f"    🛡️  Binance SHORT opened: {sym} qty={hedge_qty_precise} @ {fill_price:.6f}"
                        )
                    except Exception as ord_err:
                        print(f"    ❌ Binance SHORT FAILED for {sym}: {ord_err}")
                        pos.entry_hedge_qty = 0.0
                        pos.current_hedge_qty = 0.0
                        pos.entry_hedge_price = 0.0
                        pos.entry_hedge_amount_usdt = 0.0
                else:
                    print(f"    ⚠️  {sym}: no Binance futures — spot only, exit manually on Bithumb")

                db.commit()

        # ── Tag ghost-closed positions ──────────────────────────────────────────
        print("\n=== TAGGING GHOST-CLOSED POSITIONS ===")
        with SessionLocal() as db:
            for pid in [110, 123, 127]:
                p = db.query(Position).get(pid)
                if p:
                    if p.exit_reason is None:
                        p.exit_reason = "GHOST_SELL"
                        print(f"  📝 Position {pid} ({p.symbol}/{p.exchange}): exit_reason = GHOST_SELL")
                    else:
                        print(f"  ⏭️  Position {pid}: exit_reason already set to '{p.exit_reason}'")
                else:
                    print(f"  ⚠️  Position {pid}: not found in DB")
            db.commit()

        print("\n✅ Recovery complete!")

    finally:
        await binance.close()


if __name__ == "__main__":
    asyncio.run(main())
