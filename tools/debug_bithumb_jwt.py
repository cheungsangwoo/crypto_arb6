    async def save_portfolio_snapshot(self):
        logger.info("📸 Capturing Portfolio Snapshot...")
        try:
            # 1. Fetch Global FX
            ref_fx = 1450.0
            upbit = self.spot_clients.get("UPBIT")
            if upbit:
                try:
                    t = await upbit.fetch_ticker("USDT/KRW")
                    raw = t.get("info", {}).get("trade_price")
                    ref_fx = float(raw) if raw else t.get("last", 1450.0)
                except Exception:
                    ref_fx = 1450.0

            if ref_fx <= 100:
                ref_fx = 1450.0

            # 2. Binance Data
            b_bal = await self.binance.fetch_balance()
            bin_free = float(b_bal["USDT"]["free"])
            bin_total = float(b_bal["USDT"]["total"])

            b_positions = await self.binance.fetch_positions()
            total_unrealized_pnl = sum(
                float(p.get("unrealizedPnl", 0)) for p in b_positions
            )

            # 3. Iterate Spot Exchanges
            details = {}
            total_spot_krw_value = 0.0
            total_spot_free_krw = 0.0

            for name, client in self.spot_clients.items():
                bal = await client.fetch_balance()

                # Prepare list of coins we hold for the Fallback strategy
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

                # --- HYBRID PRICING CALL ---
                # Pass the 'coins_holding' list so it can loop if bulk fails
                price_map = await ValuationService.get_prices(
                    client, name, coins_to_price=coins_holding
                )

                for coin in coins_holding:
                    qty = bal[coin].get("total", 0.0)

                    # LOOKUP PRICE (From Bulk or Fallback Loop)
                    price = price_map.get(coin, 0.0)

                    if price == 0:
                        # Only warn if it's not a known ghost coin
                        if coin != "P" and (qty * ref_fx > 1):
                            logger.warning(f"   ⚠️ Unpriced Asset: {coin} on {name}")
                        continue

                    val_krw = qty * price
                    details[name]["coins"][coin] = {
                        "qty": qty,
                        "val_krw": val_krw,
                    }
                    total_spot_krw_value += val_krw

            # 4. Final Calculations
            total_equity_usdt = (
                bin_total
                + (total_spot_krw_value / ref_fx)
                + (total_spot_free_krw / ref_fx)
            )

            with SessionLocal() as db:
                snap = PortfolioSnapshot(
                    timestamp=self.get_kst_now(),
                    total_usdt_value=total_equity_usdt,
                    total_krw_value=total_equity_usdt * ref_fx,
                    binance_usdt_free=bin_free,
                    binance_unrealized_pnl=total_unrealized_pnl,
                    spot_krw_free=total_spot_free_krw,
                    fx_rate=ref_fx,
                    details=details,
                )
                db.add(snap)
                db.commit()
            logger.info(
                f"   ✅ Snapshot Saved. Total Equity: ${total_equity_usdt:,.2f}"
            )
        except Exception as e:
            logger.error(f"   ❌ Snapshot Error: {e}")
