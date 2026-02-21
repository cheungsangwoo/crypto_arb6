import logging
import asyncio
import ccxt.async_support as ccxt
import os
from clients.coinone_client import CoinoneClient

logger = logging.getLogger(__name__)


class ValuationService:
    @staticmethod
    def _find_ccxt_instance(wrapper):
        """
        Scans wrapper to find the underlying CCXT object.
        """
        candidates = ["client", "exchange", "api", "_client", "_exchange"]
        for attr in candidates:
            if hasattr(wrapper, attr):
                obj = getattr(wrapper, attr)
                if hasattr(obj, "fetch_tickers"):
                    return obj
        try:
            for name, obj in wrapper.__dict__.items():
                if hasattr(obj, "fetch_tickers") and hasattr(obj, "id"):
                    return obj
        except:
            pass
        return None

    @staticmethod
    async def _create_temp_client(exchange_name):
        """
        Creates a fresh, temporary CCXT instance or custom client.
        """
        try:
            if exchange_name == "UPBIT":
                config = {
                    "apiKey": os.getenv(f"{exchange_name}_ACCESS_KEY"),
                    "secret": os.getenv(f"{exchange_name}_SECRET_KEY"),
                    "enableRateLimit": True,
                }
                return ccxt.upbit(config)
            elif exchange_name == "BITHUMB":
                config = {
                    "apiKey": os.getenv(f"{exchange_name}_ACCESS_KEY"),
                    "secret": os.getenv(f"{exchange_name}_SECRET_KEY"),
                    "enableRateLimit": True,
                }
                return ccxt.bithumb(config)
            elif exchange_name == "COINONE":
                return CoinoneClient(
                    "COINONE",
                    os.getenv("COINONE_ACCESS_KEY", ""),
                    os.getenv("COINONE_SECRET_KEY", ""),
                )
        except Exception:
            return None
        return None

    @staticmethod
    def _extract_price(ticker):
        """Helper to safely extract price from ticker"""
        raw = ticker.get("info", {})
        return float(
            ticker.get("last")
            or ticker.get("close")
            or raw.get("trade_price")
            or raw.get("closing_price")
            or 0.0
        )

    @staticmethod
    async def get_prices(client_wrapper, exchange_name, coins_to_price=None):
        """
        Robust pricing engine with Cross-Rate support (BTC/USDT markets).
        """
        price_map = {}
        temp_client_used = False

        # 1. Acquire CCXT Object
        ccxt_client = ValuationService._find_ccxt_instance(client_wrapper)
        if not ccxt_client:
            ccxt_client = await ValuationService._create_temp_client(exchange_name)
            temp_client_used = True

        if not ccxt_client:
            logger.error(
                f"   ❌ Could not establish pricing connection for {exchange_name}"
            )
            return {}

        # --- STRATEGY 1: BULK FETCH (FAST) ---
        try:
            tickers = await ccxt_client.fetch_tickers()

            # PASS 1: Direct KRW Pairs
            for symbol, ticker in tickers.items():
                base_coin = None

                # Standard CCXT: "BTC/KRW" -> Base: BTC
                if symbol.endswith("/KRW"):
                    base_coin = symbol.split("/")[0]
                # Upbit Raw: "KRW-BTC" -> Base: BTC
                elif symbol.startswith("KRW-"):
                    base_coin = symbol.replace("KRW-", "")

                if base_coin:
                    price = ValuationService._extract_price(ticker)
                    if price > 0:
                        price_map[base_coin] = price

            # PASS 2: Cross-Rates (BTC/USDT Markets)
            # Only needed for coins we didn't find in Pass 1
            ref_btc_krw = price_map.get("BTC", 0.0)
            ref_usdt_krw = price_map.get("USDT", 0.0)

            for symbol, ticker in tickers.items():
                # Skip if we already processed KRW pairs
                if "/KRW" in symbol or symbol.startswith("KRW-"):
                    continue

                base_coin = None
                quote_currency = None
                conversion_rate = 0.0

                # Check BTC Pairs (e.g. JUV/BTC)
                if ref_btc_krw > 0 and (
                    symbol.endswith("/BTC") or symbol.startswith("BTC-")
                ):
                    if symbol.endswith("/BTC"):
                        base_coin = symbol.split("/")[0]
                    elif symbol.startswith("BTC-"):
                        base_coin = symbol.replace("BTC-", "")
                    quote_currency = "BTC"
                    conversion_rate = ref_btc_krw

                # Check USDT Pairs (e.g. ALICE/USDT)
                elif ref_usdt_krw > 0 and (
                    symbol.endswith("/USDT") or symbol.startswith("USDT-")
                ):
                    if symbol.endswith("/USDT"):
                        base_coin = symbol.split("/")[0]
                    elif symbol.startswith("USDT-"):
                        base_coin = symbol.replace("USDT-", "")
                    quote_currency = "USDT"
                    conversion_rate = ref_usdt_krw

                # If valid cross-pair found AND we don't have a KRW price yet
                if base_coin and base_coin not in price_map:
                    raw_price = ValuationService._extract_price(ticker)
                    if raw_price > 0:
                        final_krw_price = raw_price * conversion_rate
                        price_map[base_coin] = final_krw_price
                        # logger.debug(f"   💱 Derived {base_coin} price via {quote_currency}: {final_krw_price:,.0f} KRW")

            if temp_client_used:
                await ccxt_client.close()

            return price_map

        except Exception as e:
            pass
            # logger.warning(f"   ⚠️ Bulk Fetch failed for {exchange_name}: {e}")

        # --- STRATEGY 2: FALLBACK INDIVIDUAL FETCH ---
        # Only runs if Bulk Fetch crashed
        if coins_to_price:
            for coin in coins_to_price:
                if coin in ["KRW", "USDT"]:
                    continue
                try:
                    await asyncio.sleep(0.05)

                    # Try KRW first
                    target_symbol = f"{coin}/KRW"
                    quote_rate = 1.0

                    try:
                        ticker = await ccxt_client.fetch_ticker(target_symbol)
                    except:
                        # If KRW fails, try BTC
                        target_symbol = f"{coin}/BTC"
                        try:
                            ticker = await ccxt_client.fetch_ticker(target_symbol)
                            # Fetch BTC price if we don't have it
                            if "BTC" not in price_map:
                                btc_t = await ccxt_client.fetch_ticker("BTC/KRW")
                                price_map["BTC"] = ValuationService._extract_price(
                                    btc_t
                                )
                            quote_rate = price_map.get("BTC", 0.0)
                        except:
                            continue  # Give up

                    raw_price = ValuationService._extract_price(ticker)
                    if raw_price > 0 and quote_rate > 0:
                        price_map[coin] = raw_price * quote_rate

                except Exception:
                    pass

        if temp_client_used:
            await ccxt_client.close()

        return price_map
