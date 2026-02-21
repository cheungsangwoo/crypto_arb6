# services/hedger.py

# --- 1. MANUAL INTERVENTION LIST ---
# Use this ONLY for:
#   A) Coins with "1000" prefix on Binance (e.g. 1000PEPE)
#   B) Coins with different tickers (e.g. Binance: HOT -> Upbit: HOLO)
import logging

logger = logging.getLogger(__name__)

MANUAL_OVERRIDES = {
    # Memes (1000 prefix)
    "1000PEPE": "PEPE",
    "1000BONK": "BONK",
    "1000FLOKI": "FLOKI",
    "1000SHIB": "SHIB",
    "1000XEC": "XEC",
    "1000SATS": "SATS",
    "1000RATS": "RATS",
    "1000LUNC": "LUNC",
    "1000BTT": "BTT",
    "1000X": "X",
    "1000CAT": "CAT",
    "1000MOG": "MOG",
    # Ticker Mismatches (Binance Key -> Spot Value)
    "HOT": "HOLO",  # Binance: HOT, Upbit: HOLO
    "HOLO": "HOLO",  # Safety
    "BTTC": "BTT",  # Binance: BTTC
    "POLYX": "POLY",  # Check specific listing (Polymath vs Polymesh)
    "WAXP": "WAX",
    "LUNA2": "LUNA",  # Terra 2.0
}

# --- 2. THE DYNAMIC MAP ---
# This dictionary will be populated automatically at runtime.
# Structure: { "BINANCE_FUTURES_SYMBOL": "SPOT_SYMBOL" }
# Example: { "BTC": "BTC", "ETH": "ETH", "1000PEPE": "PEPE" }
SYMBOL_MAP = MANUAL_OVERRIDES.copy()


def update_symbol_map(binance_markets, spot_markets_list):
    """
    Cross-references Binance Futures against available Spot Markets.
    Updates SYMBOL_MAP in-place to cast the 'widest net' possible.

    Args:
        binance_markets: List or Dict of Binance market keys (e.g. ['BTC/USDT', ...])
        spot_markets_list: List of all 'KRW-XXX' markets from Upbit/Bithumb
    """
    global SYMBOL_MAP

    # 1. Gather all unique Spot Assets (e.g. 'BTC', 'ETH', 'PEPE')
    available_spot_assets = set()
    for m in spot_markets_list:
        # Handle Upbit/Bithumb format: 'KRW-BTC'
        ticker = m.get("market", "")
        if ticker.startswith("KRW-"):
            asset = ticker.split("-")[1]
            available_spot_assets.add(asset)

    # 2. Iterate Binance Futures to find matches
    # We look for ANY Binance Future that has a matching Spot partner
    count_new = 0
    for b_symbol in binance_markets:
        # We only care about USDT futures
        if not b_symbol.endswith("/USDT"):
            continue

        # Clean Symbol: 'BTC/USDT' -> 'BTC'
        f_base = b_symbol.split("/")[0]

        # CASE A: Direct Match (The vast majority: BTC, ETH, XRP)
        if f_base in available_spot_assets:
            if f_base not in SYMBOL_MAP:
                SYMBOL_MAP[f_base] = f_base
                count_new += 1

        # CASE B: Already in Manual Overrides (e.g. 1000PEPE)
        # We just verify the spot asset actually exists to be safe
        elif f_base in MANUAL_OVERRIDES:
            spot_target = MANUAL_OVERRIDES[f_base]
            if spot_target not in available_spot_assets:
                # Warning: We have a map for it, but Spot doesn't list it?
                # We keep it in the map anyway, Scanner will filter it out later if orderbook fails.
                pass

    if count_new > 0:
        logger.info(
            f"   🗺️  Hedger: Auto-discovered {count_new} new trading pairs (Total: {len(SYMBOL_MAP)})"
        )
