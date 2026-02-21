# Coinone Integration Guide

## Overview

Your cryptocurrency arbitrage bot now supports **Coinone** as a third spot exchange alongside Upbit and Bithumb. This enables the same long spot / short futures arbitrage strategy across three Korean exchanges.

## What Was Added

### 1. **CoinoneClient** (`clients/coinone_client.py`)
A complete implementation of the `BaseSpotClient` interface for Coinone with:
- **Authentication**: HMAC-SHA256 signed requests
- **Market Data Methods**:
  - `fetch_ticker()` - Get current price
  - `fetch_orderbooks()` - Batch orderbook fetching
  - `fetch_balance()` - Account balance
- **Order Execution**:
  - `create_limit_buy_order()`
  - `create_limit_sell_order()`
  - `create_market_sell_order()`
  - `create_ioc_buy_order()` - Immediate-or-Cancel orders (wait 1.5s then cancel unfilled)
- **Order Management**:
  - `fetch_order()` - Check order status
  - `cancel_order()` - Cancel individual order
  - `fetch_open_orders()` - List pending orders
  - `cancel_all_orders()` - Emergency cancellation

### 2. **main.py Updates**
- Imported `CoinoneClient`
- Added automatic initialization if `COINONE_ACCESS_KEY` environment variable is present
- Added Coinone to pause/resume logic
- Added Coinone to emergency shutdown cancellation

### 3. **bot_config.json Updates**
Added Coinone with sensible default thresholds:
```json
"COINONE": {
    "ENTRY": -0.3,
    "EXIT": 0.7
}
```
These can be auto-tuned by the optimizer or manually adjusted.

### 4. **Environment Configuration**
Created `.env.example` with Coinone credentials template:
```
COINONE_ACCESS_KEY=your_coinone_access_key_here
COINONE_SECRET_KEY=your_coinone_secret_key_here
```

### 5. **PositionManager Updates**
Added "COINONE" to `exchange_slots` dictionary for capacity tracking.

## Setup Instructions

### Step 1: Get Coinone API Credentials

1. Create a Coinone account at https://coinone.co.kr (if not already done)
2. Go to **Settings → API Management**
3. Create a new API key with these permissions:
   - ✅ Basic Information
   - ✅ Balance
   - ✅ Trading (Buy/Sell)
   - ✅ Order History

4. You'll receive:
   - `Access Token` (API Key)
   - `Secret Key`

### Step 2: Add to .env File

Create or edit your `.env` file in the root directory:

```bash
# Existing exchanges
UPBIT_ACCESS_KEY=your_upbit_key
UPBIT_SECRET_KEY=your_upbit_secret

BITHUMB_ACCESS_KEY=your_bithumb_key
BITHUMB_SECRET_KEY=your_bithumb_secret

# New Coinone exchange
COINONE_ACCESS_KEY=your_coinone_access_token
COINONE_SECRET_KEY=your_coinone_secret_key
```

### Step 3: Start the Bot

No additional setup needed! The bot will automatically detect Coinone and enable it:

```bash
python main.py
```

You should see in the logs:
```
✅ Upbit Enabled
✅ Bithumb Enabled
✅ Coinone Enabled
```

## How It Works

### Market Scanning
The bot scans all three exchanges (Upbit, Bithumb, Coinone) for arbitrage opportunities. It looks for:
- KRW price differences across exchanges
- Spread vs. entry/exit thresholds defined in `bot_config.json`

### Trade Execution
When an opportunity is found:
1. **Spot Entry**: Places IOC buy order on the cheapest exchange(s)
   - Waits 1.5 seconds for matching
   - Cancels any unfilled portion
2. **Futures Hedge**: Simultaneously enters short on Binance futures
3. **Position Tracking**: Stores positions with exchange name for later unwind

### Position Unwinding (Exit)
The exit logic runs continuously to:
1. **Detect Ready Exits**: When unrealized PnL crosses EXIT threshold
2. **Execute Sells**: 
   - Sells spot position on the exchange it was bought
   - Cancels futures short position on Binance
3. **State Cleanup**: Removes closed positions from database

## Configuration Tuning

### Default Entry/Exit Thresholds

```json
{
    "UPBIT": {
        "ENTRY": 0.0,      // Buy if premium >= 0%
        "EXIT": 1.0        // Sell when gain >= 1%
    },
    "BITHUMB": {
        "ENTRY": -0.5,     // Buy if premium >= -0.5%
        "EXIT": 0.5        // Sell when gain >= 0.5%
    },
    "COINONE": {
        "ENTRY": -0.3,     // Buy if premium >= -0.3%
        "EXIT": 0.7        // Sell when gain >= 0.7%
    }
}
```

### Adjusting Thresholds

You can manually tune these values:
- **Lower ENTRY threshold**: More aggressive buying (higher volume, more risk)
- **Lower EXIT threshold**: Take profits faster (lower yield)
- Or use the built-in optimizer to auto-tune them

## Monitoring & Debugging

### Check bot logs
```bash
tail -f data/logs/bot_execution.log
```

### Test Coinone connectivity
```bash
python -c "
from clients.coinone_client import CoinoneClient
import asyncio
import os

async def test():
    client = CoinoneClient(
        'COINONE',
        os.getenv('COINONE_ACCESS_KEY'),
        os.getenv('COINONE_SECRET_KEY')
    )
    balance = await client.fetch_balance()
    print('Balance:', balance)
    await client.close()

asyncio.run(test())
"
```

## Common Issues & Solutions

### Issue: "❌ No spot clients enabled"
**Solution**: Check that at least one of these is set in `.env`:
- `UPBIT_ACCESS_KEY` + `UPBIT_SECRET_KEY`
- `BITHUMB_ACCESS_KEY` + `BITHUMB_SECRET_KEY`
- `COINONE_ACCESS_KEY` + `COINONE_SECRET_KEY`

### Issue: Coinone orders failing with signature error
**Solution**: Ensure `COINONE_SECRET_KEY` is set correctly (not the Access Token)

### Issue: Orders not appearing in bot logs
**Solution**: 
1. Verify API permissions include "Trading"
2. Check that premium threshold is being hit (use dashboard or logs to see premiums)
3. Ensure account has sufficient KRW balance

## Database Considerations

Positions are stored with the exchange name, so Coinone positions are tracked independently:
- Position table stores `exchange` field ("UPBIT", "BITHUMB", or "COINONE")
- Unwind logic respects this and sells on the correct exchange
- Each exchange can have at most `MAX_POSITIONS` open trades

## Safety Features

All three exchanges integrated with the same safety mechanisms:
- **Graceful Shutdown**: Cancels all open orders when `Ctrl+C` is pressed
- **Pause Mode**: Can pause/resume via `bot_config.json` `SYSTEM.PAUSED`
- **Rate Limiting**: Built-in delays between API calls
- **Error Handling**: Gracefully handles API timeouts and errors
- **State Reconciliation**: On startup, syncs database with actual wallet balances

## Performance Notes

- Coinone API limits are typically similar to Upbit/Bithumb
- Orderbook fetching is batched (10 symbols per request)
- Each exchange operates independently - adding Coinone has minimal performance impact
- IOC timeout set to 1.5 seconds (tunable in `create_ioc_buy_order`)

## Next Steps

1. ✅ Load Coinone API credentials in `.env`
2. ✅ Run the bot: `python main.py`
3. Monitor the logs and dashboard
4. Optionally tune thresholds in `bot_config.json`
5. Monitor positions and profits

Enjoy risk-free arbitrage across three Korea exchanges! 🚀
