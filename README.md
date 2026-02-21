# 🛡️ Hedged Holder Bot (Arbitrage V3 - Self-Optimizing)

A sophisticated Python-based arbitrage bot that captures the **Kimchi Premium** risk-free. 
It executes a **Market Neutral** strategy (Long Spot KRW / Short Futures USDT) and features an autonomous optimizer that tunes trading thresholds in real-time based on market conditions.

## ✨ Key Features

### 🧠 Autonomous Intelligence
* **Background Optimizer:** A dedicated thread runs in parallel with the trading engine, continuously simulating the last 24-72 hours of market data to find the optimal `ENTRY` and `EXIT` thresholds.
* **Auto-Tuning:** Automatically updates `bot_config.json` with the best parameters for the current market regime without stopping the bot.
* **Market Temperature Tracking:** Calculates and logs median premiums for the entire investable universe (Upbit/Bithumb) to detect market-wide shifts.

### ⚡ Execution & Safety
* **Risk-Free Hedging:** 1:1 hedging ensures price movements don't affect PnL. Uses `Immediate-Or-Cancel` (IOC) for Spot and `Limit` orders for Futures to minimize slippage.
* **Smart State Management:** * **Ghost Sell Protection:** Distinguishes between "locked in order" and "sold" assets to prevent panic-closing hedges.
    * **Zombie Detection:** Automatically detects and repairs state mismatches between the database and exchange wallets.
* **Graceful Shutdown:** Safely cancels open orders, closes DB connections, and stops background threads on `Ctrl+C`.

### 📊 Real-Time Dashboard
* **Live PnL Tracking:** Calculates unrealized profit in real-time by joining active positions with live market ticks.
* **Portfolio Charting:** Visualizes USDT equity performance over the last 24 hours.
* **Manual Override:** Resume/Pause the bot and manually tweak thresholds directly from the UI.

## 🚀 Quick Start

### 1. Prerequisites
* Python 3.10+
* MySQL Database (Local or Remote)
* **Upbit Account** (API Keys with Trade/Asset permissions)
* **Binance Account** (Futures enabled, API Keys with Futures permissions)

### 2. Installation
```bash
# Clone the repository
git clone [https://github.com/your-username/crypto_arb4.git](https://github.com/your-username/crypto_arb4.git)
cd crypto_arb4

# Install dependencies
pip install -r requirements.txt