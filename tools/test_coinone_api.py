#!/usr/bin/env python3
"""
Quick diagnostic script to test Coinone API connectivity and response format.
"""
import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

# Test public endpoints
print("=" * 60)
print("COINONE API DIAGNOSTIC")
print("=" * 60)

# Test 1: Markets endpoint
print("\n1. Testing /public/v2/markets endpoint:")
try:
    url = "https://api.coinone.co.kr/public/v2/markets"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    res = requests.get(url, headers=headers, timeout=5)
    print(f"   Status: {res.status_code}")
    data = res.json()
    print(f"   Response type: {type(data)}")
    print(f"   Response keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
    if isinstance(data, dict) and "markets" in data:
        print(f"   Markets found: {len(data['markets'])}")
        if data["markets"]:
            print(f"   First market sample: {json.dumps(data['markets'][0], indent=2)}")
    elif isinstance(data, dict):
        print(f"   Full response (first 500 chars): {json.dumps(data, indent=2)[:500]}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test 2: Ticker endpoint
print("\n2. Testing /public/v2/ticker endpoint (BTC):")
try:
    url = "https://api.coinone.co.kr/public/v2/ticker?currency=btc&quote_currency=krw"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    res = requests.get(url, headers=headers, timeout=5)
    print(f"   Status: {res.status_code}")
    data = res.json()
    print(f"   Response type: {type(data)}")
    print(f"   Response keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
    if isinstance(data, dict):
        print(f"   Response (first 500 chars): {json.dumps(data, indent=2)[:500]}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test 3: Orderbook endpoint
print("\n3. Testing /public/v2/orderbook endpoint (BTC):")
try:
    url = (
        "https://api.coinone.co.kr/public/v2/orderbook?currency=btc&quote_currency=krw"
    )
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    res = requests.get(url, headers=headers, timeout=5)
    print(f"   Status: {res.status_code}")
    data = res.json()
    print(f"   Response type: {type(data)}")
    print(f"   Response keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
    if isinstance(data, dict):
        print(f"   Response (first 500 chars): {json.dumps(data, indent=2)[:500]}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test 4: Authentication check
print("\n4. Testing Authentication (Private Endpoint):")
api_key = os.getenv("COINONE_ACCESS_KEY")
secret_key = os.getenv("COINONE_SECRET_KEY")

if not api_key or not secret_key:
    print("   ⚠️ No COINONE_ACCESS_KEY or COINONE_SECRET_KEY in .env")
else:
    print("   ✅ API credentials found in .env")
    print(f"   Access Key: {api_key[:10]}...")

    # Try to call balance endpoint
    import hmac
    import hashlib
    import time

    try:
        nonce = str(int(time.time() * 1000))
        message = "GET" + "/v2/account/balance" + nonce
        signature = hmac.new(
            secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "X-COINONE-SIGNATURE": signature,
            "X-COINONE-NONCE": nonce,
        }

        url = "https://api.coinone.co.kr/v2/account/balance"
        res = requests.get(url, headers=headers, timeout=5)
        print(f"   Status: {res.status_code}")
        print(f"   Response: {res.text[:300]}")
    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\n" + "=" * 60)
