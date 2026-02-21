import requests
import pandas as pd
import time
from datetime import datetime, timedelta

def fetch_usdt_krw_history():
    # Configuration
    market = "KRW-USDT"
    unit = 1  # 1-minute candles
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    
    # Calculate time range (Last 24 hours)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    
    print(f"Fetching {market} data from {start_time} to {end_time} (UTC)...")

    all_candles = []
    current_cursor = end_time.strftime("%Y-%m-%dT%H:%M:%S")

    while True:
        try:
            # Request parameters
            params = {
                "market": market,
                "to": current_cursor,
                "count": 200  # Max candles per request
            }
            
            headers = {"accept": "application/json"}
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            if not data:
                print("No more data returned.")
                break

            all_candles.extend(data)

            # Get the timestamp of the last candle in this batch
            last_candle_time_str = data[-1]['candle_date_time_utc']
            last_candle_time = datetime.strptime(last_candle_time_str, "%Y-%m-%dT%H:%M:%S")

            print(f"Fetched {len(data)} candles... cursor now at {last_candle_time}")

            # Stop if we've gone back past our start time
            if last_candle_time < start_time:
                break

            # Update cursor for next batch (Upbit returns data *before* the 'to' date)
            current_cursor = last_candle_time_str
            
            # Rate limiting (Upbit allows ~10 req/sec, but let's be safe)
            time.sleep(0.1)

        except Exception as e:
            print(f"Error fetching data: {e}")
            break

    # Process Data
    if all_candles:
        df = pd.DataFrame(all_candles)
        
        # Select and rename relevant columns
        df = df[[
            'candle_date_time_kst', 
            'opening_price', 
            'high_price', 
            'low_price', 
            'trade_price', 
            'candle_acc_trade_volume'
        ]]
        df.columns = ['Time (KST)', 'Open', 'High', 'Low', 'Close', 'Volume']

        # Convert Time to datetime objects and sort
        df['Time (KST)'] = pd.to_datetime(df['Time (KST)'])
        df = df.sort_values('Time (KST)').reset_index(drop=True)

        # Filter exactly for the last 24 hours (cleanup extra fetched data)
        cutoff_time = datetime.now() - timedelta(hours=24)
        # Note: 'Time (KST)' is naive, so we compare with naive local time or adjust logic if strict UTC needed
        # For simplicity, we just save all fetched data which covers >24h slightly.
        
        filename = "usdt_krw_history.csv"
        df.to_csv(filename, index=False)
        print(f"\nSuccess! Saved {len(df)} rows to '{filename}'.")
        print(df.head())
        print(df.tail())
    else:
        print("Failed to fetch any data.")

if __name__ == "__main__":
    fetch_usdt_krw_history()