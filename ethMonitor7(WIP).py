import csv
import logging
import requests
import threading
import time
from datetime import datetime, timedelta

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = "https://api.binance.us/api/v3/klines"
SYMBOL = "ETHUSDT"
INTERVAL = "1m"
CSV_HEADER = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']
FILENAME = f'{datetime.utcnow().strftime("%Y-%m-%d")}_ETHKlines.csv'

def fetch_klines(start_time, end_time=None):
    """Fetch klines for a specific period."""
    params = {
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'startTime': int(start_time.timestamp() * 1000),
        'limit': 1000  # Max limit
    }
    if end_time:
        params['endTime'] = int(end_time.timestamp() * 1000)
    
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to fetch klines: {response.text}")
        return []

def insert_missing_data(missing_data):
    """Insert missing data into CSV in the correct chronological order."""
    try:
        with open(FILENAME, 'r', newline='') as file:
            existing_data = list(csv.reader(file))
    except FileNotFoundError:
        existing_data = [CSV_HEADER]

    with open(FILENAME, 'w', newline='') as file:
        writer = csv.writer(file)
        for row in sorted(existing_data[1:] + missing_data, key=lambda x: x[0]):
            writer.writerow(row)

def kline_data_fetching():
    last_fetch_time = None
    while True:
        now = datetime.utcnow()
        if last_fetch_time and now - last_fetch_time >= timedelta(minutes=1):
            start_time = last_fetch_time + timedelta(minutes=1)
            missing_data = []
            while start_time < now:
                for kline in fetch_klines(start_time, start_time + timedelta(minutes=1)):
                    missing_data.append([datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')] + kline[1:6])
                start_time += timedelta(minutes=1)
            if missing_data:
                insert_missing_data(missing_data)
        
        klines = fetch_klines(now - timedelta(minutes=1), now)
        if klines:
            last_fetch_time = now
            for kline in klines:
                logging.info(f"Fetched kline data: {kline}")
                with open(FILENAME, 'a', newline='') as file:
                    writer = csv.writer(file)
                    if file.tell() == 0:
                        writer.writerow(CSV_HEADER)
                    writer.writerow([datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')] + kline[1:6])
        
        time.sleep(60 - (datetime.utcnow().second % 60))

if __name__ == "__main__":
    kline_data_fetching()
