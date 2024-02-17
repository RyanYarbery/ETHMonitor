import os
import csv
import logging
import requests
import time
from datetime import datetime, timezone, timedelta

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = "https://api.binance.us/api/v3/klines"
SYMBOL = "ETHUSDT"
INTERVAL = "1m"
HEADER = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']
DIRECTORY = "CSVs"

# Ensure the directory exists
if not os.path.exists(DIRECTORY):
    os.makedirs(DIRECTORY)

def get_filename():
    return f'{DIRECTORY}/{datetime.now(timezone.utc).strftime("%Y-%m")}_ETHKlines.csv'

def fetch_klines():
    params = {
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'limit': 2  # Fetch the last two candlesticks
    }
    
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to fetch klines: {response.text}")
        return []

def log_to_csv(filename, data):
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            writer.writerow(HEADER)
        writer.writerow(data)

def main():
    filename = get_filename()
    logging.info("Starting to fetch and log klines...")

    while True:
        klines = fetch_klines()
        if klines and len(klines) > 1:
            # Process and log only the second-last candlestick
            oldest_kline = klines[0]
            data_to_log = [datetime.utcfromtimestamp(oldest_kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')] + oldest_kline[1:6]
            log_to_csv(filename, data_to_log)
            logging.info(f"Logged kline: {data_to_log}")
        
        # Sleep until the next cycle
        time.sleep(60)

if __name__ == "__main__":
    main()
