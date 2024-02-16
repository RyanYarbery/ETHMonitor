import csv
import logging
import requests
import time
from datetime import datetime

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = "https://api.binance.us/api/v3/klines"
SYMBOL = "ETHUSDT"
INTERVAL = "1m"
CSV_HEADER = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']

def fetch_klines():
    """Fetch the latest kline data."""
    params = {
        'symbol': SYMBOL,
        'interval': INTERVAL,
        'limit': 1
    }
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()[0]
        return [datetime.fromtimestamp(data[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')] + data[1:6]
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def log_to_csv(data):
    """Log data to a CSV file."""
    filename = f'{datetime.now().strftime("%Y-%m-%d")}_ETHKlines.csv'
    with open(filename, 'a', newline='') as file:
        writer = csv.writer(file)
        if file.tell() == 0:
            writer.writerow(CSV_HEADER)
        writer.writerow(data)

def main():
    logging.info("Starting ETH Kline data fetch...")
    while True:
        data = fetch_klines()
        if data:
            logging.info(f"Fetched Kline data: {data}")
            log_to_csv(data)
        else:
            logging.info("No data fetched. Trying again...")
        time.sleep(60)  # Fetch every minute

if __name__ == "__main__":
    main()
