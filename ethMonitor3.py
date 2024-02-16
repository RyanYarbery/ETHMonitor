import csv
import logging
import requests
import threading
import time
from datetime import date, datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def timestamp_to_readable(ts):
    """Converts milliseconds timestamp to a human-readable format."""
    return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')

def api_request(url, params=None, retries=3):
    """General API request function with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.warning(f"HTTP error: {e}, attempt {attempt + 1} of {retries}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request exception: {e}, attempt {attempt + 1} of {retries}")
        time.sleep(2 ** attempt)  # Exponential backoff
    logging.error(f"Failed to get data after {retries} attempts")
    return None

def get_eth_klines():
    url = "https://api.binance.us/api/v3/klines"
    params = {'symbol': 'ETHUSDT', 'interval': '1m', 'limit': 1}
    data = api_request(url, params)
    if data and len(data) > 0:
        kline = data[0]
        return [timestamp_to_readable(int(kline[0])), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])]
    return [None, None, None, None, None, None]

def log_to_csv(filename, header, data):
    with open(filename, mode='a', newline='') as file:
        csv_writer = csv.writer(file)
        if file.tell() == 0:
            csv_writer.writerow(header)
        csv_writer.writerow(data)

def kline_data_fetching():
    next_call = time.time()
    while True:
        kline_data = get_eth_klines()
        if kline_data[0]:
            logging.info(f"Kline Data: {kline_data}")
            log_to_csv(f'{date.today().isoformat()}_klineData.csv', ['Kline Open Time', 'Open', 'High', 'Low', 'Close', 'Volume'], kline_data)
        next_call += 60
        time.sleep(max(0, next_call - time.time()))

if __name__ == "__main__":
    kline_data_fetching()
