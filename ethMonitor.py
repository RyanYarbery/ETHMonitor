import csv
import requests
import threading
import time
from datetime import date, datetime, timedelta

def timestamp_to_readable(ts):
    """Converts milliseconds timestamp to a human-readable format."""
    return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')

def get_eth_live_price():
    try:
        url = "https://api.binance.us/api/v3/ticker/price?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return float(data['price'])
    except Exception as e:
        print(f"Error fetching live price: {e}")
        return None

def get_eth_klines():
    try:
        url = "https://api.binance.us/api/v3/klines"
        params = {'symbol': 'ETHUSDT', 'interval': '1m', 'limit': 1}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()[0]
        # print("Raw kline data: ", data)
        return [timestamp_to_readable(int(data[0])), float(data[1]), float(data[2]), float(data[3]), float(data[4]), float(data[5])]
    except Exception as e:
        print(f"Error fetching kline data: {e}")
        return [None, None, None, None, None, None]

def log_to_csv(filename, header, data):
    with open(filename, mode='a', newline='') as file:
        csv_writer = csv.writer(file)
        if file.tell() == 0:
            csv_writer.writerow(header)
        csv_writer.writerow(data)

def kline_thread_func():
    next_call = time.time()
    while True:
        kline_data = get_eth_klines()
        if kline_data[0]:
            print(f"Kline Data: Open Time: {kline_data[0]}, Open: {kline_data[1]}, High: {kline_data[2]}, Low: {kline_data[3]}, Close: {kline_data[4]}, Volume: {kline_data[5]}")
            log_to_csv(f'{date.today().isoformat()}_klineData.csv', ['Kline Open Time', 'Open', 'High', 'Low', 'Close', 'Volume'], kline_data)
        next_call = next_call + 60
        time.sleep(max(0, next_call - time.time()))

def main():
    kline_thread = threading.Thread(target=kline_thread_func)
    kline_thread.start()

    next_call = time.time()
    while True:
        live_price = get_eth_live_price()
        if live_price is not None:
            print(f"Live Price: {live_price}")
            log_to_csv(f'{date.today().isoformat()}_livePriceData.csv', ['Timestamp', 'Live Price'], [datetime.utcnow().isoformat(), live_price])
        next_call = next_call + 10
        time.sleep(max(0, next_call - time.time()))

if __name__ == "__main__":
    main()
