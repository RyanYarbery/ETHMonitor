import csv
import requests
import threading
import time
from datetime import date, datetime

def timestamp_to_readable(ts):
    """Converts milliseconds timestamp to a human-readable format."""
    return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')

def get_eth_24hr_data():
    try:
        url = "https://api.binance.us/api/v3/ticker/24hr?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return float(data['lastPrice']), float(data['volume'])
    except Exception as e:
        print(f"Error fetching 24hr data: {e}")
        return None, None

def get_eth_klines():
    try:
        url = "https://api.binance.us/api/v3/klines"
        params = {'symbol': 'ETHUSDT', 'interval': '1m', 'limit': 1}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()[0]
        return [timestamp_to_readable(int(data[0])), float(data[1]), float(data[2]), float(data[3]), float(data[4]), float(data[5])]
    except Exception as e:
        print(f"Error fetching kline data: {e}")
        return [None, None, None, None, None, None]

def log_to_csv(filename, header, data):
    with open(filename, mode='a', newline='') as file:
        csv_writer = csv.writer(file)
        # Check if file is empty
        if file.tell() == 0:
            csv_writer.writerow(header)
        csv_writer.writerow(data)

def kline_thread_func():
    while True:
        kline_data = get_eth_klines()
        if kline_data[0]:
            print(f"Kline Data: Open Time: {kline_data[0]}, Open: {kline_data[1]}, High: {kline_data[2]}, Low: {kline_data[3]}, Close: {kline_data[4]}, Volume: {kline_data[5]}")
            log_to_csv(f'{date.today().isoformat()}_klineData.csv', ['Kline Open Time', 'Open', 'High', 'Low', 'Close', 'Volume'], kline_data)
        time.sleep(60)  # Query every minute

def main():
    last_volume = None
    kline_thread = threading.Thread(target=kline_thread_func)
    kline_thread.start()

    while True:
        price_24hr, volume_24hr = get_eth_24hr_data()
        if price_24hr is not None and volume_24hr is not None:
            volume_diff = volume_24hr - last_volume if last_volume is not None else 0
            print(f"24hr Data: Price: {price_24hr}, Volume: {volume_24hr}, 10s Volume Diff: {volume_diff}")
            log_to_csv(f'{date.today().isoformat()}_24hrData.csv', ['Timestamp', 'Price', 'Volume', '10s Volume Diff'], [datetime.utcnow().isoformat(), price_24hr, volume_24hr, volume_diff])
            last_volume = volume_24hr
        time.sleep(10)  # Query every 10 seconds

if __name__ == "__main__":
    main()
