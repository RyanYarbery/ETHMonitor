import csv
from datetime import date, datetime
import requests
import threading
import time

def timestamp_to_readable(ts):
    """Converts milliseconds timestamp to a human-readable format."""
    return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')

def get_eth_live_price():
    try:
        url = "https://api.binance.us/api/v3/ticker/price?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        return float(response.json()['price'])
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
        return [timestamp_to_readable(int(data[0])), float(data[4])]  # Returning [Open Time, Close Price]
    except Exception as e:
        print(f"Error fetching kline data: {e}")
        return [None, None]

def main():
    filename = f'{date.today().isoformat()}_liveAndKlineData.csv'

    # Check if the file exists and has content
    try:
        with open(filename, 'r') as f:
            first_char = f.read(1)
            file_exists = True if first_char else False
    except FileNotFoundError:
        file_exists = False

    while True:
        live_price_thread = threading.Thread(target=get_eth_live_price)
        kline_thread = threading.Thread(target=get_eth_klines)

        live_price_thread.start()
        kline_thread.start()

        live_price_thread.join()
        kline_thread.join()

        live_price = get_eth_live_price()
        kline_open_time, kline_close_price = get_eth_klines()

        if live_price and kline_close_price:
            print(f"Live Price: {live_price}, Kline Close Price: {kline_close_price}, Kline Open Time: {kline_open_time}")
            print("Price Comparison:", "Same" if live_price == kline_close_price else "Different")

            # Writing data to CSV
            with open(filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write header if file is new or empty
                if not file_exists:
                    csv_writer.writerow(['Timestamp', 'Live Price', 'Kline Open Time', 'Kline Close Price'])
                    file_exists = True

                # Write data
                csv_writer.writerow([datetime.utcnow().isoformat(), live_price, kline_open_time, kline_close_price])
        else:
            print("Failed to retrieve data.")

        time.sleep(60)

if __name__ == "__main__":
    main()
