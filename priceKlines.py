import csv
from datetime import date, datetime
import requests
import time

def timestamp_to_readable(ts):
    """Converts milliseconds timestamp to a human-readable format."""
    return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')

def get_eth_klines():
    try:
        url = "https://api.binance.us/api/v3/klines"
        params = {
            'symbol': 'ETHUSDT',
            'interval': '1m', 
            'limit': 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data[0]
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    filename = f'{date.today().isoformat()}_priceKlinesWeighted.csv'

    # Check if the file exists and has content
    try:
        with open(filename, 'r') as f:
            first_char = f.read(1)
            file_exists = True if first_char else False
    except FileNotFoundError:
        file_exists = False

    while True:
        kline_data = get_eth_klines()
        if kline_data:
            open_time = timestamp_to_readable(int(kline_data[0]))
            close_time = timestamp_to_readable(int(kline_data[6]))
            print(f"Request Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Kline Data:\nOpen Time: {open_time}, Close Time: {close_time}, Open: {kline_data[1]}, High: {kline_data[2]}, Low: {kline_data[3]}, Close: {kline_data[4]}, Volume: {kline_data[5]}")

            # Writing data to CSV
            with open(filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write header if file is new or empty
                if not file_exists:
                    csv_writer.writerow(['Request Time', 'Open Time', 'Close Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    file_exists = True

                # Write data
                csv_writer.writerow([datetime.utcnow().isoformat(), open_time, close_time, kline_data[1], kline_data[2], kline_data[3], kline_data[4], kline_data[5]])
        else:
            print("Failed to retrieve Ethereum kline data.")
        
        time.sleep(60)

if __name__ == "__main__":
    main()
