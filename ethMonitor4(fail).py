import csv
import logging
import requests
import time
from datetime import datetime, timedelta

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = "https://api.binance.us/api/v3/klines"
SYMBOL = "ETHUSDT"
INTERVAL = "1m"
CSV_HEADER = ['Kline Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']

def fetch_klines(symbol, interval, start_time, end_time=None, limit=1000):
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': int(start_time.timestamp() * 1000),
        'limit': limit
    }
    if end_time:
        params['endTime'] = int(end_time.timestamp() * 1000)
    
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()

def insert_missing_data(csv_filename, missing_data):
    # Load existing data
    with open(csv_filename, mode='r', newline='') as file:
        reader = csv.reader(file)
        existing_data = list(reader)

    # Insert missing data in correct place based on timestamp
    for missing_row in missing_data:
        for i, row in enumerate(existing_data):
            if datetime.strptime(missing_row[0], '%Y-%m-%d %H:%M:%S') < datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'):
                existing_data.insert(i, missing_row)
                break
        else:
            existing_data.append(missing_row)

    # Rewrite CSV with missing data inserted
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(existing_data)

def main():
    csv_filename = f'{datetime.now().strftime("%Y-%m-%d")}_klineData.csv'
    last_fetch_time = None

    while True:
        now = datetime.utcnow()
        if last_fetch_time is None or now - last_fetch_time >= timedelta(minutes=1):
            try:
                # Fetch latest kline
                klines = fetch_klines(SYMBOL, INTERVAL, last_fetch_time if last_fetch_time else now - timedelta(minutes=1))
                for kline in klines:
                    kline_data = [datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')] + [str(k) for k in kline[1:6]]
                    logging.info(f"Fetched kline data: {kline_data}")
                    # Log to CSV
                    with open(csv_filename, 'a', newline='') as file:
                        writer = csv.writer(file)
                        if file.tell() == 0:
                            writer.writerow(CSV_HEADER)
                        writer.writerow(kline_data)

                last_fetch_time = now
            except Exception as e:
                logging.error(f"Failed to fetch kline data: {e}")
                # Handle missing data
                if last_fetch_time:
                    missing_data_start = last_fetch_time + timedelta(minutes=1)
                    missing_data_end = now
                    missing_klines = fetch_klines(SYMBOL, INTERVAL, missing_data_start, missing_data_end)
                    missing_data = [[datetime.fromtimestamp(kline[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')] + [str(k) for k in kline[1:6]] for kline in missing_klines]
                    insert_missing_data(csv_filename, missing_data)

            time.sleep(60 - (datetime.utcnow().second % 60))

if __name__ == "__main__":
    main()
