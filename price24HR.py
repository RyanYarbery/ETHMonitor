import csv
from datetime import date, datetime
import requests
import time

def get_eth_data():
    try:
        url = "https://api.binance.us/api/v3/ticker/24hr?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['lastPrice'], data['volume']
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def main():
    filename = f'{date.today().isoformat()}_price24HRWeighted.csv'

    # Check if the file exists and has content
    try:
        with open(filename, 'r') as f:
            first_char = f.read(1)
            file_exists = True if first_char else False
    except FileNotFoundError:
        file_exists = False

    while True:
        data = get_eth_data()
        if data:
            price, volume = data
            print(f"Current Ethereum Price: {price} USD, Volume: {volume}")

            # Writing data to CSV
            with open(filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write header if file is new or empty
                if not file_exists:
                    csv_writer.writerow(['Timestamp', 'Ethereum Price (USD)', 'Volume'])
                    file_exists = True

                # Write data
                csv_writer.writerow([datetime.utcnow().isoformat(), price, volume])
        else:
            print("Failed to retrieve Ethereum data.")
        
        time.sleep(10)

if __name__ == "__main__":
    main()
