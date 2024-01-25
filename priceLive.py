import csv
from datetime import date, datetime
import requests
import time

def get_eth_latest_price():
    try:
        url = "https://api.binance.us/api/v3/ticker/price?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        price_data = response.json()
        return price_data['price']
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def main():
    filename = f'{date.today().isoformat()}_priceLiveWeighted.csv'

    # Check if the file exists and has content
    try:
        with open(filename, 'r') as f:
            first_char = f.read(1)
            file_exists = True if first_char else False
    except FileNotFoundError:
        file_exists = False

    while True:
        price = get_eth_latest_price()
        if price:
            print(f"Current Ethereum Price: {price} USD")

            # Writing data to CSV
            with open(filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write header if file is new or empty
                if not file_exists:
                    csv_writer.writerow(['Timestamp', 'Ethereum Price (USD)'])
                    file_exists = True  # Update flag to avoid rewriting header

                # Write data
                csv_writer.writerow([datetime.utcnow().isoformat(), price])
        else:
            print("Failed to retrieve Ethereum price.")
        
        time.sleep(10)

if __name__ == "__main__":
    main()

