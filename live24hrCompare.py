import csv
from datetime import date, datetime
import requests
import threading
import time

def get_eth_live_price():
    try:
        url = "https://api.binance.us/api/v3/ticker/price?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        return float(response.json()['price'])
    except Exception as e:
        print(f"Error fetching live price: {e}")
        return None

def get_eth_24hr_data():
    try:
        url = "https://api.binance.us/api/v3/ticker/24hr?symbol=ETHUSDT"
        response = requests.get(url)
        response.raise_for_status()
        return float(response.json()['lastPrice']), float(response.json()['volume'])
    except Exception as e:
        print(f"Error fetching 24hr data: {e}")
        return None, None

def main():
    filename = f'{date.today().isoformat()}_combinedData.csv'

    # Check if the file exists and has content
    try:
        with open(filename, 'r') as f:
            first_char = f.read(1)
            file_exists = True if first_char else False
    except FileNotFoundError:
        file_exists = False

    while True:
        # Fetch data simultaneously
        live_price_thread = threading.Thread(target=get_eth_live_price)
        data_24hr_thread = threading.Thread(target=get_eth_24hr_data)

        live_price_thread.start()
        data_24hr_thread.start()

        live_price_thread.join()
        data_24hr_thread.join()

        live_price = get_eth_live_price()
        last_price_24hr, volume_24hr = get_eth_24hr_data()

        if live_price and last_price_24hr:
            print(f"Live Price: {live_price}, 24hr Last Price: {last_price_24hr}, Volume: {volume_24hr}")
            print("Price Comparison:", "Same" if live_price == last_price_24hr else "Different")

            # Writing data to CSV
            with open(filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)

                # Write header if file is new or empty
                if not file_exists:
                    csv_writer.writerow(['Timestamp', 'Live Price', '24hr Last Price', 'Volume'])
                    file_exists = True

                # Write data
                csv_writer.writerow([datetime.utcnow().isoformat(), live_price, last_price_24hr, volume_24hr])
        else:
            print("Failed to retrieve data.")

        time.sleep(10)

if __name__ == "__main__":
    main()
