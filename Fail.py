import requests

def get_ethereum_price():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT"
    # url = "https://api.binance.com/api/v3/ping"
    response = requests.get(url)
    if response.status_code == 200:
        price_data = response.json()
        return price_data['price']
    else:
        return "Error fetching price data"

# Example usage
eth_price = get_ethereum_price()
print(f"Ethereum Price: {eth_price}")
