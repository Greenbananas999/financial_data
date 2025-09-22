import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()
APIKEY = os.getenv("APIKEY")

def save_json_to_file(json_data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)
    print(f"JSON data saved to {filename}")

def stock_request():
    payload = {
        "country": "United States",
        "currency": "USD",
        "type": "Common Stock"
    }
    resp = requests.request(
        url=f"https://api.twelvedata.com/stocks?apikey={APIKEY}",
        method="GET",
        params=payload
    )
    print(resp)
    data = resp.json()
    save_json_to_file(data, "./data/stocks_data.json")
    return data

def commodities_request():
    resp = requests.request(
        url=f"https://api.twelvedata.com/commodities?apikey={APIKEY}",
        method="GET"
    )
    data = resp.json()
    save_json_to_file(data, "./data/commodities_data.json")
    return data

def statistics_request(symbol="AAPL"):
    payload = {
        "symbol": f"{symbol}"
    }
    resp = requests.request(
        url=f"https://api.twelvedata.com/statistics?apikey={APIKEY}",
        method="GET",
        params=payload
    )
    data = resp.json()
    save_json_to_file(data, "./data/statistics_data.json")
    return data

def income_statement_request(symbol="AAPL"):
    payload = {
        "symbol": f"{symbol}"
    }
    resp = requests.request(
        url=f"https://api.twelvedata.com/income_statement/consolidated?apikey={APIKEY}",
        method="GET",
        params=payload
    )
    data = resp.json()
    print(data)
    save_json_to_file(data, "./data/income_statement_data.json")
    return data