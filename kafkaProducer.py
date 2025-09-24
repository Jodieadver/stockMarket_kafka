import asyncio
from datetime import datetime
import json
import time
from kafka import KafkaProducer
import requests
from dotenv import load_dotenv
import os


# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producer started. Sending stock updates...")

# API endpoints
load_dotenv()  
api_key = os.getenv('FMP_API_KEY')
symbol = "^GSPC"

# Periodically fetch and publish data to Kafka
while True:
    response = requests.get(f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={api_key}")
    if response.status_code == 200:
        stock_data = response.json()
        if stock_data:
            stock_info = stock_data[0]
            message = {
                "symbol": stock_info["symbol"],
                "name": stock_info["name"],
                "price": stock_info["price"],
                "changePercentage": stock_info["changePercentage"],
                "change": stock_info["change"],
                "volume": stock_info["volume"],
                "dayLow": stock_info["dayLow"],
                "dayHigh": stock_info["dayHigh"],
                "yearHigh": stock_info["yearHigh"],
                "yearLow": stock_info["yearLow"],
                "marketCap": stock_info["marketCap"],
                "priceAvg50": stock_info["priceAvg50"],
                "priceAvg200": stock_info["priceAvg200"],
                "exchange": stock_info["exchange"],
                "open": stock_info["open"],
                "previousClose": stock_info["previousClose"],
                "timestamp": datetime.now().isoformat()
            }
            producer.send("GSPC", json.dumps(message))
            producer.flush()
            print(f"Sent: {message}")
            time.sleep(1)
    else:
        print(f"Failed to fetch data from API 1 (Status Code: {response.status_code})")
    

