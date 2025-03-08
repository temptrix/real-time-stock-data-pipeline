from kafka import KafkaProducer
import yfinance as yf
import json
import time

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "stock_prices"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stock Symbols to Track
stocks = ["AAPL", "GOOGL", "MSFT", "TSLA"]

def fetch_stock_data():
    """Fetches real-time stock data from Yahoo Finance."""
    stock_data = {}
    for stock in stocks:
        ticker = yf.Ticker(stock)
        data = ticker.history(period="1d")  # Fetch today's stock prices
        if not data.empty:
            latest = data.iloc[-1]  # Get the latest price
            stock_data[stock] = {
                "symbol": stock,
                "timestamp": str(latest.name),
                "open": latest["Open"],
                "high": latest["High"],
                "low": latest["Low"],
                "close": latest["Close"],
                "volume": int(latest["Volume"]),
            }
    return stock_data

def produce_messages():
    """Continuously fetch and send stock data to Kafka."""
    while True:
        stock_prices = fetch_stock_data()
        if stock_prices:
            for symbol, data in stock_prices.items():
                producer.send(TOPIC, value=data)
                print(f"Sent data to Kafka: {data}")
        time.sleep(10)  # Fetch data every 10 seconds

if __name__ == "__main__":
    print("Starting Kafka Producer for Stock Market Data...")
    produce_messages()
 
