#gets the partion from the coinbase api and puts it in the partion 1
from confluent_kafka import Producer
import json
import websocket

# Kafka producer configuration
config = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(config)

def aftersend(err, msg):
    if err:
        print(f"Error occurred: {err}")
    else:
        print("Message sent successfully")

# Kafka topic
topic = "usdt-sol-prices"

last_updated_price = None  # Store the last price to detect changes

def on_message(ws, message):
    global last_updated_price
    data = json.loads(message)

    if "price" in data:
        price = data["price"]  # Price is a string

        try:
            if price != last_updated_price:
                print("Price changed:", price)
                last_updated_price = price
                print(last_updated_price);
                producer.produce(topic, key=b"coinbase-usdt-prices-123", value=price, callback=aftersend,partition=1)
                producer.flush()
        except Exception as e:
            print("Error processing price:", price, "\nException:", e)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    # Coinbase WebSocket subscription message
    subscribe_message = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["SOL-USDT"]}]
    }
    ws.send(json.dumps(subscribe_message))

if __name__ == "__main__":
    ws_url = "wss://ws-feed.exchange.coinbase.com"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
