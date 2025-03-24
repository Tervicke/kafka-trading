#gets the data from the binance api and puts it in the partion 0 
from confluent_kafka import Producer
import json
import websocket

config = {
        "bootstrap.servers": "localhost:9092",
}

producer = Producer(config)

def aftersend(err,send):
    if not err == None:
        print(f"error occured {err}")
        return
    print("Message sent")

topic="usdt-sol-prices"

#producer.produce(topic, json_data ,callback=aftersend)

last_updated_price = None; #string

def on_message(ws, message):
    global last_updated_price
    data = json.loads(message)
    price = data.get("p")  # Price is in string format
    try:
        if(price != last_updated_price):
            print("Price changed!")
            last_updated_price = price
            producer.produce(topic , key=b"122-usdt-sol-prices-binance" , value=last_updated_price , callback=aftersend)
            producer.flush()
    except:
        print(price)
        print("error converting value")

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": ["solusdt@trade"],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))


if __name__ == "__main__":
    ws_url = "wss://stream.binance.com:9443/ws/solusdt@trade"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close,partition=0) 
    ws.on_open = on_open
    ws.run_forever()
