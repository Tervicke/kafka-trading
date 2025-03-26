from confluent_kafka import Consumer, KafkaException
import math
conf  = {
        'bootstrap.servers':'localhost:9092',
        'group.id':"price consumer",
}
def calculate_spread(binance_price , coinbase_price ):
    if(binance_price == -1 or coinbase_price == -1):
        return
    print("------------------------------------")
    print(f"Binance Price {binance_price}")
    print(f"Coinbase Price {coinbase_price}")
    spread = abs(coinbase_price - binance_price)
    print(f"Spread {spread}")
    spread_percentage = (spread/max(coinbase_price , binance_price)) * 100
    print(f"spread % {spread_percentage:.4f}%")
    print("------------------------------------")

consumer = Consumer(conf);
consumer.subscribe(["usdt-sol-prices"])
last_binance_price = -1
last_coinbase_price = -1

try:
    while True:
        msg = consumer.poll(timeout=0)
        if msg is None or msg.value is None:
            continue
        if msg.error():
            print(f"consumer error : {msg.error()}")
            continue;
        price_partition = msg.partition()
        price = float(msg.value().decode('utf-8'))
        if(price_partition == 0):
            last_coinbase_price = price
        else:
            last_binance_price = price
        calculate_spread(last_binance_price , last_coinbase_price)

except KeyboardInterrupt:
    print("Stopped")
finally:
    consumer.close()
