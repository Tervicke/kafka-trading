from confluent_kafka import Consumer, KafkaException

conf  = {
        'bootstrap.servers':'localhost:9092',
        'group.id':"price consumer",
}

consumer = Consumer(conf);
consumer.subscribe(["usdt-sol-prices"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error : {msg.error()}")
            continue;
        price_partition = msg.partition()
        price = float(msg.value().decode('utf-8'))
        print(f"{price_partition} => {price}")
except KeyboardInterrupt:
    print("Stopped")
finally:
    consumer.close()
