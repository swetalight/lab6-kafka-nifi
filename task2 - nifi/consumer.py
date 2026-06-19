from confluent_kafka import Consumer
import json

if __name__ == "__main__":
    consumer_conf = {
        "bootstrap.servers": "localhost:9093",
        "group.id": "nifi-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 6000,
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["nifi-data"])
    
    print("Ожидание сообщений из топика nifi-data...")
    
    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue
            
            value = msg.value().decode("utf-8")
            print(
                f"Получено сообщение: {value}, "
                f"partition={msg.partition()}, offset={msg.offset()}"
            )
    except KeyboardInterrupt:
        print("Остановка консьюмера...")
    finally:
        consumer.close()