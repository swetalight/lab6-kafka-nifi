from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': 'localhost:9093',
    'client.id': 'python-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Отправка тестового сообщения
data = {"id": "7", "name": "Test", "age": "30", "email": "test@example.com"}
producer.produce('nifi-data', key='key1', value=json.dumps(data), callback=delivery_report)
producer.flush()
print("Сообщение отправлено!")