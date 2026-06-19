#!/usr/bin/python3
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Загружаем схемы
with open('key.avsc', 'r') as f:
    key_schema_str = f.read()
with open('value.avsc', 'r') as f:
    value_schema_str = f.read()

key_schema = avro.loads(key_schema_str)
value_schema = avro.loads(value_schema_str)

# Настройки
config = {
    "bootstrap.servers": "rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net:9091,rc1a-d5vf59eikk7ram0s.mdb.yandexcloud.net:9091,rc1a-f14svcqf7i9md6cr.mdb.yandexcloud.net:9091",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "/Users/svetlanaolefirenko/YandexInternalRootCA.crt",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "app-user",
    "sasl.password": "yandex555",
    "schema.registry.url": "https://rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net:443",
    "schema.registry.ssl.ca.location": "/Users/svetlanaolefirenko/YandexInternalRootCA.crt",
    "schema.registry.basic.auth.credentials.source": "SASL_INHERIT"
}

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [partition {msg.partition()}]")

producer = AvroProducer(config, default_key_schema=key_schema, default_value_schema=value_schema)

# Отправка тестового сообщения
key = {"user_id": "test_user_1"}
value = {"name": "Test User", "timestamp": 1640995200000}

producer.produce(topic="messages", key=key, value=value, on_delivery=delivery_report)
producer.flush()