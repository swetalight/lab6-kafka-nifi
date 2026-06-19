#!/usr/bin/python3
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

config = {
    "bootstrap.servers": "rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net:9091,rc1a-d5vf59eikk7ram0s.mdb.yandexcloud.net:9091,rc1a-f14svcqf7i9md6cr.mdb.yandexcloud.net:9091",
    "group.id": "test-consumer-group",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "/Users/svetlanaolefirenko/YandexInternalRootCA.crt",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "app-user",
    "sasl.password": "yandex555",
    "schema.registry.url": "https://rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net:443",
    "schema.registry.ssl.ca.location": "/Users/svetlanaolefirenko/YandexInternalRootCA.crt",
    "schema.registry.basic.auth.credentials.source": "SASL_INHERIT",
    "auto.offset.reset": "earliest"
}

consumer = AvroConsumer(config)
consumer.subscribe(["messages"])

print("Waiting for messages...")
try:
    while True:
        msg = consumer.poll(10)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        print(f"Received key: {msg.key()}, value: {msg.value()}")
        consumer.commit()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()