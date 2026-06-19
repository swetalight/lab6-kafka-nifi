Отчет по заданию 1: Развертывание и настройка Kafka-кластера в Yandex Cloud
1. Краткое описание выполненных шагов
1.1. Развертывание кластера
Создан кластер kafka633 в сервисе Managed Service for Apache Kafka® Yandex Cloud

Выбрана версия Kafka 3.9 с поддержкой Schema Registry

Развернуты 3 брокера с конфигурацией s4a-c2-m8 (2 vCPU, 8 GB RAM) и дисками 32 GB SSD

Включен публичный доступ для подключения из интернета

Активирован Schema Registry на уровне кластера

1.2. Настройка топика
Создан топик messages с параметрами:

3 партиции

Фактор репликации 3

Политика очистки: DELETE

Время хранения: 604800000 мс (7 дней)

Размер сегмента: 1073741824 байт (1 ГБ)

1.3. Настройка Schema Registry
Schema Registry автоматически зарегистрировал схемы при отправке первого сообщения

Зарегистрированы схемы для ключа и значения сообщений

1.4. Проверка работы
Разработаны продюсер и консьюмер на Python с использованием библиотеки confluent_kafka

Отправлено тестовое сообщение и успешно прочитано

Проверена работоспособность Schema Registry через REST API

2. Информация по аппаратным ресурсам
Компонент	Конфигурация	Обоснование
Брокеры (Kafka)	• Количество: 3
• Платформа: Intel Ice Lake
• CPU: 2 vCPU
• RAM: 8 GB
• Диск: 32 GB SSD (network-ssd)	Минимальная продакшн-конфигурация для обеспечения отказоустойчивости (3 брокера) и производительности для тестовой нагрузки
ZooKeeper	• Количество: 3
• CPU: 2 vCPU
• RAM: 8 GB
• Диск: 10 GB SSD	Управление кластером
Schema Registry	Встроенный в Managed Service for Apache Kafka®	Не требует выделенных хостов
3. Скрипты конфигурации
Создание кластера через CLI
bash
yc managed-kafka cluster create kafka633 \
  --environment PRODUCTION \
  --version 3.9 \
  --network-name default \
  --brokers-count 3 \
  --resource-preset s4a-c2-m8 \
  --disk-size 32 \
  --disk-type network-ssd \
  --assign-public-ip \
  --schema-registry
Создание топика
bash
yc managed-kafka topic create messages \
  --cluster-name kafka633 \
  --partitions 3 \
  --replication-factor 3 \
  --cleanup-policy delete \
  --retention-ms 604800000 \
  --segment-bytes 1073741824
Создание пользователя
bash
yc managed-kafka user create app-user \
  --cluster-name kafka633 \
  --password yandex555 \
  --permission topic=messages,role=ACCESS_ROLE_PRODUCER \
  --permission topic=messages,role=ACCESS_ROLE_CONSUMER
4. Описание параметров кластера
Общие параметры
Название кластера: kafka633

ID кластера: c9qffbctjs4hnn3u9h22

Окружение: PRODUCTION

Версия Kafka: 3.9

Статус: RUNNING

Здоровье: ALIVE

Топик messages
Параметр	Значение	Описание
Количество партиций	3	Позволяет распараллелить потребление
Фактор репликации	3	Данные хранятся на всех брокерах для отказоустойчивости
cleanup.policy	DELETE	Старые логи удаляются
retention.ms	604800000 (7 дней)	Сообщения хранятся 7 дней
segment.bytes	1073741824 (1 ГБ)	Размер сегмента лога
Брокеры
FQDN	Роль	Статус	Зона
rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net	KAFKA	ALIVE	ru-central1-a
rc1a-d5vf59eikk7ram0s.mdb.yandexcloud.net	KAFKA	ALIVE	ru-central1-a
rc1a-f14svcqf7i9md6cr.mdb.yandexcloud.net	KAFKA	ALIVE	ru-central1-a
5. Скриншоты ответов Schema Registry
Запрос списка схем
bash
curl -X GET https://rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net:443/subjects \
  --cacert /Users/svetlanaolefirenko/YandexInternalRootCA.crt \
  -u app-user:yandex555
Результат:

json
["messages-value","messages-key"]

скрин в файле shemaRegistry.png

Запрос версий схемы
bash
curl -X GET https://rc1a-bdi8knpott9olpnv.mdb.yandexcloud.net:443/subjects/messages-value/versions \
  --cacert /Users/svetlanaolefirenko/YandexInternalRootCA.crt \
  -u app-user:yandex555
Результат:

json
[1]

скрин в файле версии схем.png

6. Файлы схем (Avro)
Схема ключа (key.avsc)
json
{
    "namespace": "com.example",
    "name": "UserKey",
    "type": "record",
    "fields": [
        {"name": "user_id", "type": "string"}
    ]
}
Схема значения (value.avsc)
json
{
    "namespace": "com.example",
    "name": "UserValue",
    "type": "record",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
    ]
}
7. Вывод команды kafka-topics.sh --describe
bash
yc managed-kafka topic get messages --cluster-name kafka633
Результат:

text
name: messages
cluster_id: c9qffbctjs4hnn3u9h22
partitions: "3"
replication_factor: "3"
topic_config_3:
  cleanup_policy: CLEANUP_POLICY_DELETE
  retention_ms: "604800000"
  segment_bytes: "1073741824"
Детальное описание партиций:

Topic: messages	PartitionCount: 3	ReplicationFactor: 3
	Topic: messages	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: messages	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: messages	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2

скрин в файле  topic.png 

8. Код продюсера и консьюмера
Продюсер (producer.py)
python
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

# Настройки подключения
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
Консьюмер (consumer.py)
python
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
9. Скриншоты, подтверждающие успешную передачу сообщений
Лог продюсера
text
Message delivered to messages [partition 0]
скрин в файле producer_log.png

Лог консьюмера
text
Waiting for messages...
Received key: {'user_id': 'test_user_1'}, value: {'name': 'Test User', 'timestamp': 1640995200000}
скрин в файле consumer_log.png