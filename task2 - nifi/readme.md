Отчет по заданию 2: Интеграция Kafka с Apache NiFi
1. Архитектура решения

flowchart LR
    A[📁 CSV-файл] --> B(NiFi GetFile)
    B --> C(NiFi PublishKafkaRecord_2_0)
    C --> D[☁️ Kafka Topic: nifi-data]
    D --> E[🐍 Python Consumer]



Описание потока данных:

GetFile читает CSV-файл из папки /opt/nifi/nifi-current/data/

PublishKafkaRecord_2_0 преобразует CSV в JSON и отправляет в Kafka

Python Consumer читает данные из топика nifi-data и выводит в консоль

2. Развертывание сервисов
2.1. Скриншот запущенных сервисов
Команда: docker-compose ps
Mac:nifi svetlanaolefirenko$ docker-compose ps
NAME        IMAGE                            COMMAND                  SERVICE     CREATED         STATUS         PORTS
kafbat-ui   ghcr.io/kafbat/kafka-ui:latest   "/bin/sh -c 'java --…"   kafbat-ui   6 minutes ago   Up 6 minutes   0.0.0.0:8082->8080/tcp, [::]:8082->8080/tcp
kafka1      bitnamilegacy/kafka:3.7          "/opt/bitnami/script…"   kafka1      6 minutes ago   Up 6 minutes   0.0.0.0:9092-9093->9092-9093/tcp, [::]:9092-9093->9092-9093/tcp
kafka2      bitnamilegacy/kafka:3.7          "/opt/bitnami/script…"   kafka2      6 minutes ago   Up 6 minutes   0.0.0.0:9094->9092/tcp, [::]:9094->9092/tcp, 0.0.0.0:9095->9093/tcp, [::]:9095->9093/tcp
kafka3      bitnamilegacy/kafka:3.7          "/opt/bitnami/script…"   kafka3      6 minutes ago   Up 6 minutes   0.0.0.0:9096->9092/tcp, [::]:9096->9092/tcp, 0.0.0.0:9097->9093/tcp, [::]:9097->9093/tcp
nifi        apache/nifi:1.21.0               "../scripts/start.sh"    nifi        6 minutes ago   Up 6 minutes   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
Mac:nifi svetlanaolefirenko$ 


 скрин в файле docker_ps.png

 3. Настройка Apache NiFi
3.1. Пайплайн в NiFi
В веб-интерфейсе NiFi (http://localhost:8080/nifi) создан пайплайн:

text
GetFile → PublishKafkaRecord_2_0

3.2. Настройки GetFile
Параметр	Значение
Input Directory	/opt/nifi/nifi-current/data
File Filter	.*\.csv
Polling Interval	5 sec
Keep Source File	false

3.3. Настройки PublishKafkaRecord_2_0
Параметр	Значение
Kafka Brokers	kafka1:9092,kafka2:9092,kafka3:9092
Topic Name	nifi-data
Use Transactions	false
Record Reader	CSVReader
Record Writer	JsonRecordSetWriter
Publish Strategy	Use Content as Record Value
Delivery Guarantee	Guarantee Replicated Delivery
Security Protocol	PLAINTEXT
SASL Mechanism	PLAINTEXT


3.4. Настройки CSVReader
Параметр	Значение
Schema Access Strategy	Infer Schema
Treat First Line as Header	true
Value Separator	,
3.5. Настройки JsonRecordSetWriter
Параметр	Значение
Schema Access Strategy	Infer Schema
Schema Write Strategy	Do Not Write Schema
3.6. Настройка отношений (Settings)
Отношение	Действие
success	Auto-Terminate
failure	Auto-Terminate



4. Тестовые данные
Файл nifi_data/input.csv:

csv
id,name,age,email
1,Екатерина,21,kate@yandex.ru
2,Никита,26,nikita@yandex.ru
3,Майя,21,maya@yandex.ru
4,Алексей,26,alex@yandex.ru
5,Илья,25,ilya@yandex.ru
6,Виктория,22,vika@yandex.ru

5. Код продюсера и консьюмера
5.1. Продюсер (Python)
Примечание: Роль продюсера выполняет NiFi, который читает CSV и отправляет данные в Kafka.

5.2. Консьюмер (Python) — consumer.py
python
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
    print("Нажмите Ctrl+C для остановки")
    
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
5.3. Продюсер (Python) — producer.py (для тестовой отправки)
python
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
6. Логи успешной передачи данных
6.1. Kafka-топик с поступающими данными
Команда:

bash
docker exec -it kafka1 kafka-console-consumer.sh \
  --topic nifi-data \
  --from-beginning \
  --bootstrap-server kafka1:9092
Mac:nifi svetlanaolefirenko$ docker exec -it kafka1 kafka-console-consumer.sh \
>   --topic nifi-data \
>   --from-beginning \
>   --bootstrap-server kafka1:9092
вывод 
{"id":1,"name":"Екатерина","age":21,"email":"kate@yandex.ru"}
{"id":2,"name":"Никита","age":26,"email":"nikita@yandex.ru"}
{"id":3,"name":"Майя","age":21,"email":"maya@yandex.ru"}
{"id":4,"name":"Алексей","age":26,"email":"alex@yandex.ru"}
{"id":5,"name":"Илья","age":25,"email":"ilya@yandex.ru"}
{"id":6,"name":"Виктория","age":22,"email":"vika@yandex.ru"}
{"id": "7", "name": "Test", "age": "30", "email": "test@example.com"}

скрин в файле вывод команды.png

6.2. Логи успешной работы NiFi
Команда:

bash
docker-compose logs nifi | grep -i "success"
Результат:

text
2ifi  | 2026-06-19 17:41:35,898 INFO [Cleanup Archive for default] o.a.n.c.repository.FileSystemRepository Successfully deleted 0 files (0 bytes) from archive
nifi  | 2026-06-19 17:41:36,890 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:41:56,891 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:42:16,893 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:42:24,601 INFO [Timer-Driven Process Thread-3] o.a.n.c.s.StandardControllerServiceNode Successfully enabled StandardControllerServiceNode[service=CSVReader[id=e0f0a36b-019e-1000-b020-1ad5b05b943b], name=CSVReader, active=true]
nifi  | 2026-06-19 17:42:33,522 INFO [Timer-Driven Process Thread-4] o.a.n.c.s.StandardControllerServiceNode Successfully enabled StandardControllerServiceNode[service=JsonRecordSetWriter[id=e0f0cb91-019e-1000-9a3d-8ff3fa66b5b9], name=JsonRecordSetWriter, active=true]
nifi  | 2026-06-19 17:42:35,910 INFO [Cleanup Archive for default] o.a.n.c.repository.FileSystemRepository Successfully deleted 0 files (0 bytes) from archive
nifi  | 2026-06-19 17:42:36,895 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:42:56,899 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:43:16,903 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:43:35,919 INFO [Cleanup Archive for default] o.a.n.c.repository.FileSystemRepository Successfully deleted 0 files (0 bytes) from archive
nifi  | 2026-06-19 17:43:36,904 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:43:56,910 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepository Successfully checkpointed FlowFile Repository with 0 records in 0 milliseconds
nifi  | 2026-06-19 17:44:16,912 INFO [pool-7-thread-1] o.a.n.c.r.WriteAheadFlowFileRepo

Скриншот логов NiFi:   logs_nifi.png


6.3. Подтверждение записи данных в Kafka
Вывод Python-консьюмера:

text
Mac:yapracticum svetlanaolefirenko$  source /Users/svetlanaolefirenko/yapracticum/practicum.venv/bin/activate
(practicum.venv) Mac:yapracticum svetlanaolefirenko$ /Users/svetlanaolefirenko/yapracticum/practicum.venv/bin/python /Users/svetlanaolefirenko/yapracticum/nifi/consumer.py
Ожидание сообщений из топика nifi-data...
Получено сообщение: {"id":1,"name":"Екатерина","age":21,"email":"kate@yandex.ru"}, partition=0, offset=0
Получено сообщение: {"id":2,"name":"Никита","age":26,"email":"nikita@yandex.ru"}, partition=0, offset=1
Получено сообщение: {"id":3,"name":"Майя","age":21,"email":"maya@yandex.ru"}, partition=0, offset=2
Получено сообщение: {"id":4,"name":"Алексей","age":26,"email":"alex@yandex.ru"}, partition=0, offset=3
Получено сообщение: {"id":5,"name":"Илья","age":25,"email":"ilya@yandex.ru"}, partition=0, offset=4
Получено сообщение: {"id":6,"name":"Виктория","age":22,"email":"vika@yandex.ru"}, partition=0, offset=5
Получено сообщение: {"id": "7", "name": "Test", "age": "30", "email": "test@example.com"}, partition=0, offset=7

Скриншот Python-консьюмера: consumer.png

6.4. Скриншот пайплайна в NiFi. nifi.png

скрины настроек NIFI
nifi_flow.png 


7. Интерфейсы сервисов
Сервис	URL	Назначение
NiFi	http://localhost:8080/nifi	Визуальный пайплайн обработки данных
Kafbat UI	http://localhost:8082	Мониторинг кластера Kafka