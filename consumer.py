import time
import json
import logging
from io import BytesIO
from kafka import KafkaConsumer, KafkaProducer
from fastavro import schemaless_reader, parse_schema
import random

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

order_schema = {
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
        {"name": "orderId", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "float"}
    ]
}
parsed_schema = parse_schema(order_schema)

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
AGG_TOPIC = "orders-aggregates"
GROUP_ID = "orders-consumer-group"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID
)

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

count = 0
total = 0.0

def avro_deserialize(schema, data_bytes):
    bio = BytesIO(data_bytes)
    return schemaless_reader(bio, schema)

def send_dlq(original_bytes, error_info):
    headers = [
        ('error', str(error_info).encode('utf-8'))
    ]
    producer.send(DLQ_TOPIC, original_bytes, headers=headers)
    producer.flush()
    logging.warning("Sent message to DLQ: %s", error_info)

def publish_aggregate(avg_price, count):
    payload = json.dumps({"running_average": avg_price, "count": count}).encode('utf-8')
    producer.send(AGG_TOPIC, payload)
    producer.flush()

def is_temporary_failure(exception):
    return isinstance(exception, Exception) and random.random() < 0.7

MAX_RETRIES = 5
BASE_BACKOFF = 0.5  

for msg in consumer:
    raw = msg.value
    try:
        order = avro_deserialize(parsed_schema, raw)
    except Exception as e:
        logging.error("Failed to deserialize message, sending to DLQ: %s", e)
        send_dlq(raw, f"deserialization error: {e}")
        continue

    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            if random.random() < 0.05:
                raise RuntimeError("simulated permanent processing error")
            if random.random() < 0.12:
                raise ConnectionError("simulated temporary network error")

            count += 1
            total += float(order["price"])
            running_avg = total / count
            logging.info("Processed order %s | price=%.2f | running_avg=%.4f (count=%d)",
                         order["orderId"], order["price"], running_avg, count)

            publish_aggregate(running_avg, count)
            break  

        except Exception as e:
            attempt += 1
            logging.warning("Processing error (attempt %d) for %s: %s", attempt, order["orderId"], e)
            if attempt > MAX_RETRIES or not is_temporary_failure(e):
                logging.error("Permanent failure for %s, sending to DLQ", order["orderId"])
                send_dlq(raw, f"processing error: {e}")
                break
            else:
                backoff = BASE_BACKOFF * (2 ** (attempt - 1))
                logging.info("Temporary failure; retrying after %.2fs", backoff)
                time.sleep(backoff)
