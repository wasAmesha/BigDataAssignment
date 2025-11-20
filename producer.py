import json
import time
import uuid
import random
from io import BytesIO
from kafka import KafkaProducer
from fastavro import schemaless_writer, parse_schema

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
TOPIC = "orders"

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

products = ["Widget", "Gadget", "Thingamajig", "Doohickey", "Gizmo"]

def avro_serialize(schema, record):
    """Return bytes of Avro serialized record (schemaless)."""
    bio = BytesIO()
    schemaless_writer(bio, schema, record)
    return bio.getvalue()

def produce_orders(count=100, delay=0.5):
    for i in range(count):
        order = {
            "orderId": str(uuid.uuid4()),
            "product": random.choice(products),
            "price": round(random.uniform(5.0, 200.0), 2)
        }
        data = avro_serialize(parsed_schema, order)
        producer.send(TOPIC, data)
        print(f"Produced: {order}")
        time.sleep(delay)
    producer.flush()

if __name__ == "__main__":
    produce_orders(count=200, delay=0.2)
