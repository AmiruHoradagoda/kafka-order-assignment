import random
import time

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
from utils.avro_utils import (
    load_schema,
    serialize_avro
)
from utils.config_loader import load_config

config = load_config()

KAFKA_BOOTSTRAP_SERVERS = config["kafka"]["bootstrap_servers"]
ORDERS_TOPIC = config["kafka"]["topics"]["orders"]
ORDER_SCHEMA_PATH = config["avro"]["order_schema_path"]

config = load_config()
schema = load_schema(
    "schemas/order.avsc"
)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered -> "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


parsed_schema = parse_schema(schema)


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


products = [
    "Laptop",
    "Phone",
    "Keyboard",
    "Mouse",
    "Monitor"
]


for i in range(1, 11):

    order = {
        "orderId": str(1000 + i),
        "product": random.choice(products),
        "price": round(random.uniform(50.0, 1500.0), 2)
    }

    avro_bytes = serialize_avro(order,schema)

    producer.produce(
        topic=ORDERS_TOPIC,
        key=order["orderId"].encode("utf-8"),
        value=avro_bytes,
        callback=delivery_report
    )

    print(
        f"Sent -> "
        f"{order['orderId']} | "
        f"{order['product']} | "
        f"{order['price']}"
    )

    producer.poll(0)

    time.sleep(1)


producer.flush()