import io
import json
import random
import time

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer


TOPIC = "orders.received.v1"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered -> "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


with open("schemas/order.avsc", "r") as schema_file:
    schema = json.load(schema_file)

parsed_schema = parse_schema(schema)


producer = Producer({
    "bootstrap.servers": "localhost:9092"
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

    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        parsed_schema,
        order
    )

    avro_bytes = buffer.getvalue()

    producer.produce(
        topic=TOPIC,
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