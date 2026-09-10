import io
import json

from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader
from utils.config_loader import load_config
from utils.avro_utils import (
    load_schema,
    deserialize_avro
)
config = load_config()

DLQ_TOPIC = config["topics"]["dlq"]


schema = load_schema(
    "schemas/order.avsc"
)

parsed_schema = parse_schema(schema)


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-monitor-group",
    "auto.offset.reset": "earliest"
})


consumer.subscribe([DLQ_TOPIC])

print("Waiting for DLQ messages...")


try:
    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        order = deserialize_avro(
            message.value(),
            schema
        )

        key = (
            message.key().decode("utf-8")
            if message.key() is not None
            else None
        )

        print("\nDLQ Order Received")
        print(f"Key: {key}")
        print(f"Order ID: {order['orderId']}")
        print(f"Product: {order['product']}")
        print(f"Price: {order['price']:.2f}")
        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")
        print("------------------------")


except KeyboardInterrupt:
    print("\nStopping DLQ consumer...")


finally:
    consumer.close()