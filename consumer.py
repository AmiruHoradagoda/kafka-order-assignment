import io
import json

from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader


TOPIC = "orders.received.v1"


# -------------------------
# Load Avro schema
# -------------------------

with open("schemas/order.avsc", "r") as schema_file:
    schema = json.load(schema_file)

parsed_schema = parse_schema(schema)


# -------------------------
# Create Kafka Consumer
# -------------------------

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-avro-processing-group",
    "auto.offset.reset": "earliest"
})


consumer.subscribe([TOPIC])

print("Waiting for Avro orders...")


try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue


        # -------------------------
        # Deserialize Avro bytes
        # -------------------------

        buffer = io.BytesIO(message.value())

        order = schemaless_reader(
            buffer,
            parsed_schema
        )


        # -------------------------
        # Decode Kafka key
        # -------------------------

        key = (
            message.key().decode("utf-8")
            if message.key() is not None
            else None
        )


        # -------------------------
        # Print Order
        # -------------------------

        print("\nReceived Order")

        print(f"Key: {key}")
        print(f"Order ID: {order['orderId']}")
        print(f"Product: {order['product']}")
        print(f"Price: {order['price']}")

        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")

        print("------------------------")


except KeyboardInterrupt:

    print("\nStopping consumer...")


finally:

    consumer.close()