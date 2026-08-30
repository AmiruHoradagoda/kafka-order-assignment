import io
import json

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer

TOPIC = "orders.received.v1"


# This function is called when Kafka confirms
# whether the message was delivered or failed.
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print("Message delivered successfully")
        print(f"Topic: {msg.topic()}")
        print(f"Partition: {msg.partition()}")
        print(f"Offset: {msg.offset()}")


# -------------------------
# Load Avro schema
# -------------------------

with open("schemas/order.avsc", "r") as schema_file:
    schema = json.load(schema_file)

parsed_schema = parse_schema(schema)

# -------------------------
# Create Kafka Producer
# -------------------------

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

# -------------------------
# Create Order
# -------------------------

order = {
    "orderId": "1001",
    "product": "Laptop",
    "price": 1250.50
}

# -------------------------
# Serialize Order using Avro
# -------------------------

buffer = io.BytesIO()

schemaless_writer(
    buffer,
    parsed_schema,
    order
)

avro_bytes = buffer.getvalue()

# -------------------------
# Send Avro bytes to Kafka
# -------------------------
producer.produce(
    topic=TOPIC,
    key=order["orderId"],
    value=avro_bytes,
    callback=delivery_report
)


# Wait until all pending messages are delivered
producer.flush()