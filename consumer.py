import io
import json

from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader


TOPIC = "orders.received.v1"


with open("schemas/order.avsc", "r") as schema_file:
    schema = json.load(schema_file)

parsed_schema = parse_schema(schema)


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-aggregation-group",
    "auto.offset.reset": "earliest"
})


consumer.subscribe([TOPIC])

print("Waiting for Avro orders...")


total_price = 0.0
order_count = 0


try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue


        buffer = io.BytesIO(message.value())

        order = schemaless_reader(
            buffer,
            parsed_schema
        )


        key = (
            message.key().decode("utf-8")
            if message.key() is not None
            else None
        )


        # -------------------------
        # Running aggregation
        # -------------------------

        total_price += order["price"]

        order_count += 1

        running_average = total_price / order_count


        # -------------------------
        # Print result
        # -------------------------

        print("\nReceived Order")

        print(f"Key: {key}")
        print(f"Order ID: {order['orderId']}")
        print(f"Product: {order['product']}")
        print(f"Price: {order['price']:.2f}")

        print()

        print(f"Orders Processed: {order_count}")
        print(f"Total Price: {total_price:.2f}")
        print(f"Running Average: {running_average:.2f}")

        print()

        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")

        print("-----------------------------")


except KeyboardInterrupt:

    print("\nStopping consumer...")


finally:

    consumer.close()