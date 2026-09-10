import io
import json
import time
from pprint import pprint

from confluent_kafka import Consumer, Producer
from fastavro import parse_schema, schemaless_reader
from exceptions import PermanentProcessingError, TemporaryProcessingError

TOPIC = "orders.received.v1"
DLQ_TOPIC = "orders.dlq.v1"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

with open("schemas/order.avsc", "r") as schema_file:
    schema = json.load(schema_file)

parsed_schema = parse_schema(schema)


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-aggregation-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False

})

dlq_producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

#make order 1003 failed 2 times
temporary_failures_remaining = {
    "1003": 2
}

def process_order(order):

    order_id = order["orderId"]

    # Permanent failure simulation
    if order_id == "1004":
        raise PermanentProcessingError(
            "Simulated permanent validation failure"
        )

    # Temporary failure simulation
    if (
        order_id in temporary_failures_remaining
        and temporary_failures_remaining[order_id] > 0
    ):
        temporary_failures_remaining[order_id] -= 1

        raise TemporaryProcessingError(
            "Temporary service unavailable"
        )

    print(f"Order {order_id} processed successfully")

def process_with_retry(order):

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            process_order(order)
            return True

        except TemporaryProcessingError as error:

            print(
                f"\nTemporary failure for Order "
                f"{order['orderId']}"
            )

            print(
                f"Attempt {attempt}/{MAX_RETRIES}"
            )

            print(f"Reason: {error}")

            if attempt < MAX_RETRIES:

                print(
                    f"Retrying in "
                    f"{RETRY_DELAY_SECONDS} seconds..."
                )

                time.sleep(RETRY_DELAY_SECONDS)

        except PermanentProcessingError:
            # Do not retry permanent failures
            raise

    return False

def send_to_dlq(message, order, error):

    print(
        f"Sending Order {order['orderId']} "
        f"to DLQ..."
    )

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=message.key(),
        value=message.value()
    )

    dlq_producer.flush()

    print(
        f"Order {order['orderId']} "
        f"sent to {DLQ_TOPIC}"
    )
    
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

        # -------------------------
        # Avro deserialization
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
        # Process order
        # -------------------------
        try:
            success = process_with_retry(order)

            if success:
                total_price += order["price"]

                order_count += 1

                running_average = total_price / order_count


                # -------------------------
                # Print result
                # -------------------------

                print("\nReceived Order")

                pprint({
                    "Key": key,
                    "Order ID": order["orderId"],
                    "Product": order["product"],
                    "Price": f"{order['price']:.2f}",
                })

                print()

                print(f"Orders Processed: {order_count}")
                print(f"Total Price: {total_price:.2f}")
                print(f"Running Average: {running_average:.2f}")

                print()
            else:
                print(
                    f"Order {order['orderId']} "
                    f"failed after all retries."
                )
                # After retry exhaustion,
                # also move it to DLQ
                send_to_dlq(
                    message,
                    order,
                    TemporaryProcessingError(
                        "Maximum retry attempts exhausted"
                    )
                )
        except PermanentProcessingError as error:
            send_to_dlq(
            message,
            order,
            error
        )

        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")
        print("-----------------------------")
        



except KeyboardInterrupt:

    print("\nStopping consumer...")


finally:

    consumer.close()