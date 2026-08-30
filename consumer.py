from confluent_kafka import Consumer

TOPIC = "orders.received.v1"


consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-processing-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe([TOPIC])

print("Waiting for orders...")


try:
    while True:

        message = consumer.poll(1.0)

        # No message received during this poll
        if message is None:
            continue

        # Kafka returned an error
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        # Kafka gives key/value as bytes,
        # so convert them to Python strings.
        key = (
            message.key().decode("utf-8")
            if message.key() is not None
            else None
        )

        value = (
            message.value().decode("utf-8")
            if message.value() is not None
            else None
        )

        print("\nReceived Order")
        print(f"Key: {key}")
        print(f"Value: {value}")
        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")
        print("------------------------")


except KeyboardInterrupt:
    print("\nStopping consumer...")


finally:
    consumer.close()