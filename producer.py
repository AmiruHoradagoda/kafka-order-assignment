from confluent_kafka import Producer

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


# Kafka producer configuration
producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)


# Simple Kafka record
order_id = "1001"
order_message = "Order-1001"


producer.produce(
    topic=TOPIC,
    key=order_id,
    value=order_message,
    callback=delivery_report
)


# Wait until all pending messages are delivered
producer.flush()