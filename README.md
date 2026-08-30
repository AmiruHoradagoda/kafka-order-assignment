# Kafka Order Assignment

This is a beginner Kafka project for one simple flow:

```text
Python Producer
      |
      | serialize/send
      v
orders.received.v1
    Kafka Topic
      |
      | poll
      v
Python Consumer
```

This stage intentionally uses plain strings only:

```text
Topic: orders.received.v1
Key: 1001
Value: Order-1001
```

No Avro, Schema Registry, retry logic, DLQ, running averages, databases, FastAPI, or other advanced features are included yet.

## Requirements

- Python 3.12+
- Docker Desktop or Docker Engine with Docker Compose
- `uv`

## 1. Install `uv` If Needed

Follow the official install instructions:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Check it:

```powershell
uv --version
```

## 2. Initialize Or Sync Dependencies

If this project has already been created, run:

```powershell
uv sync
```

The dependency used by the Python code is:

```powershell
uv add confluent-kafka
```

Do not use `pip install` for this project.

## 3. Start Kafka

From the `kafka-order-assignment` folder:

```powershell
docker compose up -d
```

This starts one local Apache Kafka broker in KRaft mode. ZooKeeper is not used.

## 4. Check The Kafka Container

```powershell
docker ps
```

You should see a container named:

```text
kafka-order-assignment
```

## 5. Create The Topic

Create the topic manually after Kafka starts:

```powershell
docker exec -it kafka-order-assignment /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders.received.v1 --partitions 1 --replication-factor 1
```

This creates:

- Topic: `orders.received.v1`
- Partitions: `1`
- Replication factor: `1`

## 6. List Kafka Topics

```powershell
docker exec -it kafka-order-assignment /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

You should see:

```text
orders.received.v1
```

## 7. Start The Consumer

Keep this terminal open:

```powershell
uv run consumer.py
```

The consumer waits for messages from Kafka.

## 8. Open Another Terminal

Open a second terminal and go to the same folder:

```powershell
cd kafka-order-assignment
```

## 9. Run The Producer

```powershell
uv run producer.py
```

The producer sends one message:

```text
Topic: orders.received.v1
Key: 1001
Value: Order-1001
```

## 10. Observe The Consumer Output

The first terminal should print something similar to:

```text
Received Order
Key: 1001
Value: Order-1001
Partition: 0
Offset: 0
------------------------
```

Run the producer again:

```powershell
uv run producer.py
```

Each new message is appended to the topic partition, so the offset should increase:

```text
Offset: 1
Offset: 2
Offset: 3
```

## 11. Stop Kafka When Finished

```powershell
docker compose down
```

## Useful Docker Commands

Check running containers:

```powershell
docker ps
```

Check all containers:

```powershell
docker ps -a
```

View Kafka logs:

```powershell
docker logs kafka-order-assignment
```

List Kafka topics:

```powershell
docker exec -it kafka-order-assignment /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Stop containers:

```powershell
docker compose down
```

## Kafka Concepts In This Project

### Producer

A Kafka Producer is an application that sends records to Kafka. In this project, `producer.py` sends one order message.

### Consumer

A Kafka Consumer is an application that reads records from Kafka. In this project, `consumer.py` continuously polls Kafka and prints received orders.

### Topic

A topic is a named stream of records. This project uses one topic:

```text
orders.received.v1
```

### Message Key And Value

The key is metadata Kafka can use to choose a partition. This project uses:

```text
Key: 1001
```

The value is the actual message payload. This project uses:

```text
Value: Order-1001
```

### `bootstrap.servers`

`bootstrap.servers` is the Kafka broker address the Python client connects to first. This project uses:

```text
localhost:9092
```

### `produce()`

`produce()` queues a message to be sent by the producer to a Kafka topic.

### `flush()`

`flush()` waits for queued producer messages to finish sending before the Python program exits. Without it, a short script might end before the message is delivered.

### `subscribe()`

`subscribe()` tells the consumer which Kafka topic or topics it wants to read from.

### `poll()`

`poll()` asks Kafka for the next available message. If no message is available before the timeout, it returns `None`.

### Consumer Group

A consumer group is a named group of consumers that work together. Kafka tracks offsets for the group. This project uses:

```text
order-processing-group
```

### Partition

A partition is an ordered log inside a topic. This project creates one partition, so all messages go to partition `0`.

### Offset

An offset is the position of a message inside a partition. When you run the producer multiple times, Kafka appends new messages and the offsets increase.

## Later Evolution

This simple string-based version can later evolve into:

```text
Python Order Object
      ↓
Avro Serialization
      ↓
Kafka
      ↓
Avro Deserialization
      ↓
Consumer
```

At that later stage, the producer will convert a Python order object into Avro bytes before sending it to Kafka, and the consumer will convert Avro bytes back into a Python-friendly structure after reading from Kafka.
