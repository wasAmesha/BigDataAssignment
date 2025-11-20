# System Design — Kafka Order Processing System

## 1. Overview

This system implements a Kafka-based real-time order processing pipeline using Avro serialization.  
It includes:

- Producer that generates order messages serialized using Avro
- Consumer that processes messages with retry logic
- Dead Letter Queue (DLQ) for permanently failed messages
- Running aggregation (running average of product prices)
- Optional publication of aggregates to another Kafka topic

## 2. Components

### 2.1 Producer

- Generates sample order messages.
- Each message contains:
  - `orderId` (string)
  - `product` (string)
  - `price` (float)
- Uses `fastavro` for binary Avro serialization.
- Sends messages to the `orders` topic.

### 2.2 Consumer

- Reads from the `orders` topic.
- Deserializes messages using the same Avro schema.
- Implements retry logic:
  - Temporary failures → retry with exponential backoff
  - Permanent failures → send to `orders-dlq`
- Maintains a running average using in-memory state:
  - `total_price += price`
  - `count += 1`
  - `running_avg = total_price / count`
- Publishes running averages to `orders-aggregates`.

### 2.3 Dead Letter Queue (DLQ)

- Topic: `orders-dlq`
- Stores original failed messages + error metadata.

### 2.4 Topics

| Topic Name          | Purpose                     |
| ------------------- | --------------------------- |
| `orders`            | Main input topic            |
| `orders-dlq`        | Permanently failed messages |
| `orders-aggregates` | Running average output      |

## 3. Avro Schema

File: `order.avsc`

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "product", "type": "string" },
    { "name": "price", "type": "float" }
  ]
}
```
