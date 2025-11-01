# Message Queue (MQ) Integration Guide

This guide explains how to configure and use Message Queue integration for real-time data ingestion into Tile38.

## Supported Message Queue Systems

- **Apache Kafka** - High-throughput distributed streaming platform
- **RabbitMQ** - Reliable message broker with flexible routing

## Configuration

### Kafka Configuration

Enable Kafka consumer in `application.yml`:

```yaml
tile38:
  persistence:
    mq:
      enabled: true
      type: kafka
      kafka:
        bootstrap-servers: localhost:9092
        topic: tile38-data
        group-id: tile38-consumer-group
        concurrency: 3
        auto-offset-reset: latest
```

### RabbitMQ Configuration

Enable RabbitMQ consumer in `application.yml`:

```yaml
tile38:
  persistence:
    mq:
      enabled: true
      type: rabbitmq
      rabbitmq:
        host: localhost
        port: 5672
        username: guest
        password: guest
        queue: tile38-data
        concurrency: 3
```

## Message Format

Messages should be in JSON format with the following structure:

### SET Operation

Add or update a geospatial object:

```json
{
  "operation": "SET",
  "collection": "fleet",
  "id": "truck1",
  "geometry": {
    "type": "Point",
    "coordinates": [116.3883, 39.9289]
  },
  "fields": {
    "speed": 60,
    "driver": "John Doe",
    "status": "active"
  },
  "expireAt": 1735689600000
}
```

### SET with WKT

You can also use Well-Known Text (WKT) format:

```json
{
  "operation": "SET",
  "collection": "fleet",
  "id": "truck2",
  "geometry": {
    "wkt": "POINT(116.3883 39.9289)"
  },
  "fields": {
    "speed": 45
  }
}
```

### SET with Complex Geometries

#### LineString Example
```json
{
  "operation": "SET",
  "collection": "routes",
  "id": "route1",
  "geometry": {
    "type": "LineString",
    "coordinates": [
      [116.3883, 39.9289],
      [116.3984, 39.9389],
      [116.4085, 39.9489]
    ]
  }
}
```

#### Polygon Example
```json
{
  "operation": "SET",
  "collection": "zones",
  "id": "zone1",
  "geometry": {
    "type": "Polygon",
    "coordinates": [
      [
        [116.3883, 39.9289],
        [116.4883, 39.9289],
        [116.4883, 39.8289],
        [116.3883, 39.8289],
        [116.3883, 39.9289]
      ]
    ]
  }
}
```

### DEL Operation

Delete a geospatial object:

```json
{
  "operation": "DEL",
  "collection": "fleet",
  "id": "truck1"
}
```

### DROP Operation

Drop an entire collection:

```json
{
  "operation": "DROP",
  "collection": "fleet"
}
```

## Geometry Types Supported

- **Point** - Single coordinate point
- **LineString** - Series of connected points
- **Polygon** - Closed shape with optional holes
- **MultiPoint** - Collection of points
- **MultiLineString** - Collection of line strings
- **MultiPolygon** - Collection of polygons

## Coordinate Format

Coordinates are in **[longitude, latitude]** format (GeoJSON standard).

## Error Handling

### Failed Messages

When a message fails to process:
- Error is logged with details
- Message processing stops with an exception
- In Kafka: offset is not committed (message will be reprocessed)
- In RabbitMQ: message is not acknowledged (message will be reprocessed)

### Best Practices

1. **Implement Dead Letter Queues (DLQ)**
   - Configure DLQ for messages that fail repeatedly
   - Review and fix failed messages offline

2. **Monitor Consumer Lag**
   - Track Kafka consumer lag
   - Monitor RabbitMQ queue depth

3. **Use Batching**
   - Send messages in batches for better performance
   - Adjust `concurrency` settings based on load

4. **Validation**
   - Validate message format before sending
   - Ensure geometry data is valid

## Example: Sending Messages

### Kafka Producer (Python)

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message = {
    "operation": "SET",
    "collection": "fleet",
    "id": "truck1",
    "geometry": {
        "type": "Point",
        "coordinates": [116.3883, 39.9289]
    },
    "fields": {
        "speed": 60
    }
}

producer.send('tile38-data', message)
producer.flush()
```

### RabbitMQ Producer (Python)

```python
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)
channel = connection.channel()

channel.queue_declare(queue='tile38-data', durable=True)

message = {
    "operation": "SET",
    "collection": "fleet",
    "id": "truck1",
    "geometry": {
        "type": "Point",
        "coordinates": [116.3883, 39.9289]
    },
    "fields": {
        "speed": 60
    }
}

channel.basic_publish(
    exchange='',
    routing_key='tile38-data',
    body=json.dumps(message),
    properties=pika.BasicProperties(
        delivery_mode=2  # make message persistent
    )
)

connection.close()
```

## Performance Tuning

### Kafka

- **concurrency**: Number of consumer threads (default: 3)
- **max.poll.records**: Messages per poll (default: 100)
- **auto-offset-reset**: Start position for new consumers (latest/earliest)

### RabbitMQ

- **concurrency**: Number of consumer threads (default: 3)
- **prefetchCount**: Messages prefetched per consumer (default: 10)

## Monitoring

Enable Actuator endpoints to monitor MQ consumer health:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
```

## Troubleshooting

### Kafka Consumer Not Starting

1. Check Kafka broker is running: `telnet localhost 9092`
2. Verify topic exists: `kafka-topics.sh --list --bootstrap-server localhost:9092`
3. Check application logs for connection errors

### RabbitMQ Consumer Not Starting

1. Check RabbitMQ server is running: `rabbitmqctl status`
2. Verify queue exists: Check RabbitMQ management UI (http://localhost:15672)
3. Verify credentials are correct

### Messages Not Being Processed

1. Check message format matches expected JSON structure
2. Verify geometry coordinates are valid
3. Review application logs for processing errors
4. Check consumer lag/queue depth

## Integration with Other Persistence Options

MQ integration works alongside RDB and MySQL persistence:

- **RDB**: Periodic snapshots continue as scheduled
- **MySQL**: Real-time writes to MySQL happen for MQ-ingested data
- **MQ**: Real-time data ingestion from message queues

All three can be enabled simultaneously for maximum data durability.
