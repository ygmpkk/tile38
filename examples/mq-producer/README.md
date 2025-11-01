# MQ Producer Examples

This directory contains example producer applications that demonstrate how to send data to Tile38 via Message Queues.

## Prerequisites

### For Kafka Example
```bash
pip install kafka-python
```

### For RabbitMQ Example
```bash
pip install pika
```

## Running the Examples

### Kafka Producer
```bash
python kafka_producer_example.py
```

This example demonstrates:
- Sending vehicle location points
- Creating delivery zones (polygons)
- Updating object locations
- Deleting objects

### RabbitMQ Producer
```bash
python rabbitmq_producer_example.py
```

This example demonstrates:
- Sending bus location points
- Creating routes (linestrings)
- Updating vehicle locations
- Deleting objects

## Message Format

All messages should be in JSON format with the following structure:

### SET Operation
```json
{
  "operation": "SET",
  "collection": "fleet",
  "id": "vehicle1",
  "geometry": {
    "type": "Point",
    "coordinates": [116.3883, 39.9289]
  },
  "fields": {
    "speed": 60,
    "driver": "John Doe"
  }
}
```

### DEL Operation
```json
{
  "operation": "DEL",
  "collection": "fleet",
  "id": "vehicle1"
}
```

### DROP Operation
```json
{
  "operation": "DROP",
  "collection": "fleet"
}
```

## Testing

Before running the examples, ensure that:

1. **Kafka/RabbitMQ is running**
   - Kafka: `localhost:9092`
   - RabbitMQ: `localhost:5672`

2. **Tile38 server is running** with MQ enabled in `application.yml`:

   For Kafka:
   ```yaml
   tile38:
     persistence:
       mq:
         enabled: true
         type: kafka
         kafka:
           bootstrap-servers: localhost:9092
           topic: tile38-data
   ```

   For RabbitMQ:
   ```yaml
   tile38:
     persistence:
       mq:
         enabled: true
         type: rabbitmq
         rabbitmq:
           host: localhost
           port: 5672
           queue: tile38-data
   ```

3. **Run the example**:
   ```bash
   python kafka_producer_example.py
   # or
   python rabbitmq_producer_example.py
   ```

## Advanced Usage

### Batch Sending
For high-throughput scenarios, send messages in batches:

```python
# Kafka
for i in range(1000):
    producer.send('tile38-data', create_message(i))
producer.flush()

# RabbitMQ
for i in range(1000):
    channel.basic_publish(...)
```

### Error Handling
Add error handling for production use:

```python
try:
    send_point_data(...)
except Exception as e:
    logger.error(f"Failed to send message: {e}")
    # Implement retry logic or store in DLQ
```

### Performance Tips
- Use connection pooling
- Batch messages when possible
- Monitor consumer lag (Kafka) or queue depth (RabbitMQ)
- Adjust concurrency settings based on load

## See Also
- [MQ Integration Guide](../../MQ_INTEGRATION_GUIDE.md) - Complete integration documentation
- [Tile38 API Documentation](../../README.md) - Main README
