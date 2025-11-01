#!/usr/bin/env python3
"""
Example Kafka producer for sending Tile38 data
Requires: pip install kafka-python
"""

from kafka import KafkaProducer
import json
import time

def create_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_point_data(producer, collection, object_id, lon, lat, fields=None):
    """Send a point object to Tile38 via Kafka"""
    message = {
        "operation": "SET",
        "collection": collection,
        "id": object_id,
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat]
        }
    }
    
    if fields:
        message["fields"] = fields
    
    future = producer.send('tile38-data', message)
    result = future.get(timeout=10)
    print(f"Sent: {collection}/{object_id} at ({lon}, {lat})")
    return result

def send_polygon_data(producer, collection, object_id, coordinates, fields=None):
    """Send a polygon object to Tile38 via Kafka"""
    message = {
        "operation": "SET",
        "collection": collection,
        "id": object_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [coordinates]
        }
    }
    
    if fields:
        message["fields"] = fields
    
    future = producer.send('tile38-data', message)
    result = future.get(timeout=10)
    print(f"Sent polygon: {collection}/{object_id}")
    return result

def delete_object(producer, collection, object_id):
    """Delete an object from Tile38 via Kafka"""
    message = {
        "operation": "DEL",
        "collection": collection,
        "id": object_id
    }
    
    future = producer.send('tile38-data', message)
    result = future.get(timeout=10)
    print(f"Deleted: {collection}/{object_id}")
    return result

def drop_collection(producer, collection):
    """Drop a collection from Tile38 via Kafka"""
    message = {
        "operation": "DROP",
        "collection": collection
    }
    
    future = producer.send('tile38-data', message)
    result = future.get(timeout=10)
    print(f"Dropped collection: {collection}")
    return result

def main():
    """Example usage"""
    producer = create_producer()
    
    try:
        # Example 1: Send vehicle locations
        print("\n=== Sending vehicle locations ===")
        send_point_data(
            producer,
            collection="fleet",
            object_id="truck1",
            lon=116.3883,
            lat=39.9289,
            fields={"speed": 60, "driver": "John Doe", "status": "active"}
        )
        
        send_point_data(
            producer,
            collection="fleet",
            object_id="truck2",
            lon=116.4883,
            lat=39.8289,
            fields={"speed": 45, "driver": "Jane Smith", "status": "idle"}
        )
        
        # Example 2: Send delivery zone (polygon)
        print("\n=== Sending delivery zone ===")
        zone_coords = [
            [116.3883, 39.9289],
            [116.4883, 39.9289],
            [116.4883, 39.8289],
            [116.3883, 39.8289],
            [116.3883, 39.9289]  # Close the polygon
        ]
        send_polygon_data(
            producer,
            collection="zones",
            object_id="zone1",
            coordinates=zone_coords,
            fields={"name": "Downtown", "priority": "high"}
        )
        
        # Wait a bit
        time.sleep(2)
        
        # Example 3: Update vehicle location
        print("\n=== Updating vehicle location ===")
        send_point_data(
            producer,
            collection="fleet",
            object_id="truck1",
            lon=116.3983,
            lat=39.9389,
            fields={"speed": 55, "driver": "John Doe", "status": "active"}
        )
        
        # Example 4: Delete a vehicle
        print("\n=== Deleting vehicle ===")
        delete_object(producer, "fleet", "truck2")
        
        # Flush to ensure all messages are sent
        producer.flush()
        print("\n=== All messages sent successfully ===")
        
    finally:
        producer.close()

if __name__ == "__main__":
    main()
