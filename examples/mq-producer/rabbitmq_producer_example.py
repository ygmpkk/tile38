#!/usr/bin/env python3
"""
Example RabbitMQ producer for sending Tile38 data
Requires: pip install pika
"""

import pika
import json
import time

def create_connection():
    """Create RabbitMQ connection"""
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        host='localhost',
        port=5672,
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declare queue (durable)
    channel.queue_declare(queue='tile38-data', durable=True)
    
    return connection, channel

def send_message(channel, message):
    """Send a message to RabbitMQ"""
    channel.basic_publish(
        exchange='',
        routing_key='tile38-data',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2  # make message persistent
        )
    )

def send_point_data(channel, collection, object_id, lon, lat, fields=None):
    """Send a point object to Tile38 via RabbitMQ"""
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
    
    send_message(channel, message)
    print(f"Sent: {collection}/{object_id} at ({lon}, {lat})")

def send_linestring_data(channel, collection, object_id, coordinates, fields=None):
    """Send a linestring (route) to Tile38 via RabbitMQ"""
    message = {
        "operation": "SET",
        "collection": collection,
        "id": object_id,
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates
        }
    }
    
    if fields:
        message["fields"] = fields
    
    send_message(channel, message)
    print(f"Sent route: {collection}/{object_id}")

def delete_object(channel, collection, object_id):
    """Delete an object from Tile38 via RabbitMQ"""
    message = {
        "operation": "DEL",
        "collection": collection,
        "id": object_id
    }
    
    send_message(channel, message)
    print(f"Deleted: {collection}/{object_id}")

def drop_collection(channel, collection):
    """Drop a collection from Tile38 via RabbitMQ"""
    message = {
        "operation": "DROP",
        "collection": collection
    }
    
    send_message(channel, message)
    print(f"Dropped collection: {collection}")

def main():
    """Example usage"""
    connection, channel = create_connection()
    
    try:
        # Example 1: Send vehicle locations
        print("\n=== Sending vehicle locations ===")
        send_point_data(
            channel,
            collection="fleet",
            object_id="bus1",
            lon=116.3883,
            lat=39.9289,
            fields={"route": "Route 1", "capacity": 50, "passengers": 23}
        )
        
        send_point_data(
            channel,
            collection="fleet",
            object_id="bus2",
            lon=116.4883,
            lat=39.8289,
            fields={"route": "Route 2", "capacity": 50, "passengers": 35}
        )
        
        # Example 2: Send route (linestring)
        print("\n=== Sending bus route ===")
        route_coords = [
            [116.3883, 39.9289],
            [116.3983, 39.9389],
            [116.4083, 39.9489],
            [116.4183, 39.9589]
        ]
        send_linestring_data(
            channel,
            collection="routes",
            object_id="route1",
            coordinates=route_coords,
            fields={"name": "Route 1", "stops": 12}
        )
        
        # Wait a bit
        time.sleep(1)
        
        # Example 3: Update vehicle location
        print("\n=== Updating vehicle location ===")
        send_point_data(
            channel,
            collection="fleet",
            object_id="bus1",
            lon=116.3983,
            lat=39.9389,
            fields={"route": "Route 1", "capacity": 50, "passengers": 28}
        )
        
        # Example 4: Delete a vehicle
        print("\n=== Deleting vehicle ===")
        delete_object(channel, "fleet", "bus2")
        
        print("\n=== All messages sent successfully ===")
        
    finally:
        connection.close()

if __name__ == "__main__":
    main()
