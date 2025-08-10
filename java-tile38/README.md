# Tile38 Java Server

This is a Java implementation of Tile38, a geospatial database server, built using **JDK 17**, **Spring Boot**, **Lombok**, and **DUBBO**.

## Features

- ✅ HTTP REST API support
- ✅ DUBBO RPC protocol support  
- ✅ In-memory geospatial storage with JTS (Java Topology Suite)
- ✅ Spatial indexing with STRtree
- ✅ Core Tile38 operations: SET, GET, DEL, DROP, BOUNDS, NEARBY
- ✅ Object expiration support
- ✅ Statistics and monitoring endpoints
- ✅ Built with modern Java 17 features
- ✅ Lombok for clean code
- ✅ Comprehensive test coverage

## Quick Start

### Prerequisites

- JDK 17 or higher
- Maven 3.6+
- (Optional) Nacos registry for DUBBO

### Build

```bash
cd java-tile38
mvn clean package
```

### Run

```bash
java -jar target/tile38-server-1.0.0.jar
```

The server will start on:
- HTTP: http://localhost:9851
- DUBBO: localhost:20880

## HTTP API Examples

### Set a Point
```bash
curl -X POST http://localhost:9851/api/v1/keys/fleet/objects/truck1 \
  -H "Content-Type: application/json" \
  -d '{"lat": 33.5, "lon": -115.5, "fields": {"driver": "John", "speed": 65}}'
```

### Get an Object
```bash
curl http://localhost:9851/api/v1/keys/fleet/objects/truck1
```

### Search Nearby
```bash
curl "http://localhost:9851/api/v1/keys/fleet/nearby?lat=33.5&lon=-115.5&radius=1000"
```

### Get Collection Bounds
```bash
curl http://localhost:9851/api/v1/keys/fleet/bounds
```

### Delete Object
```bash
curl -X DELETE http://localhost:9851/api/v1/keys/fleet/objects/truck1
```

### Get All Keys
```bash
curl http://localhost:9851/api/v1/keys
```

### Get Server Stats
```bash
curl http://localhost:9851/api/v1/stats
```

## DUBBO RPC Usage

```java
// Inject the DUBBO service
@Reference
private Tile38RpcService tile38RpcService;

// Set a point
tile38RpcService.set("fleet", "truck1", 33.5, -115.5, 
    Map.of("driver", "John"), null);

// Get an object
Tile38Object obj = tile38RpcService.get("fleet", "truck1");

// Search nearby
List<SearchResult> results = tile38RpcService.nearby("fleet", 33.5, -115.5, 1000);
```

## Architecture

- **Model Layer**: Tile38Object, Bounds, SearchResult with Lombok annotations
- **Repository Layer**: SpatialRepository using JTS STRtree for spatial indexing  
- **Service Layer**: Tile38Service with business logic
- **Controller Layer**: REST API endpoints
- **DUBBO Layer**: RPC service interface and implementation
- **Configuration**: Spring Boot auto-configuration

## Key Components

- **JTS (Java Topology Suite)**: Geospatial operations and spatial indexing
- **STRtree**: R-tree variant for efficient spatial queries
- **Spring Boot**: Web framework and dependency injection
- **DUBBO**: High-performance RPC framework
- **Lombok**: Reduces boilerplate code
- **Caffeine**: High-performance caching (if needed)

## Differences from Original Go Version

- ❌ Removed Redis RESP protocol support
- ❌ Removed WebSocket, Telnet, gRPC protocols  
- ❌ Removed messaging endpoints (MQTT, Kafka, NATS, etc.)
- ✅ Added DUBBO RPC protocol
- ✅ Uses JTS instead of custom geospatial libraries
- ✅ Spring Boot ecosystem integration
- ✅ Java 21 features and performance improvements

## Configuration

See `application.yml` for configuration options:

- Server port (default: 9851)
- DUBBO settings
- Logging configuration
- Management endpoints

## Testing

```bash
mvn test
```

## Monitoring

Built-in Spring Boot Actuator endpoints:
- `/actuator/health` - Health check
- `/actuator/metrics` - Metrics
- `/actuator/info` - Application info

## License

MIT License (same as original Tile38)