# Tile38 Enterprise Integration

This Java Spring Boot application provides enterprise-grade integration capabilities for the Tile38 geospatial database.

## Features

- **JDK 21 with ZGC Optimization**: Optimized for low-latency geospatial operations with minimal GC pauses
- **Spring Boot 3.x**: Modern enterprise framework with reactive programming support  
- **Enterprise Security**: HTTP Basic authentication with role-based access control
- **Caching**: Intelligent caching of frequently accessed geospatial queries
- **Metrics & Monitoring**: Prometheus metrics and Spring Boot Actuator health checks
- **Async Processing**: Non-blocking batch operations for high-throughput scenarios
- **RESTful API**: Clean REST endpoints for geospatial operations

## Architecture

The application acts as an enterprise integration layer on top of the existing Tile38 Go service:

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   Client Apps   │───▶│  Spring Boot Layer   │───▶│   Tile38 Go     │
│                 │    │  (Java 21 + ZGC)    │    │   Service       │
│                 │    │                      │    │                 │
│  - Web Apps     │    │ - REST APIs          │    │ - Geospatial    │
│  - Mobile Apps  │    │ - Security           │    │   Engine        │
│  - IoT Devices  │    │ - Caching            │    │ - Redis Protocol│
│                 │    │ - Metrics            │    │ - Persistence   │
└─────────────────┘    └──────────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- JDK 21 or later
- Maven 3.8+
- Running Tile38 server (default: localhost:9851)

### Building

```bash
cd java-integration
mvn clean compile
```

### Running with ZGC

```bash
mvn spring-boot:run
```

The application will start with ZGC optimization enabled automatically.

### Running Tests

```bash
mvn test
```

## Configuration

Configure the connection to your Tile38 server in `application.properties`:

```properties
# Tile38 Connection
tile38.host=localhost
tile38.port=9851
tile38.password=
tile38.timeout=5000

# Security
spring.security.user.name=admin
spring.security.user.password=your-secure-password
```

## API Endpoints

### Store a Point
```http
POST /tile38-enterprise/api/v1/geospatial/collections/{collection}/objects/{id}/point
```

### Find Nearby Objects
```http  
GET /tile38-enterprise/api/v1/geospatial/collections/{collection}/nearby?latitude={lat}&longitude={lon}&radius={radius}&unit={unit}
```

### Find Objects Within Bounds
```http
GET /tile38-enterprise/api/v1/geospatial/collections/{collection}/within?minLat={minLat}&minLon={minLon}&maxLat={maxLat}&maxLon={maxLon}
```

### Health Check
```http
GET /tile38-enterprise/api/v1/geospatial/health
```

## ZGC Optimization

The application is configured with ZGC for optimal geospatial processing:

- **Low Latency**: Sub-10ms GC pauses regardless of heap size
- **Large Heap Support**: Efficiently handles multi-GB heaps for large datasets
- **Transparent Huge Pages**: Enhanced memory performance for geospatial calculations

## Monitoring

Access monitoring endpoints:

- Health: `/actuator/health`
- Metrics: `/actuator/metrics` 
- Prometheus: `/actuator/prometheus`

## Enterprise Features

- **Authentication**: HTTP Basic auth with configurable users/roles
- **Caching**: Caffeine-based caching with TTL for query results
- **Metrics**: Custom metrics for geospatial operations
- **Async Processing**: Background batch processing capabilities
- **Security Headers**: CSRF protection, frame options, etc.

## Performance Tuning

For production deployments, adjust JVM settings in `pom.xml`:

```xml
<jvmArguments>
    -XX:+UseZGC
    -Xms4g
    -Xmx16g
    -XX:MaxGCPauseMillis=100
</jvmArguments>
```