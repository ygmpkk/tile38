#!/bin/bash

# Demonstration script for Tile38 Enterprise Integration

echo "====================================="
echo "Tile38 Enterprise Integration Demo"
echo "JDK 21 + ZGC + Spring Boot"
echo "====================================="

# Check if Java 21 is available
java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2)
echo "Java Version: $java_version"

if [[ $java_version == 21* ]]; then
    echo "✓ Java 21 detected"
else
    echo "⚠ Warning: Java 21 not detected. ZGC optimization may not be available."
fi

# Check ZGC support
echo ""
echo "ZGC Support Check:"
java -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -version 2>/dev/null && echo "✓ ZGC is supported" || echo "⚠ ZGC may not be supported"

# Build information
echo ""
echo "Build Information:"
echo "- Go Server: Core geospatial engine with Redis protocol"
echo "- Java Integration: Spring Boot enterprise layer"
echo "- ZGC Optimization: Low-latency GC for geospatial data"
echo "- Security: HTTP Basic authentication"
echo "- Monitoring: Prometheus metrics and health checks"
echo "- Caching: Intelligent query result caching"

echo ""
echo "Enterprise Features:"
echo "- RESTful APIs for all geospatial operations"
echo "- Enterprise security and authentication"
echo "- Advanced monitoring and metrics collection"
echo "- Batch processing capabilities"
echo "- High-performance caching layer"
echo "- ZGC garbage collection for sub-10ms latency"

echo ""
echo "Architecture:"
echo "┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐"
echo "│   Client Apps   │───▶│  Spring Boot Layer   │───▶│   Tile38 Go     │"
echo "│                 │    │  (Java 21 + ZGC)    │    │   Service       │"
echo "│                 │    │                      │    │                 │"
echo "│  - Web Apps     │    │ - REST APIs          │    │ - Geospatial    │"
echo "│  - Mobile Apps  │    │ - Security           │    │   Engine        │"
echo "│  - IoT Devices  │    │ - Caching            │    │ - Redis Protocol│"
echo "│                 │    │ - Metrics            │    │ - Persistence   │"
echo "└─────────────────┘    └──────────────────────┘    └─────────────────┘"

echo ""
echo "Usage:"
echo "1. make all                    # Build both Go and Java components"
echo "2. ./start-enterprise.sh       # Start enterprise stack"
echo "3. Open http://localhost:8080/tile38-enterprise/"
echo ""
echo "API Examples:"
echo "# Store a vehicle location"
echo "curl -u admin:tile38-enterprise-2024 \\"
echo "  -X POST 'http://localhost:8080/tile38-enterprise/api/v1/geospatial/collections/vehicles/objects/car123/point' \\"
echo "  -d 'latitude=37.7749&longitude=-122.4194'"
echo ""
echo "# Find nearby vehicles"
echo "curl -u admin:tile38-enterprise-2024 \\"
echo "  'http://localhost:8080/tile38-enterprise/api/v1/geospatial/collections/vehicles/nearby?latitude=37.7749&longitude=-122.4194&radius=1000&unit=m'"
echo ""
echo "Ready to build and run!"