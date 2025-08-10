#!/bin/bash

# Start both Tile38 Go server and Java Enterprise Integration

set -e

echo "Starting Tile38 with Java Enterprise Integration..."

# Build Go server
echo "Building Tile38 Go server..."
cd "$(dirname "$0")"
make tile38-server

# Build Java integration
echo "Building Java Enterprise Integration..."
cd java-integration
export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -DskipTests

# Start Tile38 Go server in background
echo "Starting Tile38 Go server..."
./tile38-server --bind 127.0.0.1:9851 &
TILE38_PID=$!

# Wait for Tile38 to start
echo "Waiting for Tile38 server to start..."
sleep 3

# Start Java Enterprise Integration
echo "Starting Java Enterprise Integration with ZGC..."
cd java-integration
java \
  -XX:+UnlockExperimentalVMOptions \
  -XX:+UseZGC \
  -XX:+UseTransparentHugePages \
  -XX:+UseLargePages \
  -Xms2g \
  -Xmx8g \
  -XX:MaxGCPauseMillis=200 \
  -Dspring.profiles.active=production \
  -Djava.awt.headless=true \
  -Dfile.encoding=UTF-8 \
  -jar target/tile38-enterprise-integration-1.0.0-SNAPSHOT.jar &
JAVA_PID=$!

echo "Tile38 Enterprise Integration started!"
echo "Tile38 Go Server PID: $TILE38_PID"
echo "Java Integration PID: $JAVA_PID"
echo ""
echo "Services:"
echo "- Tile38 Go Server: http://localhost:9851"
echo "- Java Enterprise API: http://localhost:8080/tile38-enterprise/"
echo "- Health Check: http://localhost:8080/tile38-enterprise/actuator/health"
echo "- Metrics: http://localhost:8080/tile38-enterprise/actuator/prometheus"
echo ""
echo "Login: admin / tile38-enterprise-2024"

# Wait for user to stop
echo "Press Ctrl+C to stop both services..."
wait

# Cleanup
echo "Stopping services..."
kill $TILE38_PID 2>/dev/null || true
kill $JAVA_PID 2>/dev/null || true