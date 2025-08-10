#!/bin/bash

# Build and run script for Tile38 Enterprise Integration with ZGC

set -e

echo "Building Tile38 Enterprise Integration with JDK 21 and ZGC optimization..."

# Set Java 21 environment
export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Navigate to integration directory
cd "$(dirname "$0")"

# Clean and compile
echo "Cleaning and compiling..."
mvn clean compile

# Run tests
echo "Running tests..."
mvn test

# Package the application
echo "Packaging application..."
mvn package -DskipTests

# Create startup script with ZGC optimization
cat > start-enterprise.sh << 'EOF'
#!/bin/bash

# Tile38 Enterprise Integration Startup Script with ZGC
export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64

# ZGC Optimization Settings
JVM_OPTS=""
JVM_OPTS="$JVM_OPTS -XX:+UnlockExperimentalVMOptions"
JVM_OPTS="$JVM_OPTS -XX:+UseZGC"
JVM_OPTS="$JVM_OPTS -XX:+UseTransparentHugePages"
JVM_OPTS="$JVM_OPTS -XX:+UseLargePages"
JVM_OPTS="$JVM_OPTS -Xms2g"
JVM_OPTS="$JVM_OPTS -Xmx8g"
JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=200"

# Application Settings
JVM_OPTS="$JVM_OPTS -Dspring.profiles.active=production"
JVM_OPTS="$JVM_OPTS -Djava.awt.headless=true"
JVM_OPTS="$JVM_OPTS -Dfile.encoding=UTF-8"

# GC Logging (optional)
JVM_OPTS="$JVM_OPTS -XX:+UseZGC"
JVM_OPTS="$JVM_OPTS -XX:+UnlockDiagnosticVMOptions"
JVM_OPTS="$JVM_OPTS -XX:+LogVMOutput"

echo "Starting Tile38 Enterprise Integration with ZGC..."
echo "JVM Options: $JVM_OPTS"

$JAVA_HOME/bin/java $JVM_OPTS -jar target/tile38-enterprise-integration-1.0.0-SNAPSHOT.jar "$@"
EOF

chmod +x start-enterprise.sh

echo "Build complete! Use ./start-enterprise.sh to run with ZGC optimization."
echo ""
echo "Enterprise features:"
echo "- JDK 21 with ZGC garbage collection"
echo "- Spring Boot 3.x framework"
echo "- Enterprise security and monitoring"
echo "- Geospatial caching and metrics"
echo "- RESTful API for Tile38 integration"
echo ""
echo "Access the application at: http://localhost:8080/tile38-enterprise/"