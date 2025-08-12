# DUBBO RPC Interface - Comprehensive KV Capabilities Documentation

This document describes the enhanced DUBBO RPC interface for Tile38 with comprehensive Key-Value (KV) data support, advanced search operations, and data loading capabilities.

## Overview

The enhanced DUBBO interface provides complete parity with the HTTP controller, including:

- **KV Data Support**: Store and manage tags (String key-value pairs) and attributes (mixed type key-value pairs)
- **Advanced Filtering**: Multi-condition filtering with 13 operators and logical combinations (AND, OR)
- **KV Data Updates**: Update KV data without affecting geometry
- **Bulk Operations**: Efficient bulk loading with KV data support
- **Advanced Search**: Scan, intersects, within operations with filtering
- **Data Loading**: JSON/CSV loading and synthetic test data generation

## Interface Methods

### Basic Operations

```java
// Legacy set method (backward compatible)
void set(String key, String id, double lat, double lon, Map<String, Object> fields, Long expirationSeconds);

// Enhanced set method with KV data
void setWithKV(String key, String id, double lat, double lon, Map<String, Object> fields, 
               Map<String, String> tags, Map<String, Object> attributes, Long expirationSeconds);

// Get object
Tile38Object get(String key, String id);

// Delete object
boolean del(String key, String id);

// Drop collection
boolean drop(String key);

// Get collection bounds
Bounds bounds(String key);
```

### KV Data Operations

```java
// Update KV data without affecting geometry
boolean updateKVData(String key, String id, Map<String, String> tags, Map<String, Object> attributes);

// Update KV data using KVData object
boolean updateKVDataObject(String key, String id, KVData kvData);
```

### Search Operations

```java
// Legacy nearby search (backward compatible)
List<SearchResult> nearby(String key, double lat, double lon, double radius);

// Enhanced nearby search with KV filtering
List<SearchResult> nearbyWithFilter(String key, double lat, double lon, double radius, FilterCondition filter);

// Scan collection with optional filtering and pagination
List<SearchResult> scan(String key, FilterCondition filter, int limit, int offset);

// Search within bounding box
List<SearchResult> within(String key, double minLat, double minLon, double maxLat, double maxLon, FilterCondition filter);

// Search intersecting bounding box
List<SearchResult> intersects(String key, double minLat, double minLon, double maxLat, double maxLon, FilterCondition filter);
```

### Bulk Operations

```java
// Bulk set multiple objects
void bulkSet(String key, Map<String, Tile38Object> objects);
```

### Data Loading Operations

```java
// Load data from JSON file
CompletableFuture<DataLoader.LoadResult> loadFromJson(String filePath);

// Load data from CSV file
CompletableFuture<DataLoader.LoadResult> loadFromCsv(String filePath);

// Generate synthetic test data
CompletableFuture<DataLoader.LoadResult> generateTestData(String collectionName, int numberOfRecords,
                                                         double minLat, double maxLat, 
                                                         double minLon, double maxLon);
```

### Utility Operations

```java
// Get collection bounds
Bounds bounds(String key);

// Get all collection keys
List<String> keys();

// Get server statistics
String stats();

// Flush all data
void flushdb();

// Health check
String ping();
```

## Usage Examples

### 1. Creating Objects with KV Data

```java
// Create a restaurant with tags and attributes
Map<String, String> tags = new HashMap<>();
tags.put("cuisine", "italian");
tags.put("category", "restaurant");

Map<String, Object> attributes = new HashMap<>();
attributes.put("rating", 4.5);
attributes.put("seats", 80);
attributes.put("open", true);

tile38RpcService.setWithKV("restaurants", "restaurant1", 33.5, -115.5, 
                           new HashMap<>(), tags, attributes, null);
```

### 2. Updating KV Data

```java
// Update only KV data without affecting location
Map<String, String> newTags = new HashMap<>();
newTags.put("status", "maintenance");
newTags.put("priority", "high");

Map<String, Object> newAttributes = new HashMap<>();
newAttributes.put("fuel_level", 45.0);
newAttributes.put("active", false);

boolean updated = tile38RpcService.updateKVData("fleet", "truck1", newTags, newAttributes);
```

### 3. Filtered Searches

```java
// Create filter for Italian restaurants with high ratings
FilterCondition cuisineFilter = FilterCondition.builder()
    .key("cuisine")
    .operator(FilterCondition.Operator.EQUALS)
    .value("italian")
    .dataType(FilterCondition.DataType.TAG)
    .build();

FilterCondition ratingFilter = FilterCondition.builder()
    .key("rating")
    .operator(FilterCondition.Operator.GREATER_THAN)
    .value(4.0)
    .dataType(FilterCondition.DataType.ATTRIBUTE)
    .build();

// Combine filters with AND logic
FilterCondition complexFilter = FilterCondition.builder()
    .conditions(List.of(cuisineFilter, ratingFilter))
    .logicalOperator(FilterCondition.LogicalOperator.AND)
    .build();

List<SearchResult> results = tile38RpcService.nearbyWithFilter("restaurants", 33.5, -115.5, 5000, complexFilter);
```

### 4. Bulk Operations

```java
// Create multiple objects with KV data
Map<String, Tile38Object> objects = new HashMap<>();

for (int i = 1; i <= 100; i++) {
    KVData kvData = new KVData();
    kvData.setTag("type", "truck");
    kvData.setTag("status", "active");
    kvData.setAttribute("number", i);
    kvData.setAttribute("fuel", 80.0 - i * 0.5);

    Point point = geometryFactory.createPoint(new Coordinate(-115.0, 33.0 + i * 0.01));

    Tile38Object object = Tile38Object.builder()
        .id("truck" + i)
        .geometry(point)
        .kvData(kvData)
        .timestamp(System.currentTimeMillis())
        .build();
    
    objects.put("truck" + i, object);
}

// Bulk set all objects
tile38RpcService.bulkSet("fleet", objects);
```

### 5. Advanced Search Operations

```java
// Scan with filtering and pagination
FilterCondition highRatedFilter = FilterCondition.builder()
    .key("rating")
    .operator(FilterCondition.Operator.GREATER_THAN)
    .value(4.0)
    .dataType(FilterCondition.DataType.ATTRIBUTE)
    .build();

List<SearchResult> page1 = tile38RpcService.scan("restaurants", highRatedFilter, 10, 0);
List<SearchResult> page2 = tile38RpcService.scan("restaurants", highRatedFilter, 10, 10);

// Bounding box searches
List<SearchResult> withinResults = tile38RpcService.within("restaurants", 
                                                          30.0, -120.0, 35.0, -115.0, null);

List<SearchResult> intersectsResults = tile38RpcService.intersects("poi", 
                                                                  32.9, -115.1, 33.1, -114.9, 
                                                                  cuisineFilter);
```

### 6. Data Loading Operations

```java
// Generate synthetic test data
CompletableFuture<DataLoader.LoadResult> future = tile38RpcService.generateTestData(
    "test_collection", 1000, 30.0, 35.0, -120.0, -115.0);

DataLoader.LoadResult result = future.get();
if (result.isSuccess()) {
    System.out.println("Generated " + result.getRecordsLoaded() + " records in " + 
                      result.getDurationMs() + "ms");
}

// Load from JSON file
CompletableFuture<DataLoader.LoadResult> jsonLoad = tile38RpcService.loadFromJson("/path/to/data.json");
DataLoader.LoadResult jsonResult = jsonLoad.get();

// Load from CSV file  
CompletableFuture<DataLoader.LoadResult> csvLoad = tile38RpcService.loadFromCsv("/path/to/data.csv");
DataLoader.LoadResult csvResult = csvLoad.get();
```

## Filter Operators

### Comparison Operators
- `EQUALS` / `NOT_EQUALS`: Exact match comparison
- `GREATER_THAN` / `GREATER_EQUAL`: Numeric comparison  
- `LESS_THAN` / `LESS_EQUAL`: Numeric comparison

### List Operators
- `IN` / `NOT_IN`: Check if value exists in provided list

### String Operators  
- `CONTAINS` / `NOT_CONTAINS`: Substring matching
- `STARTS_WITH` / `ENDS_WITH`: Prefix/suffix matching

### Existence Operators
- `EXISTS` / `NOT_EXISTS`: Check if key exists

### Logical Operators
- `AND`: All conditions must be true
- `OR`: At least one condition must be true

## Data Types

### Tags
- **Type**: String key-value pairs
- **Use Case**: Categorization, labels, status indicators
- **Example**: `{"category": "restaurant", "cuisine": "italian", "status": "open"}`

### Attributes
- **Type**: Mixed type key-value pairs (String, Number, Boolean, etc.)
- **Use Case**: Flexible data storage, numeric values, complex properties
- **Example**: `{"rating": 4.5, "seats": 80, "open": true, "last_update": "2024-01-15"}`

## Performance Characteristics

- **Set Operations**: High throughput with batch processing
- **KV Updates**: ~275 updates/second for individual updates
- **Bulk Operations**: Optimized for large datasets (1M+ records)
- **Filtered Searches**: Sub-second response times on large datasets
- **Memory Efficiency**: Optimized storage structures for million-level datasets

## Backward Compatibility

The enhanced DUBBO interface maintains full backward compatibility:
- All existing methods continue to work unchanged
- Legacy `set()` method still supported
- No breaking changes to existing API contracts
- Existing clients can upgrade without code changes

## Integration

To use the enhanced DUBBO interface in your application:

1. **Add Dependency**: Ensure DUBBO dependencies are in your project
2. **Configure Registry**: Set up your DUBBO registry (Nacos, ZooKeeper, etc.)
3. **Enable Service**: Uncomment `@DubboService` annotation in `Tile38RpcServiceImpl`
4. **Client Configuration**: Configure DUBBO consumer in your client application

## Configuration

The DUBBO service can be configured through standard Spring Boot properties:

```yaml
dubbo:
  application:
    name: tile38-server
  registry:
    address: nacos://localhost:8848
  protocol:
    name: dubbo
    port: 20880
  provider:
    timeout: 10000
```

## Error Handling

The DUBBO interface provides consistent error handling:
- Invalid parameters return appropriate error responses
- Non-existent objects return null/false as appropriate
- Complex filter validation with descriptive error messages
- Proper exception handling for network and serialization issues

## Logging

All DUBBO operations are logged with DEBUG level for monitoring and troubleshooting:
- Method invocations with parameters
- Operation results and performance metrics
- Error conditions and warnings

This enhanced DUBBO interface provides a powerful, efficient, and backward-compatible RPC API for all Tile38 KV operations while maintaining the performance and scalability of the underlying system.