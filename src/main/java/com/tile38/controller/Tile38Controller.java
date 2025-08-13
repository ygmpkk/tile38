package com.tile38.controller;

import com.tile38.service.Tile38Service;
import com.tile38.aspect.Timed;
import com.tile38.aspect.TimingAspect;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.model.KVData;
import com.tile38.model.FilterCondition;
import com.tile38.model.FilterRequest;
import com.tile38.model.LocationEntity;
import com.tile38.model.param.*;
import com.tile38.model.result.*;
import com.tile38.loader.DataLoader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.ParseException;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.time.Instant;

/**
 * HTTP REST API Controller for Tile38 operations
 * Provides HTTP endpoints equivalent to the original Tile38 commands
 * Enhanced with bulk loading capabilities for million-level data
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
public class Tile38Controller {
    
    @Autowired
    private Tile38Service tile38Service;
    
    @Autowired
    private DataLoader dataLoader;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    
    /**
     * SET key id POINT geometry [FIELD name value ...] [EX seconds]
     * HTTP: POST /api/v1/keys/{key}/objects/{id}
     * Polygon-centric architecture with KV data as supplemental metadata
     */
    @PostMapping("/keys/{key}/objects/{id}")
    @Timed("Set object operation")
    public ResponseEntity<ApiResponse<ObjectResult>> setObject(
            @PathVariable String key,
            @PathVariable String id,
            @RequestBody SetObjectParam param) {
        
        try {
            // Validate geometry
            if (!param.hasValidGeometry()) {
                return ResponseEntity.badRequest().body(ApiResponse.error("Valid geometry is required"));
            }
            
            log.debug("Setting polygon object {}/{}", key, id);
            
            // Create KV data using unified approach
            KVData kvData = param.getEffectiveKVData();
            
            // Parse expiration
            Instant expireAt = param.getEx() != null ? Instant.now().plusSeconds(param.getEx()) : null;
            
            // Create object
            Tile38Object object = Tile38Object.builder()
                    .id(id)
                    .geometry(param.getGeometry())
                    .fields(param.getFields())
                    .kvData(kvData)
                    .expireAt(expireAt)
                    .timestamp(System.currentTimeMillis())
                    .build();
            
            // Store object
            tile38Service.set(key, id, object);
            
            log.debug("Completed setting object {}/{}", key, id);
            
            ObjectResult result = ObjectResult.builder().build();
            return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
            
        } catch (Exception e) {
            log.error("Error setting object {}/{}", key, id, e);
            return ResponseEntity.internalServerError().body(ApiResponse.error(e.getMessage()));
        }
    }

    
    /**
     * GET key id
     * HTTP: GET /api/v1/keys/{key}/objects/{id}
     * Returns polygon object with KV data as supplemental metadata
     */
    @GetMapping("/keys/{key}/objects/{id}")
    @Timed("Get object operation")
    public ResponseEntity<ApiResponse<ObjectResult>> getObject(
            @PathVariable String key,
            @PathVariable String id) {
        
        log.debug("Getting object {}/{}", key, id);
        
        Optional<Tile38Object> object = tile38Service.get(key, id);
        
        log.debug("Completed getting object {}/{}, found: {}", key, id, object.isPresent());
        
        if (object.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        ObjectResult result = ObjectResult.builder()
                .object(object.get())
                .found(true)
                .build();
                
        return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
    }
    
    /**
     * DEL key id
     * HTTP: DELETE /api/v1/keys/{key}/objects/{id}
     * Deletes polygon object by ID
     */
    @DeleteMapping("/keys/{key}/objects/{id}")
    @Timed("Delete object operation")
    public ResponseEntity<ApiResponse<ObjectResult>> deleteObject(
            @PathVariable String key,
            @PathVariable String id) {
        
        log.debug("Deleting object {}/{}", key, id);
        
        boolean deleted = tile38Service.del(key, id);
        
        log.debug("Completed deleting object {}/{}, deleted: {}", key, id, deleted);
        
        ObjectResult result = ObjectResult.builder()
                .deleted(deleted ? 1 : 0)
                .build();
                
        return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
    }
    
    /**
     * DROP key
     * HTTP: DELETE /api/v1/keys/{key}
     */
    @DeleteMapping("/keys/{key}")
    @Timed("Drop collection operation")
    public ResponseEntity<ApiResponse<CollectionResult>> dropCollection(@PathVariable String key) {
        log.debug("Dropping collection '{}'", key);
        
        boolean dropped = tile38Service.drop(key);
        
        log.debug("Completed dropping collection '{}', dropped: {}", key, dropped);
        
        CollectionResult result = CollectionResult.builder()
                .dropped(dropped ? 1 : 0)
                .build();
                
        return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
    }
    
    /**
     * BOUNDS key
     * HTTP: GET /api/v1/keys/{key}/bounds
     */
    @GetMapping("/keys/{key}/bounds")
    @Timed("Get bounds operation")
    public ResponseEntity<ApiResponse<CollectionResult>> getBounds(@PathVariable String key) {
        log.debug("Getting bounds for collection '{}'", key);
        
        Optional<Bounds> bounds = tile38Service.bounds(key);
        
        log.debug("Completed getting bounds for collection '{}', found: {}", key, bounds.isPresent());
        
        if (bounds.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        CollectionResult result = CollectionResult.builder()
                .bounds(bounds.get())
                .build();
                
        return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
    }
    
    /**
     * NEARBY key POINT lat lon radius
     * HTTP: GET /api/v1/keys/{key}/nearby?lat={lat}&lon={lon}&radius={radius}
     * Enhanced with optional KV filtering - polygon-centric search
     */
    @GetMapping("/keys/{key}/nearby")
    @Timed("Nearby search operation")
    public ResponseEntity<ApiResponse<SearchResultData>> nearby(
            @PathVariable String key,
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam double radius,
            @RequestParam(required = false) String filter) {
        
        log.debug("Starting nearby search for collection '{}' at ({},{}) with radius {}", key, lat, lon, radius);
        
        try {
            FilterCondition filterCondition = null;
            if (filter != null && !filter.trim().isEmpty()) {
                try {
                    // Simple filter parsing: format like "tag:category=restaurant" or "attr:speed>50"
                    filterCondition = parseSimpleFilter(filter);
                } catch (Exception e) {
                    return ResponseEntity.badRequest().body(ApiResponse.error("Invalid filter format: " + e.getMessage()));
                }
            }
            
            List<SearchResult> results = filterCondition != null ? 
                    tile38Service.nearby(key, lat, lon, radius, filterCondition) :
                    tile38Service.nearby(key, lat, lon, radius);
            
            log.debug("Completed nearby search for collection '{}', found {} results", key, results.size());
            
            SearchResultData resultData = SearchResultData.builder()
                    .count(results.size())
                    .objects(results)
                    .build();
            
            return ResponseEntity.ok(ApiResponse.success(resultData, TimingAspect.getAndClearExecutionTime()));
            
        } catch (Exception e) {
            log.error("Error in nearby search for collection '{}'", key, e);
            return ResponseEntity.internalServerError().body(ApiResponse.error(e.getMessage()));
        }
    }
    
    /**
     * Advanced NEARBY with complex filter conditions
     * HTTP: POST /api/v1/keys/{key}/nearby/filter
     */
    @PostMapping("/keys/{key}/nearby/filter")
    @Timed("Nearby search with filter operation")
    public ResponseEntity<ApiResponse<SearchResultData>> nearbyWithFilter(
            @PathVariable String key,
            @RequestBody NearbySearchParam param) {
        
        // Validate center point
        if (!param.hasValidCenterPoint()) {
            return ResponseEntity.badRequest().body(ApiResponse.error("Valid center point is required"));
        }
        
        Point centerPoint = param.getCenterPoint();
        double lat = centerPoint.getY();
        double lon = centerPoint.getX();
        
        log.debug("Starting nearby search with complex filter for collection '{}' at ({},{}) with radius {}", 
                 key, lat, lon, param.getRadius());
        
        try {
            FilterCondition filter = null;
            if (param.getFilterRequest() != null) {
                try {
                    filter = param.getFilterRequest().toFilterCondition();
                } catch (Exception e) {
                    return ResponseEntity.badRequest().body(ApiResponse.error("Invalid filter: " + e.getMessage()));
                }
            } else if (param.getFilter() != null) {
                try {
                    filter = parseSimpleFilter(param.getFilter());
                } catch (Exception e) {
                    return ResponseEntity.badRequest().body(ApiResponse.error("Invalid simple filter: " + e.getMessage()));
                }
            }
            
            List<SearchResult> results = tile38Service.nearby(key, lat, lon, param.getRadius(), filter);
            
            log.debug("Completed nearby search with complex filter for collection '{}', found {} results", 
                     key, results.size());
            
            SearchResultData resultData = SearchResultData.builder()
                    .count(results.size())
                    .objects(results)
                    .build();
                    
            return ResponseEntity.ok(ApiResponse.success(resultData, TimingAspect.getAndClearExecutionTime()));
            
        } catch (Exception e) {
            log.error("Error in nearby search with complex filter for collection '{}'", key, e);
            return ResponseEntity.internalServerError().body(ApiResponse.error(e.getMessage()));
        }
    }

    
    /**
     * KEYS pattern
     * HTTP: GET /api/v1/keys
     */
    @GetMapping("/keys")
    @Timed("Get keys operation")
    public ResponseEntity<ApiResponse<CollectionResult>> getKeys() {
        log.debug("Getting keys");
        
        List<String> keys = tile38Service.keys();
        
        log.debug("Completed getting keys, found {} keys", keys.size());
        
        CollectionResult result = CollectionResult.builder()
                .keys(keys)
                .build();
        
        return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
    }
    
    /**
     * STATS
     * HTTP: GET /api/v1/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<String> getStats() {
        String stats = tile38Service.stats();
        return ResponseEntity.ok(stats);
    }
    
    /**
     * FLUSHDB
     * HTTP: POST /api/v1/flushdb
     */
    @PostMapping("/flushdb")
    @Timed("Flush database operation")
    public ResponseEntity<ApiResponse<ObjectResult>> flushDb() {
        log.debug("Flushing database");
        
        tile38Service.flushdb();
        
        log.debug("Completed flushing database");
        
        ObjectResult result = ObjectResult.builder().build();
        
        return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
    }
    
    /**
     * Bulk load objects for a collection
     * HTTP: POST /api/v1/keys/{key}/bulk
     */
    @PostMapping("/keys/{key}/bulk")
    @Timed("Bulk set operation")
    public ResponseEntity<ApiResponse<BulkOperationResult>> bulkSetObjects(
            @PathVariable String key,
            @RequestBody Map<String, Map<String, Object>> objects) {
        
        try {
            log.debug("Starting bulk set operation for collection '{}' with {} objects", key, objects.size());
            
            Map<String, Tile38Object> tile38Objects = new HashMap<>();
            
            for (Map.Entry<String, Map<String, Object>> entry : objects.entrySet()) {
                String id = entry.getKey();
                Map<String, Object> objData = entry.getValue();
                
                Double lat = (Double) objData.get("lat");
                Double lon = (Double) objData.get("lon");
                
                if (lat == null || lon == null) {
                    log.warn("Skipping object {} - missing lat/lon", id);
                    continue;
                }
                
                Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                
                Map<String, Object> fields = (Map<String, Object>) objData.get("fields");
                if (fields == null) {
                    fields = new HashMap<>();
                }
                
                // Parse KV data
                @SuppressWarnings("unchecked")
                Map<String, Object> tagsMap = (Map<String, Object>) objData.get("tags");
                @SuppressWarnings("unchecked")
                Map<String, Object> attributesMap = (Map<String, Object>) objData.get("attributes");
                
                KVData kvData = new KVData();
                if (tagsMap != null) {
                    tagsMap.forEach((k, v) -> kvData.setTag(k, v != null ? v.toString() : null));
                }
                if (attributesMap != null) {
                    attributesMap.forEach(kvData::setAttribute);
                }
                
                Tile38Object tile38Object = Tile38Object.builder()
                    .id(id)
                    .geometry(point)
                    .fields(fields)
                    .kvData(kvData.isEmpty() ? null : kvData)
                    .timestamp(System.currentTimeMillis())
                    .build();
                
                tile38Objects.put(id, tile38Object);
            }
            
            tile38Service.bulkSet(key, tile38Objects);
            
            log.debug("Completed bulk set operation for collection '{}', loaded {} objects", key, tile38Objects.size());
            
            BulkOperationResult result = BulkOperationResult.builder()
                    .objectsLoaded(tile38Objects.size())
                    .build();
            
            return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
            
        } catch (Exception e) {
            log.error("Error in bulk set operation", e);
            return ResponseEntity.badRequest().body(ApiResponse.error(e.getMessage()));
        }
    }
    
    /**
     * Load data from JSON file
     * HTTP: POST /api/v1/load/json?filePath=/path/to/file.json
     */
    @PostMapping("/load/json")
    public ResponseEntity<Map<String, Object>> loadFromJson(@RequestParam String filePath) {
        try {
            CompletableFuture<DataLoader.LoadResult> future = dataLoader.loadFromJson(filePath);
            DataLoader.LoadResult result = future.get(); // Wait for completion
            
            Map<String, Object> response = new HashMap<>();
            response.put("ok", result.isSuccess());
            response.put("records_loaded", result.getRecordsLoaded());
            response.put("duration_ms", result.getDurationMs());
            response.put("message", result.getMessage());
            
            return result.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
            
        } catch (Exception e) {
            log.error("Error loading from JSON", e);
            Map<String, Object> response = new HashMap<>();
            response.put("ok", false);
            response.put("err", e.getMessage());
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * Load data from CSV file
     * HTTP: POST /api/v1/load/csv?filePath=/path/to/file.csv
     */
    @PostMapping("/load/csv")
    public ResponseEntity<Map<String, Object>> loadFromCsv(@RequestParam String filePath) {
        try {
            CompletableFuture<DataLoader.LoadResult> future = dataLoader.loadFromCsv(filePath);
            DataLoader.LoadResult result = future.get(); // Wait for completion
            
            Map<String, Object> response = new HashMap<>();
            response.put("ok", result.isSuccess());
            response.put("records_loaded", result.getRecordsLoaded());
            response.put("duration_ms", result.getDurationMs());
            response.put("message", result.getMessage());
            
            return result.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
            
        } catch (Exception e) {
            log.error("Error loading from CSV", e);
            Map<String, Object> response = new HashMap<>();
            response.put("ok", false);
            response.put("err", e.getMessage());
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * Generate test data for performance testing
     * HTTP: POST /api/v1/generate/test-data
     */
    @PostMapping("/generate/test-data")
    public ResponseEntity<Map<String, Object>> generateTestData(
            @RequestParam String collection,
            @RequestParam(defaultValue = "100000") int records,
            @RequestParam(defaultValue = "30.0") double minLat,
            @RequestParam(defaultValue = "40.0") double maxLat,
            @RequestParam(defaultValue = "-120.0") double minLon,
            @RequestParam(defaultValue = "-110.0") double maxLon) {
        
        try {
            log.info("Starting test data generation: {} records for collection '{}'", records, collection);
            CompletableFuture<DataLoader.LoadResult> future = dataLoader.generateTestData(
                collection, records, minLat, maxLat, minLon, maxLon);
            DataLoader.LoadResult result = future.get(); // Wait for completion
            
            Map<String, Object> response = new HashMap<>();
            response.put("ok", result.isSuccess());
            response.put("records_generated", result.getRecordsLoaded());
            response.put("duration_ms", result.getDurationMs());
            response.put("message", result.getMessage());
            
            return result.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
            
        } catch (Exception e) {
            log.error("Error generating test data", e);
            Map<String, Object> response = new HashMap<>();
            response.put("ok", false);
            response.put("err", e.getMessage());
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * Update KV data for an existing object
     * HTTP: PUT /api/v1/keys/{key}/objects/{id}/kv
     * Pure ID-based KV update - polygon-centric architecture
     */
    @PutMapping("/keys/{key}/objects/{id}/kv")
    @Timed("Update KV data operation")
    public ResponseEntity<ApiResponse<ObjectResult>> updateKVData(
            @PathVariable String key,
            @PathVariable String id,
            @RequestBody UpdateKVParam param) {
        
        try {
            log.debug("Updating KV data for object {}/{}", key, id);
            
            if (!param.hasValidKVData()) {
                return ResponseEntity.badRequest().body(ApiResponse.error("Valid KV data must be provided"));
            }
            
            boolean updated = tile38Service.updateKVData(key, id, param.getKvData());
            
            log.debug("Completed updating KV data for object {}/{}, updated: {}", key, id, updated);
            
            if (!updated) {
                return ResponseEntity.notFound().build();
            }
            
            ObjectResult result = ObjectResult.builder()
                    .updated(1)
                    .build();
                    
            return ResponseEntity.ok(ApiResponse.success(result, TimingAspect.getAndClearExecutionTime()));
            
        } catch (Exception e) {
            log.error("Error updating KV data for object {}/{}", key, id, e);
            return ResponseEntity.internalServerError().body(ApiResponse.error(e.getMessage()));
        }
    }
    
    /**
     * Parse simple filter format: "tag:key=value", "attr:key>value", etc.
     */
    private FilterCondition parseSimpleFilter(String filter) {
        String[] parts = filter.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Filter must be in format 'type:condition'");
        }
        
        String type = parts[0].toLowerCase();
        String condition = parts[1];
        
        FilterCondition.DataType dataType;
        if ("tag".equals(type)) {
            dataType = FilterCondition.DataType.TAG;
        } else if ("attr".equals(type) || "attribute".equals(type)) {
            dataType = FilterCondition.DataType.ATTRIBUTE;
        } else {
            throw new IllegalArgumentException("Type must be 'tag' or 'attr'");
        }
        
        // Parse condition (key=value, key>value, etc.)
        String key;
        String op;
        String value;
        
        if (condition.contains(">=")) {
            String[] condParts = condition.split(">=", 2);
            key = condParts[0].trim();
            op = "GREATER_EQUAL";
            value = condParts[1].trim();
        } else if (condition.contains("<=")) {
            String[] condParts = condition.split("<=", 2);
            key = condParts[0].trim();
            op = "LESS_EQUAL";
            value = condParts[1].trim();
        } else if (condition.contains("!=")) {
            String[] condParts = condition.split("!=", 2);
            key = condParts[0].trim();
            op = "NOT_EQUALS";
            value = condParts[1].trim();
        } else if (condition.contains("=")) {
            String[] condParts = condition.split("=", 2);
            key = condParts[0].trim();
            op = "EQUALS";
            value = condParts[1].trim();
        } else if (condition.contains(">")) {
            String[] condParts = condition.split(">", 2);
            key = condParts[0].trim();
            op = "GREATER_THAN";
            value = condParts[1].trim();
        } else if (condition.contains("<")) {
            String[] condParts = condition.split("<", 2);
            key = condParts[0].trim();
            op = "LESS_THAN";
            value = condParts[1].trim();
        } else {
            throw new IllegalArgumentException("Unsupported condition operator");
        }
        
        // Parse value (try to convert to number if possible)
        Object parsedValue = parseValue(value);
        
        return FilterCondition.builder()
                .key(key)
                .operator(FilterCondition.Operator.valueOf(op))
                .value(parsedValue)
                .dataType(dataType)
                .build();
    }
    
    /**
     * Parse value string to appropriate type
     */
    private Object parseValue(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        
        // Remove quotes if present
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        
        // Try to parse as number
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // Try boolean
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return Boolean.parseBoolean(value);
            }
            
            // Return as string
            return value;
        }
    }
}