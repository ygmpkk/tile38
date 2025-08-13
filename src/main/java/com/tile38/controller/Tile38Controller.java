package com.tile38.controller;

import com.tile38.service.Tile38Service;
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
     * SET key id POINT lat lon [FIELD name value ...] [EX seconds]
     * HTTP: POST /api/v1/keys/{key}/objects/{id}
     * Supporting both SetObjectParam and legacy Map for backward compatibility
     */
    @PostMapping("/keys/{key}/objects/{id}")
    public ResponseEntity<?> setObject(
            @PathVariable String key,
            @PathVariable String id,
            @RequestBody Object request) {
        
        try {
            SetObjectParam param;
            boolean isLegacy = false;
            
            // Handle both structured param and legacy Map
            if (request instanceof SetObjectParam) {
                param = (SetObjectParam) request;
            } else if (request instanceof Map) {
                isLegacy = true;
                @SuppressWarnings("unchecked")
                Map<String, Object> requestMap = (Map<String, Object>) request;
                
                Double lat = (Double) requestMap.get("lat");
                Double lon = (Double) requestMap.get("lon");
                
                if (lat == null || lon == null) {
                    if (isLegacy) {
                        return ResponseEntity.badRequest().body(Map.of("error", "lat and lon are required"));
                    } else {
                        return ResponseEntity.badRequest().body(ApiResponse.error("lat and lon are required"));
                    }
                }
                
                // Create geometry from lat/lon coordinates
                Point geometry = geometryFactory.createPoint(new Coordinate(lon, lat));
                
                // Create KVData from legacy fields if present
                KVData kvData = new KVData();
                Map<String, Object> tags = (Map<String, Object>) requestMap.get("tags");
                Map<String, Object> attributes = (Map<String, Object>) requestMap.get("attributes");
                
                if (tags != null) {
                    tags.forEach((k, v) -> kvData.setTag(k, v != null ? v.toString() : null));
                }
                if (attributes != null) {
                    attributes.forEach(kvData::setAttribute);
                }
                
                param = SetObjectParam.builder()
                    .geometry(geometry)
                    .fields((Map<String, Object>) requestMap.get("fields"))
                    .kvData(kvData.isEmpty() ? null : kvData)
                    .ex(requestMap.get("ex") != null ? ((Number) requestMap.get("ex")).longValue() : null)
                    .build();
            } else {
                return ResponseEntity.badRequest().body(Map.of("error", "Invalid request format"));
            }
            
            // Validate geometry
            if (!param.hasValidGeometry()) {
                String errorMsg = "Valid geometry is required";
                if (isLegacy) {
                    return ResponseEntity.badRequest().body(Map.of("error", errorMsg));
                } else {
                    return ResponseEntity.badRequest().body(ApiResponse.error(errorMsg));
                }
            }
            
            log.debug("Setting polygon object {}/{}", key, id);
            long startTime = System.currentTimeMillis();
            
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
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Completed setting object {}/{} in {}ms", key, id, duration);
            
            // Return appropriate response format
            if (isLegacy) {
                Map<String, Object> response = new HashMap<>();
                response.put("ok", true);
                response.put("elapsed", duration + "ms");
                return ResponseEntity.ok(response);
            } else {
                ObjectResult result = ObjectResult.builder().build();
                return ResponseEntity.ok(ApiResponse.success(result, duration + "ms"));
            }
            
        } catch (Exception e) {
            log.error("Error setting object {}/{}", key, id, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * SET key id - Overloaded method for legacy tests
     */
    public ResponseEntity<Map<String, Object>> setObject(String key, String id, Map<String, Object> request) {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response = (ResponseEntity<Map<String, Object>>) setObject(key, id, (Object) request);
        return response;
    }
    
    /**
     * GET key id
     * HTTP: GET /api/v1/keys/{key}/objects/{id}
     * Supporting both structured response and legacy format
     */
    @GetMapping("/keys/{key}/objects/{id}")
    public ResponseEntity<?> getObject(
            @PathVariable String key,
            @PathVariable String id,
            @RequestParam(defaultValue = "legacy", required = false) String format) {
        
        log.debug("Getting object {}/{}", key, id);
        long startTime = System.currentTimeMillis();
        
        Optional<Tile38Object> object = tile38Service.get(key, id);
        
        long duration = System.currentTimeMillis() - startTime;
        log.debug("Completed getting object {}/{} in {}ms, found: {}", key, id, duration, object.isPresent());
        
        if (object.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        if ("structured".equals(format)) {
            ObjectResult result = ObjectResult.builder()
                    .object(object.get())
                    .found(true)
                    .build();
                    
            return ResponseEntity.ok(ApiResponse.success(result, duration + "ms"));
        } else {
            // Legacy format for backward compatibility
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("object", object.get());
            response.put("elapsed", duration + "ms");
            
            return ResponseEntity.ok(response);
        }
    }
    
    /**
     * GET key id - Overloaded method without format parameter for tests
     */
    public ResponseEntity<Map<String, Object>> getObject(String key, String id) {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response = (ResponseEntity<Map<String, Object>>) getObject(key, id, "legacy");
        return response;
    }
    
    /**
     * DEL key id
     * HTTP: DELETE /api/v1/keys/{key}/objects/{id}
     * Supporting both structured response and legacy format
     */
    @DeleteMapping("/keys/{key}/objects/{id}")
    public ResponseEntity<?> deleteObject(
            @PathVariable String key,
            @PathVariable String id,
            @RequestParam(defaultValue = "legacy", required = false) String format) {
        
        log.debug("Deleting object {}/{}", key, id);
        long startTime = System.currentTimeMillis();
        
        boolean deleted = tile38Service.del(key, id);
        
        long duration = System.currentTimeMillis() - startTime;
        log.debug("Completed deleting object {}/{} in {}ms, deleted: {}", key, id, duration, deleted);
        
        if ("structured".equals(format)) {
            ObjectResult result = ObjectResult.builder()
                    .deleted(deleted ? 1 : 0)
                    .build();
                    
            return ResponseEntity.ok(ApiResponse.success(result, duration + "ms"));
        } else {
            // Legacy format for backward compatibility
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("deleted", deleted ? 1 : 0);
            response.put("elapsed", duration + "ms");
            
            return ResponseEntity.ok(response);
        }
    }
    
    /**
     * DEL key id - Overloaded method without format parameter for tests
     */
    public ResponseEntity<Map<String, Object>> deleteObject(String key, String id) {
        @SuppressWarnings("unchecked")
        ResponseEntity<Map<String, Object>> response = (ResponseEntity<Map<String, Object>>) deleteObject(key, id, "legacy");
        return response;
    }
    
    /**
     * DROP key
     * HTTP: DELETE /api/v1/keys/{key}
     */
    @DeleteMapping("/keys/{key}")
    public ResponseEntity<ApiResponse<CollectionResult>> dropCollection(@PathVariable String key) {
        log.debug("Dropping collection '{}'", key);
        long startTime = System.currentTimeMillis();
        
        boolean dropped = tile38Service.drop(key);
        
        long duration = System.currentTimeMillis() - startTime;
        log.debug("Completed dropping collection '{}' in {}ms, dropped: {}", key, duration, dropped);
        
        CollectionResult result = CollectionResult.builder()
                .dropped(dropped ? 1 : 0)
                .build();
                
        return ResponseEntity.ok(ApiResponse.success(result, duration + "ms"));
    }
    
    /**
     * BOUNDS key
     * HTTP: GET /api/v1/keys/{key}/bounds
     */
    @GetMapping("/keys/{key}/bounds")
    public ResponseEntity<ApiResponse<CollectionResult>> getBounds(@PathVariable String key) {
        log.debug("Getting bounds for collection '{}'", key);
        long startTime = System.currentTimeMillis();
        
        Optional<Bounds> bounds = tile38Service.bounds(key);
        
        long duration = System.currentTimeMillis() - startTime;
        log.debug("Completed getting bounds for collection '{}' in {}ms, found: {}", key, duration, bounds.isPresent());
        
        if (bounds.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        CollectionResult result = CollectionResult.builder()
                .bounds(bounds.get())
                .build();
                
        return ResponseEntity.ok(ApiResponse.success(result, duration + "ms"));
    }
    
    /**
     * NEARBY key POINT lat lon radius
     * HTTP: GET /api/v1/keys/{key}/nearby?lat={lat}&lon={lon}&radius={radius}
     * Enhanced with optional KV filtering - returns legacy format for backward compatibility
     */
    @GetMapping("/keys/{key}/nearby")
    public ResponseEntity<Map<String, Object>> nearby(
            @PathVariable String key,
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam double radius,
            @RequestParam(required = false) String filter) {
        
        log.debug("Starting nearby search for collection '{}' at ({},{}) with radius {}", key, lat, lon, radius);
        long startTime = System.currentTimeMillis();
        
        try {
            FilterCondition filterCondition = null;
            if (filter != null && !filter.trim().isEmpty()) {
                try {
                    // Simple filter parsing: format like "tag:category=restaurant" or "attr:speed>50"
                    filterCondition = parseSimpleFilter(filter);
                } catch (Exception e) {
                    return ResponseEntity.badRequest().body(Map.of("error", "Invalid filter format: " + e.getMessage()));
                }
            }
            
            List<SearchResult> results = filterCondition != null ? 
                    tile38Service.nearby(key, lat, lon, radius, filterCondition) :
                    tile38Service.nearby(key, lat, lon, radius);
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Completed nearby search for collection '{}' in {}ms, found {} results", key, duration, results.size());
            
            // Return legacy format for backward compatibility
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("count", results.size());
            response.put("objects", results);
            response.put("elapsed", duration + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error in nearby search for collection '{}'", key, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * Advanced NEARBY with complex filter conditions
     * HTTP: POST /api/v1/keys/{key}/nearby/filter
     */
    @PostMapping("/keys/{key}/nearby/filter")
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
        long startTime = System.currentTimeMillis();
        
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
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Completed nearby search with complex filter for collection '{}' in {}ms, found {} results", 
                     key, duration, results.size());
            
            SearchResultData resultData = SearchResultData.builder()
                    .count(results.size())
                    .objects(results)
                    .build();
                    
            return ResponseEntity.ok(ApiResponse.success(resultData, duration + "ms"));
            
        } catch (Exception e) {
            log.error("Error in nearby search with complex filter for collection '{}'", key, e);
            return ResponseEntity.internalServerError().body(ApiResponse.error(e.getMessage()));
        }
    }
    
    /**
     * Advanced NEARBY with complex filter conditions - backward compatibility
     * HTTP: POST /api/v1/keys/{key}/nearby/filter?lat={lat}&lon={lon}&radius={radius}
     * For legacy tests that send location as query params and filter in body
     */
    @PostMapping(value = "/keys/{key}/nearby/filter", params = {"lat", "lon", "radius"})
    public ResponseEntity<?> nearbyWithFilterLegacy(
            @PathVariable String key,
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam double radius,
            @RequestBody FilterRequest filterRequest) {
        
        log.debug("Starting nearby search with complex filter (legacy) for collection '{}' at ({},{}) with radius {}", 
                 key, lat, lon, radius);
        long startTime = System.currentTimeMillis();
        
        try {
            FilterCondition filter = null;
            if (filterRequest != null) {
                try {
                    filter = filterRequest.toFilterCondition();
                } catch (Exception e) {
                    return ResponseEntity.badRequest().body(Map.of("error", "Invalid filter: " + e.getMessage()));
                }
            }
            
            List<SearchResult> results = tile38Service.nearby(key, lat, lon, radius, filter);
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Completed nearby search with complex filter (legacy) for collection '{}' in {}ms, found {} results", 
                     key, duration, results.size());
            
            // Return legacy format for backward compatibility  
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("count", results.size());
            response.put("objects", results);
            response.put("elapsed", duration + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error in nearby search with complex filter (legacy) for collection '{}'", key, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * KEYS pattern
     * HTTP: GET /api/v1/keys
     */
    @GetMapping("/keys")
    public ResponseEntity<Map<String, Object>> getKeys() {
        log.debug("Getting keys");
        long startTime = System.currentTimeMillis();
        
        List<String> keys = tile38Service.keys();
        
        long duration = System.currentTimeMillis() - startTime;
        log.debug("Completed getting keys in {}ms, found {} keys", duration, keys.size());
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("keys", keys);
        response.put("elapsed", duration + "ms");
        
        return ResponseEntity.ok(response);
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
    public ResponseEntity<Map<String, Object>> flushDb() {
        log.debug("Flushing database");
        long startTime = System.currentTimeMillis();
        
        tile38Service.flushdb();
        
        long duration = System.currentTimeMillis() - startTime;
        log.debug("Completed flushing database in {}ms", duration);
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("elapsed", duration + "ms");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Bulk load objects for a collection
     * HTTP: POST /api/v1/keys/{key}/bulk
     */
    @PostMapping("/keys/{key}/bulk")
    public ResponseEntity<Map<String, Object>> bulkSetObjects(
            @PathVariable String key,
            @RequestBody Map<String, Map<String, Object>> objects) {
        
        try {
            log.debug("Starting bulk set operation for collection '{}' with {} objects", key, objects.size());
            long startTime = System.currentTimeMillis();
            
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
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Completed bulk set operation for collection '{}' in {}ms, loaded {} objects", key, duration, tile38Objects.size());
            
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("objects_loaded", tile38Objects.size());
            response.put("elapsed", duration + "ms");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error in bulk set operation", e);
            Map<String, Object> response = new HashMap<>();
            response.put("ok", false);
            response.put("err", e.getMessage());
            
            return ResponseEntity.badRequest().body(response);
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
     * Returns legacy format for backward compatibility
     */
    @PutMapping("/keys/{key}/objects/{id}/kv")
    public ResponseEntity<Map<String, Object>> updateKVData(
            @PathVariable String key,
            @PathVariable String id,
            @RequestBody Object request) {
        
        try {
            log.debug("Updating KV data for object {}/{}", key, id);
            long startTime = System.currentTimeMillis();
            
            UpdateKVParam param;
            
            // Handle both structured param and legacy Map
            if (request instanceof UpdateKVParam) {
                param = (UpdateKVParam) request;
            } else if (request instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> requestMap = (Map<String, Object>) request;
                
                // Create KVData from legacy map structure
                KVData kvData = new KVData();
                Map<String, Object> tags = (Map<String, Object>) requestMap.get("tags");
                Map<String, Object> attributes = (Map<String, Object>) requestMap.get("attributes");
                
                if (tags != null) {
                    tags.forEach((k, v) -> kvData.setTag(k, v != null ? v.toString() : null));
                }
                if (attributes != null) {
                    attributes.forEach(kvData::setAttribute);
                }
                
                param = UpdateKVParam.builder()
                    .kvData(kvData)
                    .build();
            } else {
                return ResponseEntity.badRequest().body(Map.of("error", "Invalid request format"));
            }
            
            if (!param.hasValidKVData()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Valid KV data must be provided"));
            }
            
            boolean updated = tile38Service.updateKVData(key, id, param.getKvData());
            
            long duration = System.currentTimeMillis() - startTime;
            log.debug("Completed updating KV data for object {}/{} in {}ms, updated: {}", key, id, duration, updated);
            
            if (!updated) {
                return ResponseEntity.notFound().build();
            }
            
            // Return legacy format for backward compatibility
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("updated", 1);
            response.put("elapsed", duration + "ms");
                    
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error updating KV data for object {}/{}", key, id, e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
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