package com.tile38.controller;

import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;

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
import java.time.Instant;

/**
 * HTTP REST API Controller for Tile38 operations
 * Provides HTTP endpoints equivalent to the original Tile38 commands
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
public class Tile38Controller {
    
    @Autowired
    private Tile38Service tile38Service;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    
    /**
     * SET key id POINT lat lon [FIELD name value ...] [EX seconds]
     * HTTP: POST /api/v1/keys/{key}/objects/{id}
     */
    @PostMapping("/keys/{key}/objects/{id}")
    public ResponseEntity<Map<String, Object>> setObject(
            @PathVariable String key,
            @PathVariable String id,
            @RequestBody Map<String, Object> request) {
        
        try {
            // Parse coordinates
            Double lat = (Double) request.get("lat");
            Double lon = (Double) request.get("lon");
            
            if (lat == null || lon == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "lat and lon are required"));
            }
            
            // Create geometry
            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
            
            // Parse fields
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) request.get("fields");
            
            // Parse expiration
            Long exSeconds = request.get("ex") != null ? ((Number) request.get("ex")).longValue() : null;
            Instant expireAt = exSeconds != null ? Instant.now().plusSeconds(exSeconds) : null;
            
            // Create object
            Tile38Object object = Tile38Object.builder()
                    .id(id)
                    .geometry(point)
                    .fields(fields)
                    .expireAt(expireAt)
                    .timestamp(System.currentTimeMillis())
                    .build();
            
            // Store object
            tile38Service.set(key, id, object);
            
            Map<String, Object> response = new HashMap<>();
            response.put("ok", true);
            response.put("elapsed", "0.001s"); // Mock elapsed time
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error setting object", e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * GET key id
     * HTTP: GET /api/v1/keys/{key}/objects/{id}
     */
    @GetMapping("/keys/{key}/objects/{id}")
    public ResponseEntity<Map<String, Object>> getObject(
            @PathVariable String key,
            @PathVariable String id) {
        
        Optional<Tile38Object> object = tile38Service.get(key, id);
        
        if (object.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("object", object.get());
        response.put("elapsed", "0.001s");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * DEL key id
     * HTTP: DELETE /api/v1/keys/{key}/objects/{id}
     */
    @DeleteMapping("/keys/{key}/objects/{id}")
    public ResponseEntity<Map<String, Object>> deleteObject(
            @PathVariable String key,
            @PathVariable String id) {
        
        boolean deleted = tile38Service.del(key, id);
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("deleted", deleted ? 1 : 0);
        response.put("elapsed", "0.001s");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * DROP key
     * HTTP: DELETE /api/v1/keys/{key}
     */
    @DeleteMapping("/keys/{key}")
    public ResponseEntity<Map<String, Object>> dropCollection(@PathVariable String key) {
        boolean dropped = tile38Service.drop(key);
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("dropped", dropped ? 1 : 0);
        response.put("elapsed", "0.001s");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * BOUNDS key
     * HTTP: GET /api/v1/keys/{key}/bounds
     */
    @GetMapping("/keys/{key}/bounds")
    public ResponseEntity<Map<String, Object>> getBounds(@PathVariable String key) {
        Optional<Bounds> bounds = tile38Service.bounds(key);
        
        if (bounds.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("bounds", bounds.get());
        response.put("elapsed", "0.001s");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * NEARBY key POINT lat lon radius
     * HTTP: GET /api/v1/keys/{key}/nearby?lat={lat}&lon={lon}&radius={radius}
     */
    @GetMapping("/keys/{key}/nearby")
    public ResponseEntity<Map<String, Object>> nearby(
            @PathVariable String key,
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam double radius) {
        
        List<SearchResult> results = tile38Service.nearby(key, lat, lon, radius);
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("count", results.size());
        response.put("objects", results);
        response.put("elapsed", "0.001s");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * KEYS pattern
     * HTTP: GET /api/v1/keys
     */
    @GetMapping("/keys")
    public ResponseEntity<Map<String, Object>> getKeys() {
        List<String> keys = tile38Service.keys();
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("keys", keys);
        response.put("elapsed", "0.001s");
        
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
        tile38Service.flushdb();
        
        Map<String, Object> response = new HashMap<>();
        response.put("ok", true);
        response.put("elapsed", "0.001s");
        
        return ResponseEntity.ok(response);
    }
}