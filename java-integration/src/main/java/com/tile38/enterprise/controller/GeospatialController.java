package com.tile38.enterprise.controller;

import com.tile38.enterprise.model.GeospatialObject;
import com.tile38.enterprise.model.Point;
import com.tile38.enterprise.service.EnterpriseGeospatialService;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.util.List;
import java.util.Map;

/**
 * REST API for enterprise geospatial operations
 */
@RestController
@RequestMapping("/api/v1/geospatial")
@Validated
public class GeospatialController {
    
    @Autowired
    private EnterpriseGeospatialService geospatialService;
    
    @PostMapping("/collections/{collection}/objects/{id}/point")
    public ResponseEntity<String> setPoint(
            @PathVariable @NotBlank String collection,
            @PathVariable @NotBlank String id,
            @RequestParam 
            @DecimalMin(value = "-90.0") @DecimalMax(value = "90.0") double latitude,
            @RequestParam 
            @DecimalMin(value = "-180.0") @DecimalMax(value = "180.0") double longitude,
            @RequestParam(required = false) Double altitude,
            @RequestBody(required = false) Map<String, Object> fields) {
        
        Point point = altitude != null ? 
            new Point(latitude, longitude, altitude) : 
            new Point(latitude, longitude);
        
        geospatialService.storeObject(collection, id, point, fields);
        return ResponseEntity.ok("Point stored successfully");
    }
    
    @GetMapping("/collections/{collection}/objects/{id}")
    public ResponseEntity<GeospatialObject> getObject(
            @PathVariable @NotBlank String collection,
            @PathVariable @NotBlank String id) {
        
        GeospatialObject obj = geospatialService.getObject(collection, id);
        return obj != null ? ResponseEntity.ok(obj) : ResponseEntity.notFound().build();
    }
    
    @GetMapping("/collections/{collection}/nearby")
    public ResponseEntity<List<GeospatialObject>> findNearby(
            @PathVariable @NotBlank String collection,
            @RequestParam 
            @DecimalMin(value = "-90.0") @DecimalMax(value = "90.0") double latitude,
            @RequestParam 
            @DecimalMin(value = "-180.0") @DecimalMax(value = "180.0") double longitude,
            @RequestParam @Positive double radius,
            @RequestParam(defaultValue = "m") String unit) {
        
        Point center = new Point(latitude, longitude);
        List<GeospatialObject> objects = geospatialService.findNearby(collection, center, radius, unit);
        return ResponseEntity.ok(objects);
    }
    
    @GetMapping("/collections/{collection}/within")
    public ResponseEntity<List<GeospatialObject>> findWithin(
            @PathVariable @NotBlank String collection,
            @RequestParam 
            @DecimalMin(value = "-90.0") @DecimalMax(value = "90.0") double minLat,
            @RequestParam 
            @DecimalMin(value = "-180.0") @DecimalMax(value = "180.0") double minLon,
            @RequestParam 
            @DecimalMin(value = "-90.0") @DecimalMax(value = "90.0") double maxLat,
            @RequestParam 
            @DecimalMin(value = "-180.0") @DecimalMax(value = "180.0") double maxLon) {
        
        List<GeospatialObject> objects = geospatialService.findWithin(collection, minLat, minLon, maxLat, maxLon);
        return ResponseEntity.ok(objects);
    }
    
    @DeleteMapping("/collections/{collection}/objects/{id}")
    public ResponseEntity<String> deleteObject(
            @PathVariable @NotBlank String collection,
            @PathVariable @NotBlank String id) {
        
        boolean deleted = geospatialService.deleteObject(collection, id);
        return deleted ? 
            ResponseEntity.ok("Object deleted successfully") : 
            ResponseEntity.notFound().build();
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        boolean healthy = geospatialService.isHealthy();
        return ResponseEntity.ok(Map.of(
            "status", healthy ? "UP" : "DOWN",
            "tile38_connection", healthy,
            "timestamp", System.currentTimeMillis()
        ));
    }
}