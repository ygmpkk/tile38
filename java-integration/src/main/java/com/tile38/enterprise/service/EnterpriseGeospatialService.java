package com.tile38.enterprise.service;

import com.tile38.enterprise.client.Tile38Client;
import com.tile38.enterprise.model.GeospatialObject;
import com.tile38.enterprise.model.Point;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Enterprise service layer for Tile38 geospatial operations with caching, metrics, and async support
 */
@Service
public class EnterpriseGeospatialService {
    
    private static final Logger logger = LoggerFactory.getLogger(EnterpriseGeospatialService.class);
    
    @Autowired
    Tile38Client tile38Client;
    
    private final Counter setPointCounter;
    private final Counter getObjectCounter;
    private final Counter nearbyQueryCounter;
    private final Counter withinQueryCounter;
    private final Timer queryTimer;
    
    public EnterpriseGeospatialService(MeterRegistry meterRegistry) {
        this.setPointCounter = Counter.builder("tile38.operations.set_point")
                .description("Number of set point operations")
                .register(meterRegistry);
        
        this.getObjectCounter = Counter.builder("tile38.operations.get_object")
                .description("Number of get object operations")
                .register(meterRegistry);
        
        this.nearbyQueryCounter = Counter.builder("tile38.operations.nearby_query")
                .description("Number of nearby queries")
                .register(meterRegistry);
        
        this.withinQueryCounter = Counter.builder("tile38.operations.within_query")
                .description("Number of within queries")
                .register(meterRegistry);
        
        this.queryTimer = Timer.builder("tile38.operations.query_duration")
                .description("Query execution time")
                .register(meterRegistry);
    }
    
    /**
     * Store a geospatial object with enterprise features
     */
    public void storeObject(String collection, String id, Point point, Map<String, Object> fields) {
        Timer.Sample sample = Timer.start();
        try {
            // Add enterprise metadata
            if (fields != null) {
                fields.put("created_at", System.currentTimeMillis());
                fields.put("enterprise_version", "1.0.0");
            }
            
            tile38Client.setPoint(collection, id, point, fields);
            setPointCounter.increment();
            
            logger.info("Stored object {} in collection {} at coordinates [{}, {}]", 
                id, collection, point.getLatitude(), point.getLongitude());
            
        } finally {
            sample.stop(queryTimer);
        }
    }
    
    /**
     * Retrieve a geospatial object with caching
     */
    @Cacheable(value = "geospatial-objects", key = "#collection + ':' + #id")
    public GeospatialObject getObject(String collection, String id) {
        Timer.Sample sample = Timer.start();
        try {
            GeospatialObject obj = tile38Client.get(collection, id);
            getObjectCounter.increment();
            
            logger.debug("Retrieved object {} from collection {}", id, collection);
            return obj;
            
        } finally {
            sample.stop(queryTimer);
        }
    }
    
    /**
     * Find nearby objects with caching and metrics
     */
    @Cacheable(value = "nearby-queries", key = "#collection + ':' + #point.latitude + ':' + #point.longitude + ':' + #radius")
    public List<GeospatialObject> findNearby(String collection, Point point, double radius, String unit) {
        Timer.Sample sample = Timer.start();
        try {
            List<GeospatialObject> objects = tile38Client.nearby(collection, point, radius, unit);
            nearbyQueryCounter.increment();
            
            logger.info("Found {} objects near [{}, {}] within {} {}", 
                objects.size(), point.getLatitude(), point.getLongitude(), radius, unit);
            
            return objects;
            
        } finally {
            sample.stop(queryTimer);
        }
    }
    
    /**
     * Find objects within bounding box with caching
     */
    @Cacheable(value = "within-queries", key = "#collection + ':' + #minLat + ':' + #minLon + ':' + #maxLat + ':' + #maxLon")
    public List<GeospatialObject> findWithin(String collection, double minLat, double minLon, 
                                           double maxLat, double maxLon) {
        Timer.Sample sample = Timer.start();
        try {
            List<GeospatialObject> objects = tile38Client.within(collection, minLat, minLon, maxLat, maxLon);
            withinQueryCounter.increment();
            
            logger.info("Found {} objects within bounds [{}, {}] to [{}, {}]", 
                objects.size(), minLat, minLon, maxLat, maxLon);
            
            return objects;
            
        } finally {
            sample.stop(queryTimer);
        }
    }
    
    /**
     * Async batch processing of geospatial objects
     */
    @Async
    public CompletableFuture<Void> batchStoreObjects(String collection, 
                                                   Map<String, GeospatialObject> objects) {
        logger.info("Starting batch store of {} objects in collection {}", objects.size(), collection);
        
        for (Map.Entry<String, GeospatialObject> entry : objects.entrySet()) {
            String id = entry.getKey();
            GeospatialObject obj = entry.getValue();
            
            if (obj.getGeometry() instanceof Point) {
                Point point = (Point) obj.getGeometry();
                storeObject(collection, id, point, obj.getFields());
            }
        }
        
        logger.info("Completed batch store of {} objects", objects.size());
        return CompletableFuture.completedFuture(null);
    }
    
    /**
     * Delete object and clear cache
     */
    @CacheEvict(value = {"geospatial-objects", "nearby-queries", "within-queries"}, allEntries = true)
    public boolean deleteObject(String collection, String id) {
        boolean deleted = tile38Client.delete(collection, id);
        if (deleted) {
            logger.info("Deleted object {} from collection {}", id, collection);
        }
        return deleted;
    }
    
    /**
     * Health check for Tile38 server
     */
    public boolean isHealthy() {
        return tile38Client.ping();
    }
}