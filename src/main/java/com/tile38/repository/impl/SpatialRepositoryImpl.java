package com.tile38.repository.impl;

import com.tile38.repository.SpatialRepository;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.FilterCondition;
import com.tile38.model.KVData;

import org.springframework.stereotype.Repository;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.index.ItemVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of SpatialRepository using JTS STRtree for spatial indexing
 * Optimized for million-level data with bulk loading support
 */
@Repository
public class SpatialRepositoryImpl implements SpatialRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(SpatialRepositoryImpl.class);
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    
    // Optimized node capacity for million-level data
    private static final int STRTREE_NODE_CAPACITY = 25;
    
    // Spatial indexes per collection
    private final Map<String, STRtree> spatialIndexes = new ConcurrentHashMap<>();
    
    // Single source of truth for object storage per collection
    private final Map<String, Map<String, Tile38Object>> objectStorage = new ConcurrentHashMap<>();
    
    @Override
    public void index(String key, String id, Tile38Object object) {
        if (object.getGeometry() == null) {
            return;
        }
        
        STRtree index = spatialIndexes.computeIfAbsent(key, k -> new STRtree(STRTREE_NODE_CAPACITY));
        Map<String, Tile38Object> storage = objectStorage.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        
        // Remove existing entry if present
        remove(key, id);
        
        // Clean up expired objects during indexing
        removeExpiredObjects(key, storage, index);
        
        // Add to spatial index
        index.insert(object.getGeometry().getEnvelopeInternal(), id);
        
        // Store object
        storage.put(id, object);
    }
    
    @Override
    public void bulkIndex(String key, Map<String, Tile38Object> objects) {
        if (objects == null || objects.isEmpty()) {
            return;
        }
        
        logger.info("Starting bulk index for collection '{}' with {} objects", key, objects.size());
        long startTime = System.currentTimeMillis();
        
        // Create new optimized spatial index for bulk loading
        STRtree newIndex = new STRtree(STRTREE_NODE_CAPACITY);
        Map<String, Tile38Object> newStorage = new ConcurrentHashMap<>();
        
        // Filter out expired and invalid objects
        Map<String, Tile38Object> validObjects = new HashMap<>();
        for (Map.Entry<String, Tile38Object> entry : objects.entrySet()) {
            Tile38Object object = entry.getValue();
            if (object.getGeometry() != null && !object.isExpired()) {
                validObjects.put(entry.getKey(), object);
            }
        }
        
        logger.info("Filtered {} valid objects from {} total for collection '{}'", 
                   validObjects.size(), objects.size(), key);
        
        // Bulk insert into spatial index
        for (Map.Entry<String, Tile38Object> entry : validObjects.entrySet()) {
            newIndex.insert(entry.getValue().getGeometry().getEnvelopeInternal(), entry.getKey());
            newStorage.put(entry.getKey(), entry.getValue());
        }
        
        // Build the spatial index (this optimizes the tree structure)
        newIndex.build();
        
        // Replace old index and storage atomically
        spatialIndexes.put(key, newIndex);
        objectStorage.put(key, newStorage);
        
        long endTime = System.currentTimeMillis();
        logger.info("Completed bulk index for collection '{}' in {}ms", key, (endTime - startTime));
    }
    
    @Override
    public Optional<Tile38Object> get(String key, String id) {
        Map<String, Tile38Object> storage = objectStorage.get(key);
        if (storage == null) {
            return Optional.empty();
        }
        
        Tile38Object object = storage.get(id);
        if (object != null && object.isExpired()) {
            // Remove expired object
            remove(key, id);
            return Optional.empty();
        }
        
        return Optional.ofNullable(object);
    }
    
    @Override
    public Set<String> keys() {
        return new HashSet<>(objectStorage.keySet());
    }
    
    @Override
    public Map<String, Tile38Object> getAll(String key) {
        Map<String, Tile38Object> storage = objectStorage.get(key);
        if (storage == null) {
            return Collections.emptyMap();
        }
        
        // Filter out expired objects
        Map<String, Tile38Object> result = new HashMap<>();
        for (Map.Entry<String, Tile38Object> entry : storage.entrySet()) {
            if (!entry.getValue().isExpired()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        
        return result;
    }
    
    @Override
    public void remove(String key, String id) {
        STRtree index = spatialIndexes.get(key);
        Map<String, Tile38Object> storage = objectStorage.get(key);
        
        if (index != null && storage != null) {
            Tile38Object object = storage.get(id);
            if (object != null && object.getGeometry() != null) {
                index.remove(object.getGeometry().getEnvelopeInternal(), id);
                storage.remove(id);
            }
        }
    }
    
    @Override
    public void drop(String key) {
        spatialIndexes.remove(key);
        objectStorage.remove(key);
        logger.info("Dropped collection: {}", key);
    }
    
    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius) {
        return nearby(key, lat, lon, radius, null);
    }
    
    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius, FilterCondition filter) {
        STRtree index = spatialIndexes.get(key);
        Map<String, Tile38Object> storage = objectStorage.get(key);
        
        if (index == null || storage == null) {
            return Collections.emptyList();
        }
        
        // Create search geometry (circle approximated as square for envelope)
        double latDelta = radius / 111000.0; // Rough conversion: 1 degree lat ≈ 111km
        double lonDelta = radius / (111000.0 * Math.cos(Math.toRadians(lat)));
        
        Point center = geometryFactory.createPoint(new Coordinate(lon, lat));
        List<SearchResult> results = new ArrayList<>();
        
        // Query spatial index
        index.query(center.buffer(Math.max(latDelta, lonDelta)).getEnvelopeInternal(), new ItemVisitor() {
            @Override
            public void visitItem(Object item) {
                String id = (String) item;
                Tile38Object object = storage.get(id);
                if (object != null && !object.isExpired()) {
                    double distance = center.distance(object.getGeometry());
                    // Convert distance from degrees to meters (approximation)
                    double distanceMeters = distance * 111000.0;
                    
                    if (distanceMeters <= radius && object.matchesFilter(filter)) {
                        results.add(SearchResult.builder()
                                              .id(id)
                                              .object(object)
                                              .distance(distanceMeters)
                                              .build());
                    }
                }
            }
        });
        
        // Sort by distance
        results.sort(Comparator.comparingDouble(SearchResult::getDistance));
        
        return results;
    }
    
    @Override
    public List<SearchResult> within(String key, Geometry geometry) {
        return within(key, geometry, null);
    }
    
    @Override
    public List<SearchResult> within(String key, Geometry geometry, FilterCondition filter) {
        STRtree index = spatialIndexes.get(key);
        Map<String, Tile38Object> storage = objectStorage.get(key);
        
        if (index == null || storage == null) {
            return Collections.emptyList();
        }
        
        List<SearchResult> results = new ArrayList<>();
        
        index.query(geometry.getEnvelopeInternal(), new ItemVisitor() {
            @Override
            public void visitItem(Object item) {
                String id = (String) item;
                Tile38Object object = storage.get(id);
                if (object != null && !object.isExpired() && 
                    geometry.contains(object.getGeometry()) && 
                    object.matchesFilter(filter)) {
                    results.add(SearchResult.builder()
                                          .id(id)
                                          .object(object)
                                          .withinArea(true)
                                          .build());
                }
            }
        });
        
        return results;
    }
    
    @Override
    public List<SearchResult> intersects(String key, Geometry geometry) {
        return intersects(key, geometry, null);
    }
    
    @Override
    public List<SearchResult> intersects(String key, Geometry geometry, FilterCondition filter) {
        STRtree index = spatialIndexes.get(key);
        Map<String, Tile38Object> storage = objectStorage.get(key);
        
        if (index == null || storage == null) {
            return Collections.emptyList();
        }
        
        List<SearchResult> results = new ArrayList<>();
        
        index.query(geometry.getEnvelopeInternal(), new ItemVisitor() {
            @Override
            public void visitItem(Object item) {
                String id = (String) item;
                Tile38Object object = storage.get(id);
                if (object != null && !object.isExpired() && 
                    geometry.intersects(object.getGeometry()) && 
                    object.matchesFilter(filter)) {
                    results.add(SearchResult.builder()
                                          .id(id)
                                          .object(object)
                                          .build());
                }
            }
        });
        
        return results;
    }
    
    @Override
    public long getTotalObjectCount() {
        return objectStorage.values().stream()
                          .mapToLong(storage -> storage.values().stream()
                                                      .mapToLong(obj -> obj.isExpired() ? 0 : 1)
                                                      .sum())
                          .sum();
    }
    
    @Override
    public long getObjectCount(String key) {
        Map<String, Tile38Object> storage = objectStorage.get(key);
        if (storage == null) {
            return 0;
        }
        
        return storage.values().stream()
                     .mapToLong(obj -> obj.isExpired() ? 0 : 1)
                     .sum();
    }
    
    @Override
    public void flushAll() {
        spatialIndexes.clear();
        objectStorage.clear();
        logger.info("Flushed all collections");
    }
    
    @Override
    public boolean updateKVData(String key, String id, KVData kvData) {
        Map<String, Tile38Object> storage = objectStorage.get(key);
        if (storage == null) {
            return false;
        }
        
        Tile38Object object = storage.get(id);
        if (object == null || object.isExpired()) {
            return false;
        }
        
        // Update KV data without affecting spatial indexing
        object.updateKVData(kvData);
        object.setTimestamp(System.currentTimeMillis());
        
        logger.debug("Updated KV data for object {}/{}", key, id);
        return true;
    }
    
    @Override
    public List<SearchResult> scan(String key, FilterCondition filter, int limit, int offset) {
        Map<String, Tile38Object> storage = objectStorage.get(key);
        if (storage == null || storage.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<SearchResult> allResults = new ArrayList<>();
        for (Tile38Object object : storage.values()) {
            // Apply filter if provided
            if (filter == null || filter.matches(object)) {
                SearchResult result = SearchResult.builder()
                        .object(object)
                        .distance(0.0) // No distance for scan operations
                        .build();
                allResults.add(result);
            }
        }
        
        // Apply pagination
        int startIndex = Math.max(0, offset);
        int endIndex = limit > 0 ? Math.min(allResults.size(), startIndex + limit) : allResults.size();
        
        if (startIndex >= allResults.size()) {
            return new ArrayList<>();
        }
        
        List<SearchResult> paginatedResults = allResults.subList(startIndex, endIndex);
        logger.debug("Scan collection '{}': found {} objects (offset: {}, limit: {})", 
                    key, paginatedResults.size(), offset, limit);
        
        return paginatedResults;
    }
    
    /**
     * Periodic cleanup of expired objects
     */
    private void removeExpiredObjects(String key, Map<String, Tile38Object> storage, STRtree index) {
        if (storage.size() % 1000 == 0) { // Clean up every 1000 inserts
            List<String> expiredIds = new ArrayList<>();
            for (Map.Entry<String, Tile38Object> entry : storage.entrySet()) {
                if (entry.getValue().isExpired()) {
                    expiredIds.add(entry.getKey());
                }
            }
            
            if (!expiredIds.isEmpty()) {
                logger.debug("Cleaning up {} expired objects from collection '{}'", expiredIds.size(), key);
                for (String expiredId : expiredIds) {
                    remove(key, expiredId);
                }
            }
        }
    }
}