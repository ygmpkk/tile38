package com.tile38.service.impl;

import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.model.FilterCondition;
import com.tile38.model.KVData;
import com.tile38.repository.SpatialRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of Tile38Service using repository as single source of truth
 * Optimized for million-level data operations
 */
@Service
public class Tile38ServiceImpl implements Tile38Service {
    
    private static final Logger logger = LoggerFactory.getLogger(Tile38ServiceImpl.class);
    
    @Autowired
    private SpatialRepository spatialRepository;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    
    @Override
    public void set(String key, String id, Tile38Object object) {
        logger.debug("Setting object {}/{}", key, id);
        spatialRepository.index(key, id, object);
    }
    
    @Override
    public void bulkSet(String key, Map<String, Tile38Object> objects) {
        logger.info("Starting bulk set operation for collection '{}' with {} objects", key, objects.size());
        long startTime = System.currentTimeMillis();
        
        spatialRepository.bulkIndex(key, objects);
        
        long endTime = System.currentTimeMillis();
        logger.info("Completed bulk set operation for collection '{}' in {}ms", key, (endTime - startTime));
    }
    
    @Override
    public Optional<Tile38Object> get(String key, String id) {
        return spatialRepository.get(key, id);
    }
    
    @Override
    public boolean del(String key, String id) {
        Optional<Tile38Object> existing = spatialRepository.get(key, id);
        if (existing.isPresent()) {
            spatialRepository.remove(key, id);
            return true;
        }
        return false;
    }
    
    @Override
    public boolean drop(String key) {
        Set<String> existingKeys = spatialRepository.keys();
        if (existingKeys.contains(key)) {
            spatialRepository.drop(key);
            return true;
        }
        return false;
    }
    
    @Override
    public Optional<Bounds> bounds(String key) {
        Map<String, Tile38Object> collection = spatialRepository.getAll(key);
        if (collection.isEmpty()) {
            return Optional.empty();
        }
        
        Bounds bounds = Bounds.builder().build();
        for (Tile38Object object : collection.values()) {
            if (object.getGeometry() != null) {
                Geometry geom = object.getGeometry();
                bounds.extend(geom.getEnvelopeInternal().getMinX(), geom.getEnvelopeInternal().getMinY());
                bounds.extend(geom.getEnvelopeInternal().getMaxX(), geom.getEnvelopeInternal().getMaxY());
            }
        }
        
        return bounds.isEmpty() ? Optional.empty() : Optional.of(bounds);
    }
    
    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius) {
        logger.debug("Starting nearby search for collection '{}' at ({},{}) with radius {}", key, lat, lon, radius);
        long startTime = System.currentTimeMillis();
        
        List<SearchResult> results = spatialRepository.nearby(key, lat, lon, radius);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Completed nearby search for collection '{}' in {}ms, found {} results", key, duration, results.size());
        return results;
    }
    
    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius, FilterCondition filter) {
        logger.debug("Starting nearby search with filter for collection '{}' at ({},{}) with radius {}", key, lat, lon, radius);
        long startTime = System.currentTimeMillis();
        
        List<SearchResult> results = spatialRepository.nearby(key, lat, lon, radius, filter);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Completed nearby search with filter for collection '{}' in {}ms, found {} results", key, duration, results.size());
        return results;
    }
    
    @Override
    public List<SearchResult> within(String key, Geometry geometry) {
        logger.debug("Starting within search for collection '{}'", key);
        long startTime = System.currentTimeMillis();
        
        List<SearchResult> results = spatialRepository.within(key, geometry);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Completed within search for collection '{}' in {}ms, found {} results", key, duration, results.size());
        return results;
    }
    
    @Override
    public List<SearchResult> within(String key, Geometry geometry, FilterCondition filter) {
        logger.debug("Starting within search with filter for collection '{}'", key);
        long startTime = System.currentTimeMillis();
        
        List<SearchResult> results = spatialRepository.within(key, geometry, filter);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Completed within search with filter for collection '{}' in {}ms, found {} results", key, duration, results.size());
        return results;
    }
    
    @Override
    public List<SearchResult> intersects(String key, Geometry geometry) {
        logger.debug("Starting intersects search for collection '{}'", key);
        long startTime = System.currentTimeMillis();
        
        List<SearchResult> results = spatialRepository.intersects(key, geometry);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Completed intersects search for collection '{}' in {}ms, found {} results", key, duration, results.size());
        return results;
    }
    
    @Override
    public List<SearchResult> intersects(String key, Geometry geometry, FilterCondition filter) {
        logger.debug("Starting intersects search with filter for collection '{}'", key);
        long startTime = System.currentTimeMillis();
        
        List<SearchResult> results = spatialRepository.intersects(key, geometry, filter);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.debug("Completed intersects search with filter for collection '{}' in {}ms, found {} results", key, duration, results.size());
        return results;
    }
    
    @Override
    public boolean updateKVData(String key, String id, KVData kvData) {
        return spatialRepository.updateKVData(key, id, kvData);
    }
    
    @Override
    public List<SearchResult> scan(String key, FilterCondition filter, int limit, int offset) {
        return spatialRepository.scan(key, filter, limit, offset);
    }
    
    @Override
    public List<String> keys() {
        return new ArrayList<>(spatialRepository.keys());
    }
    
    @Override
    public String stats() {
        Set<String> allKeys = spatialRepository.keys();
        long totalObjects = spatialRepository.getTotalObjectCount();
        long memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        
        StringBuilder stats = new StringBuilder();
        stats.append("{");
        stats.append("\"in_memory_size\":").append(memoryUsed).append(",");
        stats.append("\"num_collections\":").append(allKeys.size()).append(",");
        stats.append("\"num_objects\":").append(totalObjects).append(",");
        stats.append("\"collections\":{");
        
        boolean first = true;
        for (String key : allKeys) {
            if (!first) stats.append(",");
            stats.append("\"").append(key).append("\":")
                .append(spatialRepository.getObjectCount(key));
            first = false;
        }
        
        stats.append("}}");
        
        return stats.toString();
    }
    
    @Override
    public void flushdb() {
        spatialRepository.flushAll();
        logger.info("Database flushed");
    }
}