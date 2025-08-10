package com.tile38.service.impl;

import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.repository.SpatialRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Coordinate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;

/**
 * Implementation of Tile38Service using in-memory storage and JTS for geospatial operations
 */
@Service
public class Tile38ServiceImpl implements Tile38Service {
    
    @Autowired
    private SpatialRepository spatialRepository;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    
    // In-memory collections storage - maps collection key to object map
    private final Map<String, Map<String, Tile38Object>> collections = new ConcurrentHashMap<>();
    
    @Override
    public void set(String key, String id, Tile38Object object) {
        collections.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                   .put(id, object);
        
        // Update spatial index
        spatialRepository.index(key, id, object);
    }
    
    @Override
    public Optional<Tile38Object> get(String key, String id) {
        Map<String, Tile38Object> collection = collections.get(key);
        if (collection == null) {
            return Optional.empty();
        }
        
        Tile38Object object = collection.get(id);
        if (object != null && object.isExpired()) {
            // Remove expired object
            collection.remove(id);
            spatialRepository.remove(key, id);
            return Optional.empty();
        }
        
        return Optional.ofNullable(object);
    }
    
    @Override
    public boolean del(String key, String id) {
        Map<String, Tile38Object> collection = collections.get(key);
        if (collection == null) {
            return false;
        }
        
        Tile38Object removed = collection.remove(id);
        if (removed != null) {
            spatialRepository.remove(key, id);
            return true;
        }
        return false;
    }
    
    @Override
    public boolean drop(String key) {
        Map<String, Tile38Object> removed = collections.remove(key);
        if (removed != null) {
            spatialRepository.drop(key);
            return true;
        }
        return false;
    }
    
    @Override
    public Optional<Bounds> bounds(String key) {
        Map<String, Tile38Object> collection = collections.get(key);
        if (collection == null || collection.isEmpty()) {
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
        return spatialRepository.nearby(key, lat, lon, radius);
    }
    
    @Override
    public List<SearchResult> within(String key, Geometry geometry) {
        return spatialRepository.within(key, geometry);
    }
    
    @Override
    public List<SearchResult> intersects(String key, Geometry geometry) {
        return spatialRepository.intersects(key, geometry);
    }
    
    @Override
    public List<String> keys() {
        return new ArrayList<>(collections.keySet());
    }
    
    @Override
    public String stats() {
        int totalKeys = collections.size();
        int totalObjects = collections.values().stream()
                                   .mapToInt(Map::size)
                                   .sum();
        
        return String.format("{\"in_memory_size\":%d,\"num_collections\":%d,\"num_objects\":%d}", 
                           Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(),
                           totalKeys, 
                           totalObjects);
    }
    
    @Override
    public void flushdb() {
        collections.clear();
        spatialRepository.flushAll();
    }
}