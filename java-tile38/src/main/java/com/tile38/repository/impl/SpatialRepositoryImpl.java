package com.tile38.repository.impl;

import com.tile38.repository.SpatialRepository;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;

import org.springframework.stereotype.Repository;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.index.ItemVisitor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of SpatialRepository using JTS STRtree for spatial indexing
 */
@Repository
public class SpatialRepositoryImpl implements SpatialRepository {
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    
    // Spatial indexes per collection
    private final Map<String, STRtree> spatialIndexes = new ConcurrentHashMap<>();
    
    // Object storage per collection - needed for retrieving full objects
    private final Map<String, Map<String, Tile38Object>> objectStorage = new ConcurrentHashMap<>();
    
    @Override
    public void index(String key, String id, Tile38Object object) {
        if (object.getGeometry() == null) {
            return;
        }
        
        STRtree index = spatialIndexes.computeIfAbsent(key, k -> new STRtree());
        Map<String, Tile38Object> storage = objectStorage.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        
        // Remove existing entry if present
        remove(key, id);
        
        // Add to spatial index
        index.insert(object.getGeometry().getEnvelopeInternal(), id);
        
        // Store object
        storage.put(id, object);
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
    }
    
    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius) {
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
                if (object != null) {
                    double distance = center.distance(object.getGeometry());
                    // Convert distance from degrees to meters (approximation)
                    double distanceMeters = distance * 111000.0;
                    
                    if (distanceMeters <= radius) {
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
                if (object != null && geometry.contains(object.getGeometry())) {
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
                if (object != null && geometry.intersects(object.getGeometry())) {
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
    public void flushAll() {
        spatialIndexes.clear();
        objectStorage.clear();
    }
}