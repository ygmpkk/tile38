package com.tile38.service;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Optional;

/**
 * Core Tile38 service interface for geospatial operations
 */
public interface Tile38Service {
    
    /**
     * Set/Store a geospatial object
     */
    void set(String key, String id, Tile38Object object);
    
    /**
     * Get an object by key and id
     */
    Optional<Tile38Object> get(String key, String id);
    
    /**
     * Delete an object
     */
    boolean del(String key, String id);
    
    /**
     * Drop an entire collection
     */
    boolean drop(String key);
    
    /**
     * Get bounds of a collection
     */
    Optional<Bounds> bounds(String key);
    
    /**
     * Search for objects nearby a point
     */
    List<SearchResult> nearby(String key, double lat, double lon, double radius);
    
    /**
     * Search for objects within a geometry
     */
    List<SearchResult> within(String key, Geometry geometry);
    
    /**
     * Search for objects that intersect with a geometry
     */
    List<SearchResult> intersects(String key, Geometry geometry);
    
    /**
     * Get all keys (collections)
     */
    List<String> keys();
    
    /**
     * Get statistics
     */
    String stats();
    
    /**
     * Flush all data
     */
    void flushdb();
}