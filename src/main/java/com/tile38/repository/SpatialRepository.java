package com.tile38.repository;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.FilterCondition;
import com.tile38.model.KVData;
import org.locationtech.jts.geom.Geometry;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Map;

/**
 * Repository interface for spatial indexing and queries
 */
public interface SpatialRepository {
    
    /**
     * Index a spatial object
     */
    void index(String key, String id, Tile38Object object);
    
    /**
     * Bulk index multiple objects for better performance
     */
    void bulkIndex(String key, Map<String, Tile38Object> objects);
    
    /**
     * Get an object by key and id
     */
    Optional<Tile38Object> get(String key, String id);
    
    /**
     * Get all keys (collections)
     */
    Set<String> keys();
    
    /**
     * Get all objects in a collection
     */
    Map<String, Tile38Object> getAll(String key);
    
    /**
     * Remove an object from the spatial index
     */
    void remove(String key, String id);
    
    /**
     * Drop all objects for a collection
     */
    void drop(String key);
    
    /**
     * Search for objects nearby a point
     */
    List<SearchResult> nearby(String key, double lat, double lon, double radius);
    
    /**
     * Search for objects nearby a point with KV filtering
     */
    List<SearchResult> nearby(String key, double lat, double lon, double radius, FilterCondition filter);
    
    /**
     * Search for objects within a geometry
     */
    List<SearchResult> within(String key, Geometry geometry);
    
    /**
     * Search for objects within a geometry with KV filtering
     */
    List<SearchResult> within(String key, Geometry geometry, FilterCondition filter);
    
    /**
     * Search for objects that intersect with a geometry
     */
    List<SearchResult> intersects(String key, Geometry geometry);
    
    /**
     * Search for objects that intersect with a geometry with KV filtering
     */
    List<SearchResult> intersects(String key, Geometry geometry, FilterCondition filter);
    
    /**
     * Update KV data for an existing object
     */
    boolean updateKVData(String key, String id, KVData kvData);
    
    /**
     * Get total number of objects across all collections
     */
    long getTotalObjectCount();
    
    /**
     * Get number of objects in a specific collection
     */
    long getObjectCount(String key);
    
    /**
     * Clear all spatial indexes
     */
    void flushAll();
}