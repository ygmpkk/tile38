package com.tile38.repository;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * Repository interface for spatial indexing and queries
 */
public interface SpatialRepository {
    
    /**
     * Index a spatial object
     */
    void index(String key, String id, Tile38Object object);
    
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
     * Search for objects within a geometry
     */
    List<SearchResult> within(String key, Geometry geometry);
    
    /**
     * Search for objects that intersect with a geometry
     */
    List<SearchResult> intersects(String key, Geometry geometry);
    
    /**
     * Clear all spatial indexes
     */
    void flushAll();
}