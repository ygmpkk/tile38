package com.tile38.dubbo.api;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;

import java.util.List;
import java.util.Map;

/**
 * Dubbo RPC interface for Tile38 operations
 */
public interface Tile38RpcService {
    
    /**
     * Set/Store a geospatial object
     */
    void set(String key, String id, double lat, double lon, Map<String, Object> fields, Long expirationSeconds);
    
    /**
     * Get an object by key and id
     */
    Tile38Object get(String key, String id);
    
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
    Bounds bounds(String key);
    
    /**
     * Search for objects nearby a point
     */
    List<SearchResult> nearby(String key, double lat, double lon, double radius);
    
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
    
    /**
     * Ping the server
     */
    String ping();
}