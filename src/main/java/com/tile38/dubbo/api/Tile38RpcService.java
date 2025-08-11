package com.tile38.dubbo.api;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.model.KVData;
import com.tile38.model.FilterCondition;
import com.tile38.loader.DataLoader;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Dubbo RPC interface for Tile38 operations with enhanced KV capabilities
 * Provides comprehensive geospatial and key-value operations
 */
public interface Tile38RpcService {
    
    /**
     * Set/Store a geospatial object (legacy method)
     */
    void set(String key, String id, double lat, double lon, Map<String, Object> fields, Long expirationSeconds);
    
    /**
     * Set/Store a geospatial object with KV data support
     */
    void setWithKV(String key, String id, double lat, double lon, Map<String, Object> fields, 
                   Map<String, String> tags, Map<String, Object> attributes, Long expirationSeconds);
    
    /**
     * Bulk set multiple objects with KV data support
     */
    void bulkSet(String key, Map<String, Tile38Object> objects);
    
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
     * Search for objects nearby a point (legacy method)
     */
    List<SearchResult> nearby(String key, double lat, double lon, double radius);
    
    /**
     * Search for objects nearby a point with KV filtering
     */
    List<SearchResult> nearbyWithFilter(String key, double lat, double lon, double radius, FilterCondition filter);
    
    /**
     * Update KV data for an existing object without affecting geometry
     */
    boolean updateKVData(String key, String id, Map<String, String> tags, Map<String, Object> attributes);
    
    /**
     * Update KV data for an existing object with KVData object
     */
    boolean updateKVDataObject(String key, String id, KVData kvData);
    
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
    
    // Advanced Data Loading Operations
    
    /**
     * Load data from JSON file
     */
    CompletableFuture<DataLoader.LoadResult> loadFromJson(String filePath);
    
    /**
     * Load data from CSV file
     */
    CompletableFuture<DataLoader.LoadResult> loadFromCsv(String filePath);
    
    /**
     * Generate synthetic test data for performance testing
     */
    CompletableFuture<DataLoader.LoadResult> generateTestData(String collectionName, int numberOfRecords,
                                                             double minLat, double maxLat, 
                                                             double minLon, double maxLon);
    
    // Advanced Search Operations
    
    /**
     * Scan all objects in a collection with optional filter
     */
    List<SearchResult> scan(String key, FilterCondition filter, int limit, int offset);
    
    /**
     * Search for objects intersecting with a bounding box
     */
    List<SearchResult> intersects(String key, double minLat, double minLon, 
                                  double maxLat, double maxLon, FilterCondition filter);
    
    /**
     * Search for objects within a bounding box
     */
    List<SearchResult> within(String key, double minLat, double minLon, 
                              double maxLat, double maxLon, FilterCondition filter);
}