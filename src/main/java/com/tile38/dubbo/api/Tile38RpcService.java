package com.tile38.dubbo.api;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.model.KVData;
import com.tile38.model.FilterCondition;
import com.tile38.loader.DataLoader;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Dubbo RPC interface for Tile38 operations with polygon-centric design
 * Core focus on polygon data with KV as supplemental metadata
 */
public interface Tile38RpcService {
    
    /**
     * Set/Store a polygon object by ID
     */
    void set(String key, String id, Geometry geometry, Map<String, Object> fields, Long expirationSeconds);
    
    /**
     * Set/Store a polygon object with KV data
     */
    void setWithKVData(String key, String id, Geometry geometry, Map<String, Object> fields, 
                       KVData kvData, Long expirationSeconds);
    
    /**
     * Bulk set multiple polygon objects
     */
    void bulkSet(String key, Map<String, Tile38Object> objects);
    
    /**
     * Get a polygon object by key and id
     */
    Tile38Object get(String key, String id);
    
    /**
     * Delete a polygon object
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
     * Search for polygon objects near a point
     */
    List<SearchResult> nearby(String key, Point centerPoint, double radius);
    
    /**
     * Search for polygon objects near a point with KV filtering
     */
    List<SearchResult> nearbyWithFilter(String key, Point centerPoint, double radius, FilterCondition filter);
    
    /**
     * Update KV data for an existing polygon object by ID only
     * This is the core KV operation - purely ID-based, no coordinates involved
     */
    boolean updateKVData(String key, String id, KVData kvData);
    
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
     * Load polygon data from JSON file
     */
    CompletableFuture<DataLoader.LoadResult> loadFromJson(String filePath);
    
    /**
     * Load polygon data from CSV file
     */
    CompletableFuture<DataLoader.LoadResult> loadFromCsv(String filePath);
    
    /**
     * Generate synthetic polygon test data for performance testing
     */
    CompletableFuture<DataLoader.LoadResult> generateTestData(String collectionName, int numberOfRecords,
                                                             double minLat, double maxLat, 
                                                             double minLon, double maxLon);
    
    // Advanced Search Operations
    
    /**
     * Scan all polygon objects in a collection with optional KV filter
     */
    List<SearchResult> scan(String key, FilterCondition filter, int limit, int offset);
    
    /**
     * Search for polygon objects intersecting with a bounding box
     */
    List<SearchResult> intersects(String key, double minLat, double minLon, 
                                  double maxLat, double maxLon, FilterCondition filter);
    
    /**
     * Search for polygon objects within a bounding box
     */
    List<SearchResult> within(String key, double minLat, double minLon, 
                              double maxLat, double maxLon, FilterCondition filter);
}