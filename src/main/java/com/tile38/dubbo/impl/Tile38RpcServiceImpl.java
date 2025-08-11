package com.tile38.dubbo.impl;

import com.tile38.dubbo.api.Tile38RpcService;
import com.tile38.service.Tile38Service;
import com.tile38.loader.DataLoader;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.model.KVData;
import com.tile38.model.FilterCondition;

import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Autowired;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Dubbo RPC service implementation for Tile38 operations with enhanced KV capabilities
 */
@Service
@Slf4j
@DubboService  // Commented out for HTTP-only mode
public class Tile38RpcServiceImpl implements Tile38RpcService {

    @Autowired
    private Tile38Service tile38Service;
    
    @Autowired
    private DataLoader dataLoader;

    private final GeometryFactory geometryFactory = new GeometryFactory();

    @Override
    public void set(String key, String id, double lat, double lon, Map<String, Object> fields, Long expirationSeconds) {
        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));

        Instant expireAt = expirationSeconds != null ? Instant.now().plusSeconds(expirationSeconds) : null;

        Tile38Object object = Tile38Object.builder()
                .id(id)
                .geometry(point)
                .fields(fields)
                .expireAt(expireAt)
                .timestamp(System.currentTimeMillis())
                .build();

        tile38Service.set(key, id, object);
        log.debug("Set object via DUBBO: {}/{}", key, id);
    }

    @Override
    public void setWithKV(String key, String id, double lat, double lon, Map<String, Object> fields, 
                          Map<String, String> tags, Map<String, Object> attributes, Long expirationSeconds) {
        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));

        Instant expireAt = expirationSeconds != null ? Instant.now().plusSeconds(expirationSeconds) : null;

        // Create KV data
        KVData kvData = new KVData();
        if (tags != null) {
            tags.forEach(kvData::setTag);
        }
        if (attributes != null) {
            attributes.forEach(kvData::setAttribute);
        }

        Tile38Object object = Tile38Object.builder()
                .id(id)
                .geometry(point)
                .fields(fields)
                .kvData(kvData.isEmpty() ? null : kvData)
                .expireAt(expireAt)
                .timestamp(System.currentTimeMillis())
                .build();

        tile38Service.set(key, id, object);
        log.debug("Set object with KV data via DUBBO: {}/{} with {} tags, {} attributes", 
                 key, id, tags != null ? tags.size() : 0, attributes != null ? attributes.size() : 0);
    }

    @Override
    public void bulkSet(String key, Map<String, Tile38Object> objects) {
        tile38Service.bulkSet(key, objects);
        log.debug("Bulk set {} objects via DUBBO for collection: {}", objects.size(), key);
    }

    @Override
    public Tile38Object get(String key, String id) {
        Tile38Object result = tile38Service.get(key, id).orElse(null);
        log.debug("Get object via DUBBO: {}/{} - found: {}", key, id, result != null);
        return result;
    }

    @Override
    public boolean del(String key, String id) {
        boolean deleted = tile38Service.del(key, id);
        log.debug("Delete object via DUBBO: {}/{} - deleted: {}", key, id, deleted);
        return deleted;
    }

    @Override
    public boolean drop(String key) {
        boolean dropped = tile38Service.drop(key);
        log.debug("Drop collection via DUBBO: {} - dropped: {}", key, dropped);
        return dropped;
    }

    @Override
    public Bounds bounds(String key) {
        Bounds result = tile38Service.bounds(key).orElse(null);
        log.debug("Get bounds via DUBBO: {} - found: {}", key, result != null);
        return result;
    }

    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius) {
        List<SearchResult> results = tile38Service.nearby(key, lat, lon, radius);
        log.debug("Nearby search via DUBBO: {} ({},{}) radius:{} - found {} results", 
                 key, lat, lon, radius, results.size());
        return results;
    }

    @Override
    public List<SearchResult> nearbyWithFilter(String key, double lat, double lon, double radius, FilterCondition filter) {
        List<SearchResult> results = tile38Service.nearby(key, lat, lon, radius, filter);
        log.debug("Nearby search with filter via DUBBO: {} ({},{}) radius:{} - found {} results", 
                 key, lat, lon, radius, results.size());
        return results;
    }

    @Override
    public boolean updateKVData(String key, String id, Map<String, String> tags, Map<String, Object> attributes) {
        // Create KV data from maps
        KVData kvData = new KVData();
        if (tags != null) {
            tags.forEach(kvData::setTag);
        }
        if (attributes != null) {
            attributes.forEach(kvData::setAttribute);
        }

        boolean updated = tile38Service.updateKVData(key, id, kvData);
        log.debug("Update KV data via DUBBO: {}/{} - updated: {} (tags:{}, attrs:{})", 
                 key, id, updated, tags != null ? tags.size() : 0, attributes != null ? attributes.size() : 0);
        return updated;
    }

    @Override
    public boolean updateKVDataObject(String key, String id, KVData kvData) {
        boolean updated = tile38Service.updateKVData(key, id, kvData);
        log.debug("Update KV data object via DUBBO: {}/{} - updated: {}", key, id, updated);
        return updated;
    }

    @Override
    public List<String> keys() {
        List<String> result = tile38Service.keys();
        log.debug("Get keys via DUBBO - found {} collections", result.size());
        return result;
    }

    @Override
    public String stats() {
        String result = tile38Service.stats();
        log.debug("Get stats via DUBBO");
        return result;
    }

    @Override
    public void flushdb() {
        tile38Service.flushdb();
        log.debug("Flush database via DUBBO");
    }

    @Override
    public String ping() {
        log.debug("Ping via DUBBO");
        return "PONG";
    }
    
    // Advanced Data Loading Operations
    
    @Override
    public CompletableFuture<DataLoader.LoadResult> loadFromJson(String filePath) {
        log.debug("Loading data from JSON via DUBBO: {}", filePath);
        return dataLoader.loadFromJson(filePath);
    }
    
    @Override
    public CompletableFuture<DataLoader.LoadResult> loadFromCsv(String filePath) {
        log.debug("Loading data from CSV via DUBBO: {}", filePath);
        return dataLoader.loadFromCsv(filePath);
    }
    
    @Override
    public CompletableFuture<DataLoader.LoadResult> generateTestData(String collectionName, int numberOfRecords,
                                                                     double minLat, double maxLat,
                                                                     double minLon, double maxLon) {
        log.debug("Generating test data via DUBBO: collection={}, records={}", collectionName, numberOfRecords);
        return dataLoader.generateTestData(collectionName, numberOfRecords, minLat, maxLat, minLon, maxLon);
    }
    
    // Advanced Search Operations
    
    @Override
    public List<SearchResult> scan(String key, FilterCondition filter, int limit, int offset) {
        List<SearchResult> results = tile38Service.scan(key, filter, limit, offset);
        log.debug("Scan via DUBBO: {} - found {} results (limit:{}, offset:{})", 
                 key, results.size(), limit, offset);
        return results;
    }
    
    @Override
    public List<SearchResult> intersects(String key, double minLat, double minLon, 
                                         double maxLat, double maxLon, FilterCondition filter) {
        // Create bounding box geometry
        Point[] points = new Point[5];
        points[0] = geometryFactory.createPoint(new Coordinate(minLon, minLat));
        points[1] = geometryFactory.createPoint(new Coordinate(maxLon, minLat));
        points[2] = geometryFactory.createPoint(new Coordinate(maxLon, maxLat));
        points[3] = geometryFactory.createPoint(new Coordinate(minLon, maxLat));
        points[4] = geometryFactory.createPoint(new Coordinate(minLon, minLat)); // Close the polygon
        
        // Create polygon from points
        Coordinate[] coords = new Coordinate[points.length];
        for (int i = 0; i < points.length; i++) {
            coords[i] = points[i].getCoordinate();
        }
        
        org.locationtech.jts.geom.Polygon boundingBox = geometryFactory.createPolygon(coords);
        
        List<SearchResult> results = tile38Service.intersects(key, boundingBox, filter);
        log.debug("Intersects via DUBBO: {} bbox({},{},{},{}) - found {} results", 
                 key, minLat, minLon, maxLat, maxLon, results.size());
        return results;
    }
    
    @Override
    public List<SearchResult> within(String key, double minLat, double minLon, 
                                     double maxLat, double maxLon, FilterCondition filter) {
        // Create bounding box geometry
        Point[] points = new Point[5];
        points[0] = geometryFactory.createPoint(new Coordinate(minLon, minLat));
        points[1] = geometryFactory.createPoint(new Coordinate(maxLon, minLat));
        points[2] = geometryFactory.createPoint(new Coordinate(maxLon, maxLat));
        points[3] = geometryFactory.createPoint(new Coordinate(minLon, maxLat));
        points[4] = geometryFactory.createPoint(new Coordinate(minLon, minLat)); // Close the polygon
        
        // Create polygon from points
        Coordinate[] coords = new Coordinate[points.length];
        for (int i = 0; i < points.length; i++) {
            coords[i] = points[i].getCoordinate();
        }
        
        org.locationtech.jts.geom.Polygon boundingBox = geometryFactory.createPolygon(coords);
        
        List<SearchResult> results = tile38Service.within(key, boundingBox, filter);
        log.debug("Within via DUBBO: {} bbox({},{},{},{}) - found {} results", 
                 key, minLat, minLon, maxLat, maxLon, results.size());
        return results;
    }
}