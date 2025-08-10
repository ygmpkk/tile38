package com.tile38.dubbo.impl;

import com.tile38.dubbo.api.Tile38RpcService;
import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;

import org.apache.dubbo.config.annotation.DubboService;
import org.springframework.beans.factory.annotation.Autowired;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Dubbo RPC service implementation for Tile38 operations
 */
// @DubboService  // Commented out for HTTP-only mode
public class Tile38RpcServiceImpl implements Tile38RpcService {
    
    @Autowired
    private Tile38Service tile38Service;
    
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
    }
    
    @Override
    public Tile38Object get(String key, String id) {
        return tile38Service.get(key, id).orElse(null);
    }
    
    @Override
    public boolean del(String key, String id) {
        return tile38Service.del(key, id);
    }
    
    @Override
    public boolean drop(String key) {
        return tile38Service.drop(key);
    }
    
    @Override
    public Bounds bounds(String key) {
        return tile38Service.bounds(key).orElse(null);
    }
    
    @Override
    public List<SearchResult> nearby(String key, double lat, double lon, double radius) {
        return tile38Service.nearby(key, lat, lon, radius);
    }
    
    @Override
    public List<String> keys() {
        return tile38Service.keys();
    }
    
    @Override
    public String stats() {
        return tile38Service.stats();
    }
    
    @Override
    public void flushdb() {
        tile38Service.flushdb();
    }
    
    @Override
    public String ping() {
        return "PONG";
    }
}