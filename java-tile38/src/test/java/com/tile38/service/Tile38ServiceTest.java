package com.tile38.service;

import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.service.impl.Tile38ServiceImpl;
import com.tile38.repository.SpatialRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;

import java.util.Optional;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class Tile38ServiceTest {
    
    @Mock
    private SpatialRepository spatialRepository;
    
    @InjectMocks
    private Tile38ServiceImpl tile38Service;
    
    private GeometryFactory geometryFactory;
    
    @BeforeEach
    void setUp() {
        geometryFactory = new GeometryFactory();
    }
    
    @Test
    void testSetAndGet() {
        // Given
        String key = "testCollection";
        String id = "testObject";
        Point point = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "test");
        
        Tile38Object object = Tile38Object.builder()
                .id(id)
                .geometry(point)
                .fields(fields)
                .timestamp(System.currentTimeMillis())
                .build();
        
        // When
        tile38Service.set(key, id, object);
        Optional<Tile38Object> retrieved = tile38Service.get(key, id);
        
        // Then
        assertTrue(retrieved.isPresent());
        assertEquals(id, retrieved.get().getId());
        assertEquals(point, retrieved.get().getGeometry());
        assertEquals("test", retrieved.get().getFields().get("name"));
        
        verify(spatialRepository).index(key, id, object);
    }
    
    @Test
    void testDelete() {
        // Given
        String key = "testCollection";
        String id = "testObject";
        Point point = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        
        Tile38Object object = Tile38Object.builder()
                .id(id)
                .geometry(point)
                .timestamp(System.currentTimeMillis())
                .build();
        
        // When
        tile38Service.set(key, id, object);
        boolean deleted = tile38Service.del(key, id);
        Optional<Tile38Object> retrieved = tile38Service.get(key, id);
        
        // Then
        assertTrue(deleted);
        assertFalse(retrieved.isPresent());
        
        verify(spatialRepository).remove(key, id);
    }
    
    @Test
    void testNearby() {
        // Given
        String key = "testCollection";
        double lat = 33.5;
        double lon = -115.5;
        double radius = 1000.0;
        
        when(spatialRepository.nearby(key, lat, lon, radius))
                .thenReturn(Collections.emptyList());
        
        // When
        var results = tile38Service.nearby(key, lat, lon, radius);
        
        // Then
        assertNotNull(results);
        assertTrue(results.isEmpty());
        verify(spatialRepository).nearby(key, lat, lon, radius);
    }
    
    @Test
    void testKeys() {
        // Given - set some objects first
        String key1 = "collection1";
        String key2 = "collection2";
        
        Point point = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        Tile38Object object = Tile38Object.builder()
                .id("test")
                .geometry(point)
                .build();
        
        // When
        tile38Service.set(key1, "test1", object);
        tile38Service.set(key2, "test2", object);
        var keys = tile38Service.keys();
        
        // Then
        assertEquals(2, keys.size());
        assertTrue(keys.contains(key1));
        assertTrue(keys.contains(key2));
    }
}