package com.tile38.enterprise.service;

import com.tile38.enterprise.client.Tile38Client;
import com.tile38.enterprise.model.Point;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EnterpriseGeospatialServiceTest {
    
    @Mock
    private Tile38Client tile38Client;
    
    private EnterpriseGeospatialService service;
    private MeterRegistry meterRegistry;
    
    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new EnterpriseGeospatialService(meterRegistry);
        service.tile38Client = tile38Client;
    }
    
    @Test
    void testStoreObject() {
        // Given
        String collection = "vehicles";
        String id = "car123";
        Point point = new Point(37.7749, -122.4194);
        Map<String, Object> fields = new HashMap<>();
        fields.put("speed", 45.0);
        fields.put("heading", 270);
        
        // When
        service.storeObject(collection, id, point, fields);
        
        // Then
        verify(tile38Client).setPoint(eq(collection), eq(id), eq(point), any(Map.class));
        assertTrue(fields.containsKey("created_at"));
        assertTrue(fields.containsKey("enterprise_version"));
    }
    
    @Test
    void testGetObject() {
        // Given
        String collection = "vehicles";
        String id = "car123";
        
        // When
        service.getObject(collection, id);
        
        // Then
        verify(tile38Client).get(collection, id);
    }
    
    @Test
    void testFindNearby() {
        // Given
        String collection = "vehicles";
        Point center = new Point(37.7749, -122.4194);
        double radius = 1000.0;
        String unit = "m";
        
        // When
        service.findNearby(collection, center, radius, unit);
        
        // Then
        verify(tile38Client).nearby(collection, center, radius, unit);
    }
    
    @Test
    void testIsHealthy() {
        // Given
        when(tile38Client.ping()).thenReturn(true);
        
        // When
        boolean healthy = service.isHealthy();
        
        // Then
        assertTrue(healthy);
        verify(tile38Client).ping();
    }
    
    @Test
    void testDeleteObject() {
        // Given
        String collection = "vehicles";
        String id = "car123";
        when(tile38Client.delete(collection, id)).thenReturn(true);
        
        // When
        boolean deleted = service.deleteObject(collection, id);
        
        // Then
        assertTrue(deleted);
        verify(tile38Client).delete(collection, id);
    }
}