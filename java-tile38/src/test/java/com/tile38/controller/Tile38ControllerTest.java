package com.tile38.controller;

import com.tile38.controller.Tile38Controller;
import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.Bounds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class Tile38ControllerTest {
    
    @Mock
    private Tile38Service tile38Service;
    
    @InjectMocks
    private Tile38Controller controller;
    
    @Test
    void testSetObject() {
        // Given
        String key = "testCollection";
        String id = "testObject";
        Map<String, Object> request = new HashMap<>();
        request.put("lat", 33.5);
        request.put("lon", -115.5);
        request.put("fields", Map.of("name", "test"));
        
        doNothing().when(tile38Service).set(eq(key), eq(id), any(Tile38Object.class));
        
        // When
        ResponseEntity<Map<String, Object>> response = controller.setObject(key, id, request);
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue((Boolean) response.getBody().get("ok"));
        
        verify(tile38Service).set(eq(key), eq(id), any(Tile38Object.class));
    }
    
    @Test
    void testGetObject() {
        // Given
        String key = "testCollection";
        String id = "testObject";
        GeometryFactory geometryFactory = new GeometryFactory();
        Point point = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        
        Tile38Object object = Tile38Object.builder()
                .id(id)
                .geometry(point)
                .fields(Map.of("name", "test"))
                .build();
        
        when(tile38Service.get(key, id)).thenReturn(Optional.of(object));
        
        // When
        ResponseEntity<Map<String, Object>> response = controller.getObject(key, id);
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue((Boolean) response.getBody().get("ok"));
        assertNotNull(response.getBody().get("object"));
        
        verify(tile38Service).get(key, id);
    }
    
    @Test
    void testGetObjectNotFound() {
        // Given
        String key = "testCollection";
        String id = "nonexistent";
        
        when(tile38Service.get(key, id)).thenReturn(Optional.empty());
        
        // When
        ResponseEntity<Map<String, Object>> response = controller.getObject(key, id);
        
        // Then
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        
        verify(tile38Service).get(key, id);
    }
    
    @Test
    void testDeleteObject() {
        // Given
        String key = "testCollection";
        String id = "testObject";
        
        when(tile38Service.del(key, id)).thenReturn(true);
        
        // When
        ResponseEntity<Map<String, Object>> response = controller.deleteObject(key, id);
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue((Boolean) response.getBody().get("ok"));
        assertEquals(1, response.getBody().get("deleted"));
        
        verify(tile38Service).del(key, id);
    }
    
    @Test
    void testGetKeys() {
        // Given
        List<String> keys = Arrays.asList("collection1", "collection2");
        when(tile38Service.keys()).thenReturn(keys);
        
        // When
        ResponseEntity<Map<String, Object>> response = controller.getKeys();
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue((Boolean) response.getBody().get("ok"));
        assertEquals(keys, response.getBody().get("keys"));
        
        verify(tile38Service).keys();
    }
}