package com.tile38.controller;

import com.tile38.controller.Tile38Controller;
import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.Bounds;
import com.tile38.model.param.SetObjectParam;
import com.tile38.model.result.ApiResponse;
import com.tile38.model.result.ObjectResult;
import com.tile38.model.result.CollectionResult;

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
        GeometryFactory geometryFactory = new GeometryFactory();
        Point geometry = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        
        SetObjectParam param = SetObjectParam.builder()
                .geometry(geometry)
                .fields(Map.of("name", "test"))
                .build();
        
        doNothing().when(tile38Service).set(eq(key), eq(id), any(Tile38Object.class));
        
        // When
        ResponseEntity<ApiResponse<ObjectResult>> response = controller.setObject(key, id, param);
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().getOk());
        
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
        ResponseEntity<ApiResponse<ObjectResult>> response = controller.getObject(key, id);
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().getOk());
        assertNotNull(response.getBody().getData().getObject());
        
        verify(tile38Service).get(key, id);
    }
    
    @Test
    void testGetObjectNotFound() {
        // Given
        String key = "testCollection";
        String id = "nonexistent";
        
        when(tile38Service.get(key, id)).thenReturn(Optional.empty());
        
        // When
        ResponseEntity<ApiResponse<ObjectResult>> response = controller.getObject(key, id);
        
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
        ResponseEntity<ApiResponse<ObjectResult>> response = controller.deleteObject(key, id);
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().getOk());
        assertEquals(1, response.getBody().getData().getDeleted());
        
        verify(tile38Service).del(key, id);
    }
    
    @Test
    void testGetKeys() {
        // Given
        List<String> keys = Arrays.asList("collection1", "collection2");
        when(tile38Service.keys()).thenReturn(keys);
        
        // When
        ResponseEntity<ApiResponse<CollectionResult>> response = controller.getKeys();
        
        // Then
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().getOk());
        assertEquals(keys, response.getBody().getData().getKeys());
        
        verify(tile38Service).keys();
    }
}