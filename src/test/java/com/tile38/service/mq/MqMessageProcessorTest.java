package com.tile38.service.mq;

import com.tile38.model.MqMessage;
import com.tile38.model.Tile38Object;
import com.tile38.service.Tile38Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MqMessageProcessor
 */
@ExtendWith(MockitoExtension.class)
public class MqMessageProcessorTest {
    
    @Mock
    private Tile38Service tile38Service;
    
    @InjectMocks
    private MqMessageProcessor messageProcessor;
    
    private ObjectMapper objectMapper;
    private GeometryFactory geometryFactory;
    
    @BeforeEach
    public void setup() {
        objectMapper = new ObjectMapper();
        geometryFactory = new GeometryFactory();
        
        // Inject ObjectMapper manually since it's not mocked
        try {
            java.lang.reflect.Field field = MqMessageProcessor.class.getDeclaredField("objectMapper");
            field.setAccessible(true);
            field.set(messageProcessor, objectMapper);
        } catch (Exception e) {
            fail("Failed to inject ObjectMapper: " + e.getMessage());
        }
    }
    
    @Test
    public void testProcessSetOperationWithPoint() throws Exception {
        // Prepare message
        String messageJson = """
            {
              "operation": "SET",
              "collection": "fleet",
              "id": "truck1",
              "geometry": {
                "type": "Point",
                "coordinates": [116.3883, 39.9289]
              },
              "fields": {
                "speed": 60,
                "driver": "John Doe"
              }
            }
            """;
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify SET was called
        ArgumentCaptor<String> collectionCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Tile38Object> objectCaptor = ArgumentCaptor.forClass(Tile38Object.class);
        
        verify(tile38Service, times(1)).set(
            collectionCaptor.capture(), 
            idCaptor.capture(), 
            objectCaptor.capture()
        );
        
        assertEquals("fleet", collectionCaptor.getValue());
        assertEquals("truck1", idCaptor.getValue());
        
        Tile38Object capturedObject = objectCaptor.getValue();
        assertNotNull(capturedObject.getGeometry());
        assertTrue(capturedObject.getGeometry() instanceof Point);
        
        Point point = (Point) capturedObject.getGeometry();
        assertEquals(116.3883, point.getX(), 0.0001);
        assertEquals(39.9289, point.getY(), 0.0001);
        
        assertEquals(60, capturedObject.getFields().get("speed"));
        assertEquals("John Doe", capturedObject.getFields().get("driver"));
    }
    
    @Test
    public void testProcessSetOperationWithWKT() throws Exception {
        // Prepare message with WKT
        String messageJson = """
            {
              "operation": "SET",
              "collection": "fleet",
              "id": "truck2",
              "geometry": {
                "wkt": "POINT(116.3883 39.9289)"
              }
            }
            """;
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify SET was called
        ArgumentCaptor<Tile38Object> objectCaptor = ArgumentCaptor.forClass(Tile38Object.class);
        verify(tile38Service, times(1)).set(eq("fleet"), eq("truck2"), objectCaptor.capture());
        
        Tile38Object capturedObject = objectCaptor.getValue();
        assertNotNull(capturedObject.getGeometry());
        assertTrue(capturedObject.getGeometry() instanceof Point);
        
        Point point = (Point) capturedObject.getGeometry();
        assertEquals(116.3883, point.getX(), 0.0001);
        assertEquals(39.9289, point.getY(), 0.0001);
    }
    
    @Test
    public void testProcessSetOperationWithLineString() throws Exception {
        // Prepare message with LineString
        String messageJson = """
            {
              "operation": "SET",
              "collection": "routes",
              "id": "route1",
              "geometry": {
                "type": "LineString",
                "coordinates": [
                  [116.3883, 39.9289],
                  [116.3984, 39.9389],
                  [116.4085, 39.9489]
                ]
              }
            }
            """;
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify SET was called
        ArgumentCaptor<Tile38Object> objectCaptor = ArgumentCaptor.forClass(Tile38Object.class);
        verify(tile38Service, times(1)).set(eq("routes"), eq("route1"), objectCaptor.capture());
        
        Tile38Object capturedObject = objectCaptor.getValue();
        assertNotNull(capturedObject.getGeometry());
        assertEquals("LineString", capturedObject.getGeometry().getGeometryType());
    }
    
    @Test
    public void testProcessSetOperationWithPolygon() throws Exception {
        // Prepare message with Polygon
        String messageJson = """
            {
              "operation": "SET",
              "collection": "zones",
              "id": "zone1",
              "geometry": {
                "type": "Polygon",
                "coordinates": [
                  [
                    [116.3883, 39.9289],
                    [116.4883, 39.9289],
                    [116.4883, 39.8289],
                    [116.3883, 39.8289],
                    [116.3883, 39.9289]
                  ]
                ]
              }
            }
            """;
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify SET was called
        ArgumentCaptor<Tile38Object> objectCaptor = ArgumentCaptor.forClass(Tile38Object.class);
        verify(tile38Service, times(1)).set(eq("zones"), eq("zone1"), objectCaptor.capture());
        
        Tile38Object capturedObject = objectCaptor.getValue();
        assertNotNull(capturedObject.getGeometry());
        assertEquals("Polygon", capturedObject.getGeometry().getGeometryType());
    }
    
    @Test
    public void testProcessDelOperation() throws Exception {
        // Prepare DEL message
        String messageJson = """
            {
              "operation": "DEL",
              "collection": "fleet",
              "id": "truck1"
            }
            """;
        
        when(tile38Service.del(anyString(), anyString())).thenReturn(true);
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify DEL was called
        verify(tile38Service, times(1)).del("fleet", "truck1");
    }
    
    @Test
    public void testProcessDropOperation() throws Exception {
        // Prepare DROP message
        String messageJson = """
            {
              "operation": "DROP",
              "collection": "fleet"
            }
            """;
        
        when(tile38Service.drop(anyString())).thenReturn(true);
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify DROP was called
        verify(tile38Service, times(1)).drop("fleet");
    }
    
    @Test
    public void testProcessInvalidMessage() {
        // Invalid JSON
        String messageJson = "{ invalid json }";
        
        assertThrows(RuntimeException.class, () -> {
            messageProcessor.processMessage(messageJson);
        });
    }
    
    @Test
    public void testProcessSetWithoutGeometry() throws Exception {
        // Message without geometry
        String messageJson = """
            {
              "operation": "SET",
              "collection": "fleet",
              "id": "truck1"
            }
            """;
        
        assertThrows(RuntimeException.class, () -> {
            messageProcessor.processMessage(messageJson);
        });
    }
    
    @Test
    public void testProcessSetWithExpiration() throws Exception {
        // Message with expiration
        String messageJson = """
            {
              "operation": "SET",
              "collection": "fleet",
              "id": "truck1",
              "geometry": {
                "type": "Point",
                "coordinates": [116.3883, 39.9289]
              },
              "expireAt": 1735689600000
            }
            """;
        
        // Process message
        messageProcessor.processMessage(messageJson);
        
        // Verify SET was called with expiration
        ArgumentCaptor<Tile38Object> objectCaptor = ArgumentCaptor.forClass(Tile38Object.class);
        verify(tile38Service, times(1)).set(eq("fleet"), eq("truck1"), objectCaptor.capture());
        
        Tile38Object capturedObject = objectCaptor.getValue();
        assertNotNull(capturedObject.getExpireAt());
        assertEquals(1735689600000L, capturedObject.getExpireAt().toEpochMilli());
    }
}
