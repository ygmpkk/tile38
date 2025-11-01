package com.tile38.service.persistence;

import com.tile38.model.Tile38Object;
import com.tile38.repository.SpatialRepository;
import com.tile38.config.PersistenceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RdbPersistenceServiceTest {
    
    @Mock
    private SpatialRepository spatialRepository;
    
    @Mock
    private PersistenceProperties persistenceProperties;
    
    @Mock
    private PersistenceProperties.Rdb rdbConfig;
    
    @InjectMocks
    private RdbPersistenceService rdbPersistenceService;
    
    private ObjectMapper objectMapper;
    private GeometryFactory geometryFactory;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        geometryFactory = new GeometryFactory();
        
        // Mock configuration
        when(persistenceProperties.getRdb()).thenReturn(rdbConfig);
        when(rdbConfig.isEnabled()).thenReturn(true);
        
        // Set the object mapper via reflection since it's autowired
        try {
            var field = RdbPersistenceService.class.getDeclaredField("objectMapper");
            field.setAccessible(true);
            field.set(rdbPersistenceService, objectMapper);
        } catch (Exception e) {
            fail("Failed to set object mapper: " + e.getMessage());
        }
    }
    
    @Test
    void testSaveRdbDisabled() {
        // Given
        when(rdbConfig.isEnabled()).thenReturn(false);
        
        // When
        boolean result = rdbPersistenceService.saveToRdb();
        
        // Then
        assertFalse(result);
        verify(spatialRepository, never()).keys();
    }
    
    @Test
    void testSaveRdbEnabled() {
        // Given
        String rdbFilePath = tempDir.resolve("test.rdb").toString();
        when(rdbConfig.getFilePath()).thenReturn(rdbFilePath);
        
        // Mock spatial repository data
        Set<String> keys = new HashSet<>();
        keys.add("collection1");
        when(spatialRepository.keys()).thenReturn(keys);
        
        Map<String, Tile38Object> objects = new HashMap<>();
        Point point = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        Tile38Object obj = Tile38Object.builder()
            .id("obj1")
            .geometry(point)
            .timestamp(System.currentTimeMillis())
            .build();
        objects.put("obj1", obj);
        
        when(spatialRepository.getAll("collection1")).thenReturn(objects);
        
        // When
        boolean result = rdbPersistenceService.saveToRdb();
        
        // Then
        assertTrue(result);
        verify(spatialRepository).keys();
        verify(spatialRepository).getAll("collection1");
        
        // Verify file was created
        assertTrue(java.nio.file.Files.exists(java.nio.file.Paths.get(rdbFilePath)));
    }
    
    @Test
    void testLoadRdbFileNotExists() {
        // Given
        String rdbFilePath = tempDir.resolve("nonexistent.rdb").toString();
        when(rdbConfig.getFilePath()).thenReturn(rdbFilePath);
        
        // When
        boolean result = rdbPersistenceService.loadFromRdb();
        
        // Then
        assertTrue(result); // Should return true even if file doesn't exist
        assertTrue(rdbPersistenceService.isLoaded());
        verify(spatialRepository, never()).flushAll();
    }
    
    @Test
    void testIsLoaded() {
        // Given - service starts not loaded
        assertFalse(rdbPersistenceService.isLoaded());
        
        // When - simulate successful load
        String rdbFilePath = tempDir.resolve("nonexistent.rdb").toString();
        when(rdbConfig.getFilePath()).thenReturn(rdbFilePath);
        rdbPersistenceService.loadFromRdb();
        
        // Then
        assertTrue(rdbPersistenceService.isLoaded());
    }
}