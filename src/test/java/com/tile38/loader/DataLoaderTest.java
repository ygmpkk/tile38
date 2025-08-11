package com.tile38.loader;

import com.tile38.loader.impl.DataLoaderImpl;
import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataLoaderTest {
    
    @Mock
    private Tile38Service tile38Service;
    
    @InjectMocks
    private DataLoaderImpl dataLoader;
    
    @Test
    void testGenerateTestData() throws Exception {
        // Given
        String collectionName = "test_collection";
        int numberOfRecords = 1000;
        double minLat = 30.0;
        double maxLat = 40.0;
        double minLon = -120.0;
        double maxLon = -110.0;
        
        // When
        CompletableFuture<DataLoader.LoadResult> future = dataLoader.generateTestData(
            collectionName, numberOfRecords, minLat, maxLat, minLon, maxLon);
        
        DataLoader.LoadResult result = future.get();
        
        // Then
        assertTrue(result.isSuccess());
        assertEquals(numberOfRecords, result.getRecordsLoaded());
        assertTrue(result.getDurationMs() > 0);
        assertNotNull(result.getMessage());
        
        // Verify that bulkSet was called with the generated data
        verify(tile38Service, atLeastOnce()).bulkSet(eq(collectionName), any(Map.class));
    }
    
    @Test
    void testGenerateTestDataSmallBatch() throws Exception {
        // Given - small batch that fits in one bulk operation
        String collectionName = "small_test_collection";
        int numberOfRecords = 100;
        double minLat = 30.0;
        double maxLat = 40.0;
        double minLon = -120.0;
        double maxLon = -110.0;
        
        // When
        CompletableFuture<DataLoader.LoadResult> future = dataLoader.generateTestData(
            collectionName, numberOfRecords, minLat, maxLat, minLon, maxLon);
        
        DataLoader.LoadResult result = future.get();
        
        // Then
        assertTrue(result.isSuccess());
        assertEquals(numberOfRecords, result.getRecordsLoaded());
        
        // Verify that bulkSet was called exactly once for small batch
        verify(tile38Service, times(1)).bulkSet(eq(collectionName), any(Map.class));
    }
    
    @Test 
    void testLoadFromJsonFileNotFound() throws Exception {
        // Given
        String nonExistentFilePath = "/non/existent/file.json";
        
        // When
        CompletableFuture<DataLoader.LoadResult> future = dataLoader.loadFromJson(nonExistentFilePath);
        DataLoader.LoadResult result = future.get();
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals(0, result.getRecordsLoaded());
        assertTrue(result.getMessage().contains("File not found"));
    }
    
    @Test
    void testLoadFromCsvFileNotFound() throws Exception {
        // Given
        String nonExistentFilePath = "/non/existent/file.csv";
        
        // When
        CompletableFuture<DataLoader.LoadResult> future = dataLoader.loadFromCsv(nonExistentFilePath);
        DataLoader.LoadResult result = future.get();
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals(0, result.getRecordsLoaded());
        assertTrue(result.getMessage().contains("Failed to load CSV"));
    }
    
    @Test
    void testLoadResultToString() {
        // Given
        DataLoader.LoadResult result = new DataLoader.LoadResult(
            true, 1000, 5000, "Successfully loaded 1000 records");
        
        // When
        String resultString = result.toString();
        
        // Then
        assertNotNull(resultString);
        assertTrue(resultString.contains("success=true"));
        assertTrue(resultString.contains("records=1000"));
        assertTrue(resultString.contains("duration=5000ms"));
        assertTrue(resultString.contains("Successfully loaded 1000 records"));
    }
}