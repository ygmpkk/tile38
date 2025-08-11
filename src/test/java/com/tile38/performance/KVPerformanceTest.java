package com.tile38.performance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.TestPropertySource;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Performance tests for KV data with million-level datasets
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {"server.port=0"})
public class KVPerformanceTest {
    
    @LocalServerPort
    private int port;
    
    private TestRestTemplate restTemplate;
    private String baseUrl;
    private Random random = new Random();
    
    @BeforeEach
    public void setUp() {
        restTemplate = new TestRestTemplate();
        baseUrl = "http://localhost:" + port + "/api/v1";
        
        // Clear the database
        restTemplate.postForEntity(baseUrl + "/flushdb", null, Map.class);
    }
    
    @Test
    public void testLargeDatasetWithKVFiltering() {
        String collection = "performance_test";
        int recordCount = 10000; // 10K records for CI performance
        
        System.out.println("Testing KV data performance with " + recordCount + " records");
        
        // 1. Generate large dataset with KV data
        long startTime = System.currentTimeMillis();
        
        ResponseEntity<Map> generateResponse = restTemplate.postForEntity(
            baseUrl + "/generate/test-data?collection=" + collection + 
            "&records=" + recordCount + 
            "&minLat=30.0&maxLat=40.0&minLon=-120.0&maxLon=-110.0",
            null,
            Map.class);
        
        long generateTime = System.currentTimeMillis() - startTime;
        
        assertEquals(HttpStatus.OK, generateResponse.getStatusCode());
        assertTrue((Boolean) generateResponse.getBody().get("ok"));
        assertEquals(recordCount, generateResponse.getBody().get("records_generated"));
        
        System.out.println("Generated " + recordCount + " records in " + generateTime + "ms");
        System.out.println("Throughput: " + (recordCount * 1000 / generateTime) + " records/second");
        
        // 2. Since generated objects already have KV data, we can skip the manual KV addition
        // and directly test filtering performance
        System.out.println("Generated objects include KV data by default");
        
        // 3. Test filtering performance on the large dataset
        testFilteringPerformance(collection, recordCount);
        
        // 4. Test memory usage
        testMemoryUsage();
        
        // 5. Test KV data update on existing objects
        testKVUpdatePerformance(collection);
    }
    
    private void testKVUpdatePerformance(String collection) {
        System.out.println("\nTesting KV update performance...");
        
        int updateCount = 100;
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < updateCount; i++) {
            String objectId = "obj" + i;
            
            Map<String, Object> kvRequest = new HashMap<>();
            
            // Update tags
            Map<String, String> tags = new HashMap<>();
            tags.put("updated", "true");
            tags.put("batch", "performance_test");
            kvRequest.put("tags", tags);
            
            // Update attributes
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("updated_at", System.currentTimeMillis());
            attributes.put("update_count", i + 1);
            kvRequest.put("attributes", attributes);
            
            ResponseEntity<Map> updateResponse = restTemplate.exchange(
                baseUrl + "/keys/" + collection + "/objects/" + objectId + "/kv",
                org.springframework.http.HttpMethod.PUT,
                new org.springframework.http.HttpEntity<>(kvRequest),
                Map.class);
            
            assertEquals(HttpStatus.OK, updateResponse.getStatusCode());
        }
        
        long updateTime = System.currentTimeMillis() - startTime;
        System.out.println("Updated KV data for " + updateCount + " objects in " + updateTime + "ms");
        System.out.println("KV update throughput: " + (updateCount * 1000 / updateTime) + " updates/second");
    }
    
    private void testFilteringPerformance(String collection, int totalRecords) {
        System.out.println("\nTesting filtering performance...");
        
        // Test 1: Simple tag filtering
        long filterStartTime = System.currentTimeMillis();
        
        ResponseEntity<Map> tagFilterResponse = restTemplate.getForEntity(
            baseUrl + "/keys/" + collection + "/nearby?lat=35.0&lon=-115.0&radius=100000&filter=tag:category=retail",
            Map.class);
        
        long tagFilterTime = System.currentTimeMillis() - filterStartTime;
        
        assertEquals(HttpStatus.OK, tagFilterResponse.getStatusCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tagResults = (List<Map<String, Object>>) tagFilterResponse.getBody().get("objects");
        
        System.out.println("Tag filter (category=retail) found " + tagResults.size() + " results in " + tagFilterTime + "ms");
        
        // Test 2: Numeric attribute filtering
        filterStartTime = System.currentTimeMillis();
        
        ResponseEntity<Map> attrFilterResponse = restTemplate.getForEntity(
            baseUrl + "/keys/" + collection + "/nearby?lat=35.0&lon=-115.0&radius=100000&filter=attr:priority>5",
            Map.class);
        
        long attrFilterTime = System.currentTimeMillis() - filterStartTime;
        
        assertEquals(HttpStatus.OK, attrFilterResponse.getStatusCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> attrResults = (List<Map<String, Object>>) attrFilterResponse.getBody().get("objects");
        
        System.out.println("Attribute filter (priority>5) found " + attrResults.size() + " results in " + attrFilterTime + "ms");
        
        // Test 3: Complex filtering
        filterStartTime = System.currentTimeMillis();
        
        Map<String, Object> complexFilter = new HashMap<>();
        complexFilter.put("conditions", List.of(
            Map.of("key", "category", "operator", "EQUALS", "value", "service", "dataType", "TAG"),
            Map.of("key", "active", "operator", "EQUALS", "value", true, "dataType", "ATTRIBUTE")
        ));
        complexFilter.put("logicalOperator", "AND");
        
        ResponseEntity<Map> complexFilterResponse = restTemplate.postForEntity(
            baseUrl + "/keys/" + collection + "/nearby/filter?lat=35.0&lon=-115.0&radius=100000",
            complexFilter,
            Map.class);
        
        long complexFilterTime = System.currentTimeMillis() - filterStartTime;
        
        assertEquals(HttpStatus.OK, complexFilterResponse.getStatusCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> complexResults = (List<Map<String, Object>>) complexFilterResponse.getBody().get("objects");
        
        System.out.println("Complex filter (category=service AND active=true) found " + complexResults.size() + " results in " + complexFilterTime + "ms");
        
        // Performance assertions
        assertTrue(tagFilterTime < 1000, "Tag filtering should complete in under 1 second");
        assertTrue(attrFilterTime < 1000, "Attribute filtering should complete in under 1 second");
        assertTrue(complexFilterTime < 2000, "Complex filtering should complete in under 2 seconds");
    }
    
    private void testMemoryUsage() {
        System.out.println("\nTesting memory usage...");
        
        // Get runtime memory info
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();
        
        System.out.println("Memory usage:");
        System.out.println("  Used: " + formatBytes(usedMemory));
        System.out.println("  Total: " + formatBytes(totalMemory));
        System.out.println("  Max: " + formatBytes(maxMemory));
        System.out.println("  Free: " + formatBytes(freeMemory));
        System.out.println("  Usage: " + String.format("%.1f%%", (usedMemory * 100.0 / totalMemory)));
        
        // Get application statistics
        ResponseEntity<String> statsResponse = restTemplate.getForEntity(baseUrl + "/stats", String.class);
        assertEquals(HttpStatus.OK, statsResponse.getStatusCode());
        
        System.out.println("\nApplication stats:");
        System.out.println(statsResponse.getBody());
        
        // Memory efficiency assertion (should use less than 80% of available memory)
        assertTrue(usedMemory < totalMemory * 0.8, 
                  "Memory usage should be under 80% of allocated memory");
    }
    
    private String getRandomCategory() {
        String[] categories = {"retail", "service", "food", "entertainment", "healthcare", "education"};
        return categories[random.nextInt(categories.length)];
    }
    
    private String getRandomType() {
        String[] types = {"premium", "standard", "basic", "vip", "regular"};
        return types[random.nextInt(types.length)];
    }
    
    private String getRandomStatus() {
        String[] statuses = {"active", "inactive", "pending", "verified", "suspended"};
        return statuses[random.nextInt(statuses.length)];
    }
    
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
}