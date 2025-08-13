package com.tile38.integration;

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

/**
 * Integration tests for KV data functionality
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {"server.port=0"})
public class KVDataIntegrationTest {
    
    @LocalServerPort
    private int port;
    
    private TestRestTemplate restTemplate;
    private String baseUrl;
    
    @BeforeEach
    public void setUp() {
        restTemplate = new TestRestTemplate();
        baseUrl = "http://localhost:" + port + "/api/v1";
        
        // Clear the database
        restTemplate.postForEntity(baseUrl + "/flushdb", null, Map.class);
    }
    
    @Test
    public void testKVDataOperations() {
        String collectionKey = "restaurants";
        String objectId = "restaurant1";
        
        // 1. Create an object with KV data
        Map<String, Object> createRequest = new HashMap<>();
        createRequest.put("lat", 33.5);
        createRequest.put("lon", -115.5);
        
        // Add tags
        Map<String, String> tags = new HashMap<>();
        tags.put("category", "restaurant");
        tags.put("cuisine", "italian");
        createRequest.put("tags", tags);
        
        // Add attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("rating", 4.5);
        attributes.put("price_range", 25);
        attributes.put("open", true);
        createRequest.put("attributes", attributes);
        
        ResponseEntity<Map> createResponse = restTemplate.postForEntity(
            baseUrl + "/keys/" + collectionKey + "/objects/" + objectId, 
            createRequest, 
            Map.class);
        
        assertEquals(HttpStatus.OK, createResponse.getStatusCode());
        assertTrue((Boolean) createResponse.getBody().get("ok"));
        
        // 2. Get the object and verify KV data
        ResponseEntity<Map> getResponse = restTemplate.getForEntity(
            baseUrl + "/keys/" + collectionKey + "/objects/" + objectId,
            Map.class);
        
        assertEquals(HttpStatus.OK, getResponse.getStatusCode());
        assertTrue((Boolean) getResponse.getBody().get("ok"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> responseData = (Map<String, Object>) getResponse.getBody().get("data");
        assertNotNull(responseData);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> objectData = (Map<String, Object>) responseData.get("object");
        assertNotNull(objectData);
        
        // 3. Update KV data
        Map<String, Object> updateRequest = new HashMap<>();
        
        Map<String, String> newTags = new HashMap<>();
        newTags.put("atmosphere", "romantic");
        updateRequest.put("tags", newTags);
        
        Map<String, Object> newAttributes = new HashMap<>();
        newAttributes.put("rating", 4.8);
        newAttributes.put("recently_renovated", true);
        updateRequest.put("attributes", newAttributes);
        
        ResponseEntity<Map> updateResponse = restTemplate.exchange(
            baseUrl + "/keys/" + collectionKey + "/objects/" + objectId + "/kv",
            org.springframework.http.HttpMethod.PUT,
            new org.springframework.http.HttpEntity<>(updateRequest),
            Map.class);
        
        assertEquals(HttpStatus.OK, updateResponse.getStatusCode());
        assertTrue((Boolean) updateResponse.getBody().get("ok"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> updateResponseData = (Map<String, Object>) updateResponse.getBody().get("data");
        assertEquals(1, updateResponseData.get("updated"));
        
        // 4. Verify KV data was updated
        ResponseEntity<Map> getUpdatedResponse = restTemplate.getForEntity(
            baseUrl + "/keys/" + collectionKey + "/objects/" + objectId,
            Map.class);
        
        assertEquals(HttpStatus.OK, getUpdatedResponse.getStatusCode());
    }
    
    @Test
    public void testKVFilteringInNearbySearch() {
        String collectionKey = "test_restaurants";
        
        // Create multiple restaurants with different attributes
        createTestRestaurant(collectionKey, "restaurant1", 33.5, -115.5, "italian", 4.5, 25);
        createTestRestaurant(collectionKey, "restaurant2", 33.51, -115.51, "chinese", 4.0, 15);
        createTestRestaurant(collectionKey, "restaurant3", 33.49, -115.49, "italian", 3.5, 35);
        
        // 1. Test simple tag filtering
        String nearbyUrl = baseUrl + "/keys/" + collectionKey + "/nearby" +
                          "?lat=33.5&lon=-115.5&radius=10000&filter=tag:cuisine=italian";
        
        ResponseEntity<Map> tagFilterResponse = restTemplate.getForEntity(nearbyUrl, Map.class);
        assertEquals(HttpStatus.OK, tagFilterResponse.getStatusCode());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> responseData = (Map<String, Object>) tagFilterResponse.getBody().get("data");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tagResults = (List<Map<String, Object>>) responseData.get("objects");
        assertEquals(2, tagResults.size()); // Should find 2 Italian restaurants
        
        // 2. Test attribute filtering
        String attributeFilterUrl = baseUrl + "/keys/" + collectionKey + "/nearby" +
                                   "?lat=33.5&lon=-115.5&radius=10000&filter=attr:rating>4.0";
        
        ResponseEntity<Map> attrFilterResponse = restTemplate.getForEntity(attributeFilterUrl, Map.class);
        assertEquals(HttpStatus.OK, attrFilterResponse.getStatusCode());
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> attrResults = (List<Map<String, Object>>) attrFilterResponse.getBody().get("objects");
        assertEquals(1, attrResults.size()); // Should find 1 restaurant with rating > 4.0
    }
    
    @Test 
    public void testComplexFiltering() {
        String collectionKey = "complex_test";
        
        // Create test data
        createTestRestaurant(collectionKey, "rest1", 33.5, -115.5, "italian", 4.5, 25);
        createTestRestaurant(collectionKey, "rest2", 33.51, -115.51, "italian", 3.8, 20);
        createTestRestaurant(collectionKey, "rest3", 33.49, -115.49, "chinese", 4.2, 15);
        
        // Test complex filtering with POST endpoint
        Map<String, Object> filterRequest = new HashMap<>();
        filterRequest.put("conditions", List.of(
            Map.of("key", "cuisine", "operator", "EQUALS", "value", "italian", "dataType", "TAG"),
            Map.of("key", "rating", "operator", "GREATER_THAN", "value", 4.0, "dataType", "ATTRIBUTE")
        ));
        filterRequest.put("logicalOperator", "AND");
        filterRequest.put("lat", 33.5);
        filterRequest.put("lon", -115.5);
        filterRequest.put("radius", 10000.0);
        
        ResponseEntity<Map> complexFilterResponse = restTemplate.postForEntity(
            baseUrl + "/keys/" + collectionKey + "/nearby/filter",
            filterRequest,
            Map.class);
        
        assertEquals(HttpStatus.OK, complexFilterResponse.getStatusCode());
        
        @SuppressWarnings("unchecked")
        Map<String, Object> complexResponseData = (Map<String, Object>) complexFilterResponse.getBody().get("data");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> complexResults = (List<Map<String, Object>>) complexResponseData.get("objects");
        assertEquals(1, complexResults.size()); // Should find 1 Italian restaurant with rating > 4.0
    }
    
    private void createTestRestaurant(String collection, String id, double lat, double lon, 
                                     String cuisine, double rating, int price) {
        Map<String, Object> request = new HashMap<>();
        request.put("lat", lat);
        request.put("lon", lon);
        
        Map<String, String> tags = new HashMap<>();
        tags.put("cuisine", cuisine);
        tags.put("category", "restaurant");
        request.put("tags", tags);
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("rating", rating);
        attributes.put("price_range", price);
        request.put("attributes", attributes);
        
        restTemplate.postForEntity(
            baseUrl + "/keys/" + collection + "/objects/" + id,
            request,
            Map.class);
    }
}