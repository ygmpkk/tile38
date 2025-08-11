package com.tile38.dubbo;

import com.tile38.dubbo.api.Tile38RpcService;
import com.tile38.dubbo.impl.Tile38RpcServiceImpl;
import com.tile38.loader.DataLoader;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.Bounds;
import com.tile38.model.KVData;
import com.tile38.model.FilterCondition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.beans.factory.annotation.Autowired;

import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the enhanced DUBBO RPC interface with KV capabilities
 */
@SpringBootTest
@TestPropertySource(properties = {
    "dubbo.application.name=test-tile38",
    "dubbo.registry.address=N/A",
    "dubbo.protocol.name=dubbo",
    "dubbo.protocol.port=20880"
})
class Tile38RpcServiceTest {

    @Autowired
    private Tile38RpcService tile38RpcService;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();

    @BeforeEach
    void setUp() {
        // Clean the database before each test
        tile38RpcService.flushdb();
    }

    @Test
    void testBasicOperations() {
        // Test ping
        assertEquals("PONG", tile38RpcService.ping());

        // Test set and get
        Map<String, Object> fields = new HashMap<>();
        fields.put("speed", 60);
        fields.put("driver", "John");

        tile38RpcService.set("fleet", "truck1", 33.5, -115.5, fields, null);
        
        Tile38Object object = tile38RpcService.get("fleet", "truck1");
        assertNotNull(object);
        assertEquals("truck1", object.getId());
        assertEquals(60, object.getFields().get("speed"));

        // Test keys
        List<String> keys = tile38RpcService.keys();
        assertTrue(keys.contains("fleet"));

        // Test delete
        assertTrue(tile38RpcService.del("fleet", "truck1"));
        assertNull(tile38RpcService.get("fleet", "truck1"));
    }

    @Test
    void testKVOperations() {
        // Test setWithKV
        Map<String, Object> fields = new HashMap<>();
        fields.put("speed", 65);

        Map<String, String> tags = new HashMap<>();
        tags.put("category", "truck");
        tags.put("type", "delivery");

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("fuel_level", 75.5);
        attributes.put("active", true);
        attributes.put("last_maintenance", "2024-01-15");

        tile38RpcService.setWithKV("fleet", "truck1", 33.5, -115.5, fields, tags, attributes, null);

        // Verify object was created with KV data
        Tile38Object object = tile38RpcService.get("fleet", "truck1");
        assertNotNull(object);
        assertNotNull(object.getKvData());

        // Verify tags
        assertEquals("truck", object.getKvData().getTag("category"));
        assertEquals("delivery", object.getKvData().getTag("type"));

        // Verify attributes
        assertEquals(75.5, object.getKvData().getAttribute("fuel_level"));
        assertEquals(true, object.getKvData().getAttribute("active"));
        assertEquals("2024-01-15", object.getKvData().getAttribute("last_maintenance"));
    }

    @Test
    void testKVDataUpdates() {
        // First create an object
        tile38RpcService.set("fleet", "truck1", 33.5, -115.5, new HashMap<>(), null);

        // Update KV data
        Map<String, String> newTags = new HashMap<>();
        newTags.put("status", "maintenance");
        newTags.put("priority", "high");

        Map<String, Object> newAttributes = new HashMap<>();
        newAttributes.put("fuel_level", 45.0);
        newAttributes.put("active", false);

        boolean updated = tile38RpcService.updateKVData("fleet", "truck1", newTags, newAttributes);
        assertTrue(updated);

        // Verify updates
        Tile38Object object = tile38RpcService.get("fleet", "truck1");
        assertNotNull(object.getKvData());
        assertEquals("maintenance", object.getKvData().getTag("status"));
        assertEquals("high", object.getKvData().getTag("priority"));
        assertEquals(45.0, object.getKvData().getAttribute("fuel_level"));
        assertEquals(false, object.getKvData().getAttribute("active"));

        // Test updating non-existent object
        assertFalse(tile38RpcService.updateKVData("fleet", "nonexistent", newTags, newAttributes));
    }

    @Test
    void testKVDataObjectUpdate() {
        // Create an object
        tile38RpcService.set("fleet", "truck1", 33.5, -115.5, new HashMap<>(), null);

        // Create KV data object
        KVData kvData = new KVData();
        kvData.setTag("category", "van");
        kvData.setTag("status", "active");
        kvData.setAttribute("capacity", 1000);
        kvData.setAttribute("electric", true);

        // Update using KV data object
        boolean updated = tile38RpcService.updateKVDataObject("fleet", "truck1", kvData);
        assertTrue(updated);

        // Verify updates
        Tile38Object object = tile38RpcService.get("fleet", "truck1");
        assertNotNull(object.getKvData());
        assertEquals("van", object.getKvData().getTag("category"));
        assertEquals("active", object.getKvData().getTag("status"));
        assertEquals(1000, object.getKvData().getAttribute("capacity"));
        assertEquals(true, object.getKvData().getAttribute("electric"));
    }

    @Test
    void testNearbySearch() {
        // Create test objects with coordinates close to each other
        tile38RpcService.set("restaurants", "restaurant1", 33.5, -115.5, new HashMap<>(), null);
        tile38RpcService.set("restaurants", "restaurant2", 33.51, -115.51, new HashMap<>(), null);

        // Test basic nearby search with larger radius
        List<SearchResult> results = tile38RpcService.nearby("restaurants", 33.5, -115.5, 50000); // 50km radius
        assertEquals(2, results.size());

        // Verify results
        assertNotNull(results.get(0).getObject());
        assertNotNull(results.get(1).getObject());
    }

    @Test
    void testNearbySearchWithFilter() {
        // Create test objects with KV data
        Map<String, String> italianTags = new HashMap<>();
        italianTags.put("cuisine", "italian");
        italianTags.put("price_range", "moderate");

        Map<String, Object> italianAttrs = new HashMap<>();
        italianAttrs.put("rating", 4.5);
        italianAttrs.put("seats", 80);

        tile38RpcService.setWithKV("restaurants", "restaurant1", 33.5, -115.5, 
                                   new HashMap<>(), italianTags, italianAttrs, null);

        Map<String, String> chineseTags = new HashMap<>();
        chineseTags.put("cuisine", "chinese");
        chineseTags.put("price_range", "low");

        Map<String, Object> chineseAttrs = new HashMap<>();
        chineseAttrs.put("rating", 3.8);
        chineseAttrs.put("seats", 60);

        tile38RpcService.setWithKV("restaurants", "restaurant2", 33.6, -115.4, 
                                   new HashMap<>(), chineseTags, chineseAttrs, null);

        // Create filter for Italian restaurants
        FilterCondition filter = FilterCondition.builder()
                .key("cuisine")
                .operator(FilterCondition.Operator.EQUALS)
                .value("italian")
                .dataType(FilterCondition.DataType.TAG)
                .build();

        // Test filtered search with larger radius
        List<SearchResult> results = tile38RpcService.nearbyWithFilter("restaurants", 33.5, -115.5, 50000, filter);
        assertEquals(1, results.size());
        assertEquals("restaurant1", results.get(0).getObject().getId());

        // Create filter for high-rated restaurants
        FilterCondition ratingFilter = FilterCondition.builder()
                .key("rating")
                .operator(FilterCondition.Operator.GREATER_THAN)
                .value(4.0)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();

        results = tile38RpcService.nearbyWithFilter("restaurants", 33.5, -115.5, 50000, ratingFilter);
        assertEquals(1, results.size());
        assertEquals("restaurant1", results.get(0).getObject().getId());
    }

    @Test
    void testBulkOperations() {
        // Create bulk objects with proper geometry
        Map<String, Tile38Object> objects = new HashMap<>();
        
        for (int i = 1; i <= 3; i++) {
            KVData kvData = new KVData();
            kvData.setTag("type", "truck");
            kvData.setTag("status", "active");
            kvData.setAttribute("number", i);
            kvData.setAttribute("fuel", 80.0 - i * 10);

            Point point = geometryFactory.createPoint(new Coordinate(-115.0 - i, 33.0 + i));

            Tile38Object object = Tile38Object.builder()
                    .id("truck" + i)
                    .geometry(point)  // Add proper geometry
                    .kvData(kvData)
                    .timestamp(System.currentTimeMillis())
                    .build();
            
            objects.put("truck" + i, object);
        }

        // Bulk set
        tile38RpcService.bulkSet("fleet", objects);

        // Verify objects were created
        for (int i = 1; i <= 3; i++) {
            Tile38Object object = tile38RpcService.get("fleet", "truck" + i);
            assertNotNull(object, "Object truck" + i + " should exist");
            assertNotNull(object.getKvData(), "KV data should exist for truck" + i);
            assertEquals("truck", object.getKvData().getTag("type"));
            assertEquals(i, object.getKvData().getAttribute("number"));
        }
    }

    @Test
    void testCollectionOperations() {
        // Create test data
        tile38RpcService.set("fleet", "truck1", 33.5, -115.5, new HashMap<>(), null);
        tile38RpcService.set("fleet", "truck2", 33.6, -115.4, new HashMap<>(), null);

        // Test bounds
        Bounds bounds = tile38RpcService.bounds("fleet");
        assertNotNull(bounds);
        assertTrue(bounds.getMinY() <= 33.5);  // minY = min latitude
        assertTrue(bounds.getMaxY() >= 33.6);  // maxY = max latitude

        // Test drop collection
        assertTrue(tile38RpcService.drop("fleet"));
        assertNull(tile38RpcService.bounds("fleet"));

        // Verify objects are gone
        assertNull(tile38RpcService.get("fleet", "truck1"));
        assertNull(tile38RpcService.get("fleet", "truck2"));
    }
    
    @Test
    void testAdvancedSearchOperations() {
        // Create test objects in a bounding box
        Map<String, String> tags1 = new HashMap<>();
        tags1.put("category", "restaurant");
        tags1.put("type", "italian");
        
        Map<String, Object> attrs1 = new HashMap<>();
        attrs1.put("rating", 4.5);
        attrs1.put("seats", 50);
        
        tile38RpcService.setWithKV("places", "restaurant1", 33.5, -115.5, 
                                   new HashMap<>(), tags1, attrs1, null);
        
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("category", "shop");
        tags2.put("type", "grocery");
        
        Map<String, Object> attrs2 = new HashMap<>();
        attrs2.put("rating", 4.0);
        attrs2.put("area", 1000);
        
        tile38RpcService.setWithKV("places", "shop1", 33.6, -115.4, 
                                   new HashMap<>(), tags2, attrs2, null);
        
        // Test scan with filter
        FilterCondition categoryFilter = FilterCondition.tagEquals("category", "restaurant");
        List<SearchResult> scanResults = tile38RpcService.scan("places", categoryFilter, 10, 0);
        assertEquals(1, scanResults.size());
        assertEquals("restaurant1", scanResults.get(0).getObject().getId());
        
        // Test scan with pagination
        List<SearchResult> allResults = tile38RpcService.scan("places", null, 1, 0);
        assertEquals(1, allResults.size());
        
        allResults = tile38RpcService.scan("places", null, 1, 1);
        assertEquals(1, allResults.size());
        
        // Test intersects with bounding box
        List<SearchResult> intersectsResults = tile38RpcService.intersects("places", 33.0, -116.0, 34.0, -115.0, null);
        assertEquals(2, intersectsResults.size());
        
        // Test within with bounding box
        List<SearchResult> withinResults = tile38RpcService.within("places", 33.0, -116.0, 34.0, -115.0, null);
        assertEquals(2, withinResults.size());
        
        // Test intersects with filter
        intersectsResults = tile38RpcService.intersects("places", 33.0, -116.0, 34.0, -115.0, categoryFilter);
        assertEquals(1, intersectsResults.size());
        assertEquals("restaurant1", intersectsResults.get(0).getObject().getId());
    }
    
    @Test
    void testDataLoadingOperations() throws Exception {
        // Test generateTestData
        CompletableFuture<DataLoader.LoadResult> future = tile38RpcService.generateTestData(
                "test_collection", 10, 30.0, 35.0, -120.0, -115.0);
        
        DataLoader.LoadResult result = future.get();
        assertTrue(result.isSuccess());
        assertEquals(10, result.getRecordsLoaded());
        
        // Verify test data was loaded
        List<SearchResult> results = tile38RpcService.scan("test_collection", null, 20, 0);
        assertEquals(10, results.size());
        
        // Verify objects have proper KV data
        SearchResult firstResult = results.get(0);
        assertNotNull(firstResult.getObject().getKvData());
        assertNotNull(firstResult.getObject().getKvData().getTag("type"));
    }
}