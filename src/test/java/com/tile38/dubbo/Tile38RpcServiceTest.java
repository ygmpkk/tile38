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

        // Test set and get with polygon geometry
        Map<String, Object> fields = new HashMap<>();
        fields.put("speed", 60);
        fields.put("driver", "John");

        // Create a simple point geometry for the test
        Point geometry = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        
        tile38RpcService.set("fleet", "truck1", geometry, fields, null);
        
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
        // Test setWithKVData (polygon-centric approach)
        Map<String, Object> fields = new HashMap<>();
        fields.put("speed", 65);

        // Create KV data directly 
        KVData kvData = new KVData();
        kvData.setTag("category", "truck");
        kvData.setTag("type", "delivery");
        kvData.setAttribute("fuel_level", 75.5);
        kvData.setAttribute("active", true);
        kvData.setAttribute("last_maintenance", "2024-01-15");

        // Create a simple point geometry 
        Point geometry = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));

        tile38RpcService.setWithKVData("fleet", "truck1", geometry, fields, kvData, null);

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
        // First create a polygon object
        Point geometry = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        tile38RpcService.set("fleet", "truck1", geometry, new HashMap<>(), null);

        // Update KV data using KVData object (polygon-centric approach)
        KVData newKvData = new KVData();
        newKvData.setTag("status", "maintenance");
        newKvData.setTag("priority", "high");
        newKvData.setAttribute("fuel_level", 45.0);
        newKvData.setAttribute("active", false);

        boolean updated = tile38RpcService.updateKVData("fleet", "truck1", newKvData);
        assertTrue(updated);

        // Verify updates
        Tile38Object object = tile38RpcService.get("fleet", "truck1");
        assertNotNull(object.getKvData());
        assertEquals("maintenance", object.getKvData().getTag("status"));
        assertEquals("high", object.getKvData().getTag("priority"));
        assertEquals(45.0, object.getKvData().getAttribute("fuel_level"));
        assertEquals(false, object.getKvData().getAttribute("active"));

        // Test updating non-existent object
        assertFalse(tile38RpcService.updateKVData("fleet", "nonexistent", newKvData));
    }

    @Test
    void testNearbySearch() {
        // Create test polygon objects with coordinates close to each other
        Point geometry1 = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        Point geometry2 = geometryFactory.createPoint(new Coordinate(-115.51, 33.51));
        
        tile38RpcService.set("restaurants", "restaurant1", geometry1, new HashMap<>(), null);
        tile38RpcService.set("restaurants", "restaurant2", geometry2, new HashMap<>(), null);

        // Test basic nearby search with larger radius
        Point centerPoint = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        List<SearchResult> results = tile38RpcService.nearby("restaurants", centerPoint, 50000); // 50km radius
        assertEquals(2, results.size());

        // Verify results
        assertNotNull(results.get(0).getObject());
        assertNotNull(results.get(1).getObject());
    }

    @Test
    void testNearbySearchWithFilter() {
        // Create test polygon objects with KV data
        
        // Italian restaurant
        Point geometry1 = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        KVData italianKvData = new KVData();
        italianKvData.setTag("cuisine", "italian");
        italianKvData.setTag("price_range", "moderate");
        italianKvData.setAttribute("rating", 4.5);
        italianKvData.setAttribute("seats", 80);

        tile38RpcService.setWithKVData("restaurants", "restaurant1", geometry1, 
                                      new HashMap<>(), italianKvData, null);

        // Chinese restaurant
        Point geometry2 = geometryFactory.createPoint(new Coordinate(-115.4, 33.6));
        KVData chineseKvData = new KVData();
        chineseKvData.setTag("cuisine", "chinese");
        chineseKvData.setTag("price_range", "low");
        chineseKvData.setAttribute("rating", 3.8);
        chineseKvData.setAttribute("seats", 60);

        tile38RpcService.setWithKVData("restaurants", "restaurant2", geometry2, 
                                      new HashMap<>(), chineseKvData, null);

        // Create filter for Italian restaurants
        FilterCondition filter = FilterCondition.builder()
                .key("cuisine")
                .operator(FilterCondition.Operator.EQUALS)
                .value("italian")
                .dataType(FilterCondition.DataType.TAG)
                .build();

        // Test filtered search with larger radius
        Point centerPoint = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        List<SearchResult> results = tile38RpcService.nearbyWithFilter("restaurants", centerPoint, 50000, filter);
        assertEquals(1, results.size());
        assertEquals("restaurant1", results.get(0).getObject().getId());

        // Create filter for high-rated restaurants
        FilterCondition ratingFilter = FilterCondition.builder()
                .key("rating")
                .operator(FilterCondition.Operator.GREATER_THAN)
                .value(4.0)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();

        results = tile38RpcService.nearbyWithFilter("restaurants", centerPoint, 50000, ratingFilter);
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
        // Create test polygon data
        Point geometry1 = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        Point geometry2 = geometryFactory.createPoint(new Coordinate(-115.4, 33.6));
        
        tile38RpcService.set("fleet", "truck1", geometry1, new HashMap<>(), null);
        tile38RpcService.set("fleet", "truck2", geometry2, new HashMap<>(), null);

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
        // Create test polygon objects in a bounding box
        
        // Restaurant
        Point geometry1 = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        KVData kvData1 = new KVData();
        kvData1.setTag("category", "restaurant");
        kvData1.setTag("type", "italian");
        kvData1.setAttribute("rating", 4.5);
        kvData1.setAttribute("seats", 50);
        
        tile38RpcService.setWithKVData("places", "restaurant1", geometry1, 
                                      new HashMap<>(), kvData1, null);
        
        // Shop
        Point geometry2 = geometryFactory.createPoint(new Coordinate(-115.4, 33.6));
        KVData kvData2 = new KVData();
        kvData2.setTag("category", "shop");
        kvData2.setTag("type", "grocery");
        kvData2.setAttribute("rating", 4.0);
        kvData2.setAttribute("area", 1000);
        
        tile38RpcService.setWithKVData("places", "shop1", geometry2, 
                                      new HashMap<>(), kvData2, null);
        
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