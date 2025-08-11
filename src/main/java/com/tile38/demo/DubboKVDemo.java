package com.tile38.demo;

import com.tile38.dubbo.api.Tile38RpcService;
import com.tile38.model.Tile38Object;
import com.tile38.model.SearchResult;
import com.tile38.model.KVData;
import com.tile38.model.FilterCondition;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Demo class showing DUBBO RPC interface KV capabilities
 * Demonstrates practical usage of enhanced KV operations
 */
@Component
@Slf4j
public class DubboKVDemo implements CommandLineRunner {

    @Autowired
    private Tile38RpcService tile38RpcService;

    @Override
    public void run(String... args) throws Exception {
        log.info("🚀 Starting DUBBO KV Capabilities Demo");
        
        // Clean slate
        tile38RpcService.flushdb();
        
        // Demo 1: Create restaurants with KV data
        demonstrateKVCreation();
        
        // Demo 2: Update KV data
        demonstrateKVUpdates();
        
        // Demo 3: Advanced filtering
        demonstrateAdvancedFiltering();
        
        // Demo 4: Bulk operations
        demonstrateBulkOperations();
        
        log.info("✅ DUBBO KV Capabilities Demo completed successfully!");
    }

    private void demonstrateKVCreation() {
        log.info("\n📝 Demo 1: Creating Objects with KV Data");
        
        // Create Italian restaurant
        Map<String, String> italianTags = new HashMap<>();
        italianTags.put("cuisine", "italian");
        italianTags.put("category", "restaurant");
        italianTags.put("price_range", "moderate");
        
        Map<String, Object> italianAttrs = new HashMap<>();
        italianAttrs.put("rating", 4.5);
        italianAttrs.put("seats", 80);
        italianAttrs.put("open", true);
        italianAttrs.put("established", 2018);
        
        tile38RpcService.setWithKV("restaurants", "marios_pizza", 33.5, -115.5,
                                   new HashMap<>(), italianTags, italianAttrs, null);
        log.info("✓ Created Italian restaurant with tags: {} and attributes: {}", 
                italianTags.size(), italianAttrs.size());
        
        // Create Chinese restaurant
        Map<String, String> chineseTags = new HashMap<>();
        chineseTags.put("cuisine", "chinese");
        chineseTags.put("category", "restaurant");
        chineseTags.put("price_range", "low");
        
        Map<String, Object> chineseAttrs = new HashMap<>();
        chineseAttrs.put("rating", 3.8);
        chineseAttrs.put("seats", 60);
        chineseAttrs.put("open", true);
        chineseAttrs.put("delivery", true);
        
        tile38RpcService.setWithKV("restaurants", "golden_dragon", 33.6, -115.4,
                                   new HashMap<>(), chineseTags, chineseAttrs, null);
        log.info("✓ Created Chinese restaurant with tags: {} and attributes: {}", 
                chineseTags.size(), chineseAttrs.size());
        
        // Verify creation
        Tile38Object restaurant = tile38RpcService.get("restaurants", "marios_pizza");
        log.info("✓ Retrieved restaurant: {} with KV data: {}", 
                restaurant.getId(), restaurant.getKvData() != null);
    }

    private void demonstrateKVUpdates() {
        log.info("\n🔄 Demo 2: KV Data Updates");
        
        // Update Italian restaurant's status
        Map<String, String> updateTags = new HashMap<>();
        updateTags.put("status", "featured");
        updateTags.put("promotion", "happy_hour");
        
        Map<String, Object> updateAttrs = new HashMap<>();
        updateAttrs.put("rating", 4.7); // Improved rating
        updateAttrs.put("last_inspection", "2024-08-11");
        updateAttrs.put("wifi", true);
        
        boolean updated = tile38RpcService.updateKVData("restaurants", "marios_pizza", 
                                                       updateTags, updateAttrs);
        log.info("✓ Updated restaurant KV data: {}", updated);
        
        // Verify update
        Tile38Object updated_restaurant = tile38RpcService.get("restaurants", "marios_pizza");
        if (updated_restaurant.getKvData() != null) {
            log.info("✓ New rating: {}", updated_restaurant.getKvData().getAttribute("rating"));
            log.info("✓ New status: {}", updated_restaurant.getKvData().getTag("status"));
        }
    }

    private void demonstrateAdvancedFiltering() {
        log.info("\n🔍 Demo 3: Advanced KV Filtering");
        
        // Filter 1: Find Italian restaurants
        FilterCondition cuisineFilter = FilterCondition.builder()
                .key("cuisine")
                .operator(FilterCondition.Operator.EQUALS)
                .value("italian")
                .dataType(FilterCondition.DataType.TAG)
                .build();
        
        List<SearchResult> italianResults = tile38RpcService.nearbyWithFilter(
                "restaurants", 33.5, -115.5, 50000, cuisineFilter);
        log.info("✓ Found {} Italian restaurants", italianResults.size());
        
        // Filter 2: Find high-rated restaurants (rating > 4.0)
        FilterCondition ratingFilter = FilterCondition.builder()
                .key("rating")
                .operator(FilterCondition.Operator.GREATER_THAN)
                .value(4.0)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();
        
        List<SearchResult> highRatedResults = tile38RpcService.nearbyWithFilter(
                "restaurants", 33.5, -115.5, 50000, ratingFilter);
        log.info("✓ Found {} high-rated restaurants", highRatedResults.size());
        
        // Complex filter: Italian restaurants with rating > 4.0 AND open
        FilterCondition openFilter = FilterCondition.builder()
                .key("open")
                .operator(FilterCondition.Operator.EQUALS)
                .value(true)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();
        
        FilterCondition complexFilter = FilterCondition.builder()
                .conditions(List.of(cuisineFilter, ratingFilter, openFilter))
                .logicalOperator(FilterCondition.LogicalOperator.AND)
                .build();
        
        List<SearchResult> complexResults = tile38RpcService.nearbyWithFilter(
                "restaurants", 33.5, -115.5, 50000, complexFilter);
        log.info("✓ Found {} restaurants matching complex criteria", complexResults.size());
        
        if (!complexResults.isEmpty()) {
            SearchResult result = complexResults.get(0);
            log.info("✓ Example match: {} (rating: {})", 
                    result.getObject().getId(),
                    result.getObject().getKvData().getAttribute("rating"));
        }
    }

    private void demonstrateBulkOperations() {
        log.info("\n📦 Demo 4: Bulk Operations");
        
        // Create fleet objects in bulk
        Map<String, Tile38Object> vehicles = new HashMap<>();
        
        for (int i = 1; i <= 5; i++) {
            KVData kvData = new KVData();
            kvData.setTag("type", "delivery_truck");
            kvData.setTag("status", "active");
            kvData.setTag("driver", "driver_" + i);
            
            kvData.setAttribute("truck_number", i);
            kvData.setAttribute("fuel_level", 100.0 - i * 10);
            kvData.setAttribute("last_maintenance", "2024-08-01");
            kvData.setAttribute("max_capacity", 1000);
            
            // Create geometry (simple point)
            org.locationtech.jts.geom.GeometryFactory gf = new org.locationtech.jts.geom.GeometryFactory();
            org.locationtech.jts.geom.Point point = gf.createPoint(
                new org.locationtech.jts.geom.Coordinate(-115.0 - i * 0.01, 33.0 + i * 0.01));
            
            Tile38Object vehicle = Tile38Object.builder()
                    .id("truck_" + i)
                    .geometry(point)
                    .kvData(kvData)
                    .timestamp(System.currentTimeMillis())
                    .build();
            
            vehicles.put("truck_" + i, vehicle);
        }
        
        // Bulk insert
        tile38RpcService.bulkSet("fleet", vehicles);
        log.info("✓ Bulk inserted {} vehicles", vehicles.size());
        
        // Verify bulk operation
        for (int i = 1; i <= 5; i++) {
            Tile38Object vehicle = tile38RpcService.get("fleet", "truck_" + i);
            if (vehicle != null && vehicle.getKvData() != null) {
                log.info("✓ Verified truck_{}: fuel={}, driver={}", i,
                        vehicle.getKvData().getAttribute("fuel_level"),
                        vehicle.getKvData().getTag("driver"));
            }
        }
        
        // Find low-fuel vehicles
        FilterCondition lowFuelFilter = FilterCondition.builder()
                .key("fuel_level")
                .operator(FilterCondition.Operator.LESS_THAN)
                .value(80.0)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();
        
        List<SearchResult> lowFuelVehicles = tile38RpcService.nearbyWithFilter(
                "fleet", 33.0, -115.0, 100000, lowFuelFilter);
        log.info("✓ Found {} vehicles with low fuel", lowFuelVehicles.size());
        
        // Show statistics
        List<String> collections = tile38RpcService.keys();
        log.info("✓ Total collections: {}", collections);
        log.info("✓ Server stats: {}", tile38RpcService.stats().split("\n")[0]); // First line only
    }
}