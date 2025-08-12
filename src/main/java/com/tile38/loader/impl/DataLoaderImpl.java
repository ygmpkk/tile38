package com.tile38.loader.impl;

import com.tile38.loader.DataLoader;
import com.tile38.repository.data.DataSource;
import com.tile38.repository.data.RepositoryFactory;
import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import com.tile38.model.KVData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.time.Instant;

/**
 * Implementation of DataLoader for efficient bulk loading of million-level data
 */
@Component
public class DataLoaderImpl implements DataLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLoaderImpl.class);
    
    @Autowired
    private Tile38Service tile38Service;
    
    @Autowired
    private RepositoryFactory repositoryFactory;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();
    
    // Batch size for processing - tuned for memory efficiency
    private static final int BATCH_SIZE = 10000;
    
    @Override
    public CompletableFuture<LoadResult> loadFromJson(String filePath) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting JSON data load from: {}", filePath);
            long startTime = System.currentTimeMillis();
            
            try {
                File file = new File(filePath);
                if (!file.exists()) {
                    return new LoadResult(false, 0, 0, "File not found: " + filePath);
                }
                
                JsonNode root = objectMapper.readTree(file);
                long totalRecords = 0;
                
                // Assume JSON structure: { "collection_name": [ {objects...} ] }
                var fieldsIterator = root.fields();
                while (fieldsIterator.hasNext()) {
                    var collectionEntry = fieldsIterator.next();
                    String collectionName = collectionEntry.getKey();
                    JsonNode objectsArray = collectionEntry.getValue();
                    
                    if (!objectsArray.isArray()) {
                        continue;
                    }
                    
                    Map<String, Tile38Object> batch = new HashMap<>();
                    int batchCount = 0;
                    
                    for (JsonNode objNode : objectsArray) {
                        try {
                            String id = objNode.get("id").asText();
                            double lat = objNode.get("lat").asDouble();
                            double lon = objNode.get("lon").asDouble();
                            
                            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                            
                            // Extract fields
                            Map<String, Object> fields = new HashMap<>();
                            if (objNode.has("fields")) {
                                JsonNode fieldsNode = objNode.get("fields");
                                fieldsNode.fields().forEachRemaining(entry -> 
                                    fields.put(entry.getKey(), entry.getValue().asText()));
                            }
                            
                            Tile38Object tile38Object = Tile38Object.builder()
                                .id(id)
                                .geometry(point)
                                .fields(fields)
                                .timestamp(System.currentTimeMillis())
                                .build();
                            
                            batch.put(id, tile38Object);
                            batchCount++;
                            totalRecords++;
                            
                            // Process batch when it reaches the limit
                            if (batchCount >= BATCH_SIZE) {
                                tile38Service.bulkSet(collectionName, batch);
                                logger.info("Processed batch of {} objects for collection '{}', total: {}", 
                                          batchCount, collectionName, totalRecords);
                                batch.clear();
                                batchCount = 0;
                            }
                            
                        } catch (Exception e) {
                            logger.warn("Failed to process object in collection '{}': {}", collectionName, e.getMessage());
                        }
                    }
                    
                    // Process remaining batch
                    if (!batch.isEmpty()) {
                        tile38Service.bulkSet(collectionName, batch);
                        logger.info("Processed final batch of {} objects for collection '{}'", 
                                  batch.size(), collectionName);
                    }
                }
                
                long endTime = System.currentTimeMillis();
                String message = String.format("Successfully loaded %d records from JSON in %dms", 
                                              totalRecords, (endTime - startTime));
                logger.info(message);
                
                return new LoadResult(true, totalRecords, endTime - startTime, message);
                
            } catch (Exception e) {
                long endTime = System.currentTimeMillis();
                String error = "Failed to load JSON: " + e.getMessage();
                logger.error(error, e);
                return new LoadResult(false, 0, endTime - startTime, error);
            }
        });
    }
    
    @Override
    public CompletableFuture<LoadResult> loadFromCsv(String filePath) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting CSV data load from: {}", filePath);
            long startTime = System.currentTimeMillis();
            
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line = reader.readLine();
                if (line == null) {
                    return new LoadResult(false, 0, 0, "Empty CSV file");
                }
                
                // Parse header
                String[] headers = line.split(",");
                Map<String, Tile38Object> batch = new HashMap<>();
                long totalRecords = 0;
                String currentCollection = "default";
                
                // Extract collection name from filename if possible
                String fileName = new File(filePath).getName();
                if (fileName.contains(".")) {
                    currentCollection = fileName.substring(0, fileName.lastIndexOf("."));
                }
                
                while ((line = reader.readLine()) != null) {
                    try {
                        String[] values = line.split(",");
                        if (values.length < 3) continue; // Need at least id, lat, lon
                        
                        String id = values[0].trim();
                        double lat = Double.parseDouble(values[1].trim());
                        double lon = Double.parseDouble(values[2].trim());
                        
                        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                        
                        // Extract additional fields
                        Map<String, Object> fields = new HashMap<>();
                        for (int i = 3; i < values.length && i < headers.length; i++) {
                            if (!values[i].trim().isEmpty()) {
                                fields.put(headers[i].trim(), values[i].trim());
                            }
                        }
                        
                        Tile38Object tile38Object = Tile38Object.builder()
                            .id(id)
                            .geometry(point)
                            .fields(fields)
                            .timestamp(System.currentTimeMillis())
                            .build();
                        
                        batch.put(id, tile38Object);
                        totalRecords++;
                        
                        // Process batch when it reaches the limit
                        if (batch.size() >= BATCH_SIZE) {
                            tile38Service.bulkSet(currentCollection, batch);
                            logger.info("Processed batch of {} objects, total: {}", batch.size(), totalRecords);
                            batch.clear();
                        }
                        
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid number format in CSV line: {}", line);
                    }
                }
                
                // Process remaining batch
                if (!batch.isEmpty()) {
                    tile38Service.bulkSet(currentCollection, batch);
                    logger.info("Processed final batch of {} objects", batch.size());
                }
                
                long endTime = System.currentTimeMillis();
                String message = String.format("Successfully loaded %d records from CSV in %dms", 
                                              totalRecords, (endTime - startTime));
                logger.info(message);
                
                return new LoadResult(true, totalRecords, endTime - startTime, message);
                
            } catch (Exception e) {
                long endTime = System.currentTimeMillis();
                String error = "Failed to load CSV: " + e.getMessage();
                logger.error(error, e);
                return new LoadResult(false, 0, endTime - startTime, error);
            }
        });
    }
    
    @Override
    public CompletableFuture<LoadResult> generateTestData(String collectionName, int numberOfRecords, 
                                                          double minLat, double maxLat, double minLon, double maxLon) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Generating {} test records for collection '{}'", numberOfRecords, collectionName);
            long startTime = System.currentTimeMillis();
            
            try {
                Map<String, Tile38Object> batch = new HashMap<>();
                long totalRecords = 0;
                
                for (int i = 0; i < numberOfRecords; i++) {
                    double lat = minLat + (maxLat - minLat) * random.nextDouble();
                    double lon = minLon + (maxLon - minLon) * random.nextDouble();
                    
                    Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                    
                    // Generate sample fields (legacy)
                    Map<String, Object> fields = new HashMap<>();
                    fields.put("type", "vehicle_" + (i % 5)); // vehicle_0 to vehicle_4
                    fields.put("speed", 30 + random.nextInt(70)); // 30-100 speed
                    fields.put("driver", "driver_" + (i % 1000)); // 1000 unique drivers
                    fields.put("fuel", random.nextInt(100)); // 0-100 fuel level
                    
                    // Generate KV data for modern usage
                    KVData kvData = new KVData();
                    
                    // Generate tags
                    String[] categories = {"retail", "service", "food", "entertainment", "healthcare", "education"};
                    String[] types = {"premium", "standard", "basic", "vip", "regular"};
                    String[] statuses = {"active", "inactive", "pending", "verified", "suspended"};
                    
                    kvData.setTag("category", categories[i % categories.length]);
                    kvData.setTag("type", types[i % types.length]);
                    kvData.setTag("status", statuses[i % statuses.length]);
                    
                    // Generate attributes
                    kvData.setAttribute("priority", random.nextInt(10) + 1);
                    kvData.setAttribute("score", random.nextDouble() * 100);
                    kvData.setAttribute("active", random.nextBoolean());
                    kvData.setAttribute("rating", 1.0 + random.nextDouble() * 4.0); // 1.0-5.0
                    kvData.setAttribute("created_timestamp", System.currentTimeMillis());
                    
                    String id = "obj" + i; // Consistent with performance test expectations
                    Tile38Object tile38Object = Tile38Object.builder()
                        .id(id)
                        .geometry(point)
                        .fields(fields)
                        .kvData(kvData)
                        .timestamp(System.currentTimeMillis())
                        .build();
                    
                    batch.put(id, tile38Object);
                    totalRecords++;
                    
                    // Process batch when it reaches the limit
                    if (batch.size() >= BATCH_SIZE) {
                        tile38Service.bulkSet(collectionName, batch);
                        if (totalRecords % 50000 == 0) { // Log every 50k records
                            logger.info("Generated {} test records so far...", totalRecords);
                        }
                        batch.clear();
                    }
                }
                
                // Process remaining batch
                if (!batch.isEmpty()) {
                    tile38Service.bulkSet(collectionName, batch);
                }
                
                long endTime = System.currentTimeMillis();
                String message = String.format("Successfully generated %d test records for collection '%s' in %dms", 
                                              totalRecords, collectionName, (endTime - startTime));
                logger.info(message);
                
                return new LoadResult(true, totalRecords, endTime - startTime, message);
                
            } catch (Exception e) {
                long endTime = System.currentTimeMillis();
                String error = "Failed to generate test data: " + e.getMessage();
                logger.error(error, e);
                return new LoadResult(false, 0, endTime - startTime, error);
            }
        });
    }
    
    @Override
    public CompletableFuture<LoadResult> loadFromDataSource(DataSource dataSource) {
        try {
            var repository = repositoryFactory.getRepository(dataSource.getType());
            return repository.loadData(dataSource);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.completedFuture(
                new LoadResult(false, 0, 0, "Unsupported data source: " + e.getMessage()));
        }
    }
}