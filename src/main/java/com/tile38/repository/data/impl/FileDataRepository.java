package com.tile38.repository.data.impl;

import com.tile38.repository.data.DataRepository;
import com.tile38.repository.data.DataSource;
import com.tile38.loader.DataLoader.LoadResult;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation for file-based data sources (CSV, JSON, GeoJSON, SHP)
 */
@Component
public class FileDataRepository implements DataRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(FileDataRepository.class);
    
    @Autowired
    public Tile38Service tile38Service;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Batch size for processing - tuned for memory efficiency
    private static final int BATCH_SIZE = 10000;
    
    private static final Set<DataSource.DataSourceType> SUPPORTED_TYPES = Set.of(
        DataSource.DataSourceType.FILE_CSV,
        DataSource.DataSourceType.FILE_JSON,
        DataSource.DataSourceType.FILE_GEOJSON,
        DataSource.DataSourceType.FILE_SHP
    );
    
    @Override
    public CompletableFuture<LoadResult> loadData(DataSource dataSource) {
        return switch (dataSource.getType()) {
            case FILE_CSV -> loadFromCsv(dataSource);
            case FILE_JSON -> loadFromJson(dataSource);
            case FILE_GEOJSON -> loadFromGeoJson(dataSource);
            case FILE_SHP -> loadFromShapefile(dataSource);
            default -> CompletableFuture.completedFuture(
                new LoadResult(false, 0, 0, "Unsupported file type: " + dataSource.getType()));
        };
    }
    
    @Override
    public boolean supports(DataSource.DataSourceType type) {
        return SUPPORTED_TYPES.contains(type);
    }
    
    /**
     * Load data from CSV file
     */
    private CompletableFuture<LoadResult> loadFromCsv(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting CSV data load from: {}", dataSource.getLocation());
            long startTime = System.currentTimeMillis();
            
            try (BufferedReader reader = new BufferedReader(new FileReader(dataSource.getLocation()))) {
                String line = reader.readLine();
                if (line == null) {
                    return new LoadResult(false, 0, 0, "Empty CSV file");
                }
                
                // Parse header
                String[] headers = line.split(",");
                Map<String, Tile38Object> batch = new HashMap<>();
                long totalRecords = 0;
                String collectionName = dataSource.getCollectionName();
                
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
                            tile38Service.bulkSet(collectionName, batch);
                            logger.info("Processed batch of {} objects, total: {}", batch.size(), totalRecords);
                            batch.clear();
                        }
                        
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid number format in CSV line: {}", line);
                    }
                }
                
                // Process remaining batch
                if (!batch.isEmpty()) {
                    tile38Service.bulkSet(collectionName, batch);
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
    
    /**
     * Load data from JSON file
     */
    private CompletableFuture<LoadResult> loadFromJson(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting JSON data load from: {}", dataSource.getLocation());
            long startTime = System.currentTimeMillis();
            
            try {
                File file = new File(dataSource.getLocation());
                if (!file.exists()) {
                    return new LoadResult(false, 0, 0, "File not found: " + dataSource.getLocation());
                }
                
                JsonNode root = objectMapper.readTree(file);
                long totalRecords = 0;
                String collectionName = dataSource.getCollectionName();
                
                // Support both single collection and multi-collection JSON
                if (root.isArray()) {
                    // Direct array of objects for single collection
                    totalRecords = processJsonObjects(root, collectionName);
                } else {
                    // Object with collection names as keys
                    var fieldsIterator = root.fields();
                    while (fieldsIterator.hasNext()) {
                        var collectionEntry = fieldsIterator.next();
                        String jsonCollectionName = collectionEntry.getKey();
                        JsonNode objectsArray = collectionEntry.getValue();
                        
                        if (objectsArray.isArray()) {
                            String targetCollection = collectionName != null ? collectionName : jsonCollectionName;
                            totalRecords += processJsonObjects(objectsArray, targetCollection);
                        }
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
    
    /**
     * Helper method to process JSON objects array
     */
    private long processJsonObjects(JsonNode objectsArray, String collectionName) {
        Map<String, Tile38Object> batch = new HashMap<>();
        long totalRecords = 0;
        
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
                totalRecords++;
                
                // Process batch when it reaches the limit
                if (batch.size() >= BATCH_SIZE) {
                    tile38Service.bulkSet(collectionName, batch);
                    logger.info("Processed batch of {} objects for collection '{}', total: {}", 
                              batch.size(), collectionName, totalRecords);
                    batch.clear();
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
        
        return totalRecords;
    }
    
    /**
     * Load data from GeoJSON file
     */
    private CompletableFuture<LoadResult> loadFromGeoJson(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting GeoJSON data load from: {}", dataSource.getLocation());
            long startTime = System.currentTimeMillis();
            
            try {
                File file = new File(dataSource.getLocation());
                if (!file.exists()) {
                    return new LoadResult(false, 0, 0, "File not found: " + dataSource.getLocation());
                }
                
                JsonNode root = objectMapper.readTree(file);
                long totalRecords = 0;
                String collectionName = dataSource.getCollectionName();
                
                // Handle FeatureCollection
                if (root.has("type") && "FeatureCollection".equals(root.get("type").asText())) {
                    JsonNode features = root.get("features");
                    if (features != null && features.isArray()) {
                        totalRecords = processGeoJsonFeatures(features, collectionName);
                    }
                } else if (root.has("type") && "Feature".equals(root.get("type").asText())) {
                    // Handle single Feature
                    totalRecords = processGeoJsonFeature(root, collectionName);
                } else {
                    return new LoadResult(false, 0, 0, "Invalid GeoJSON format: expected FeatureCollection or Feature");
                }
                
                long endTime = System.currentTimeMillis();
                String message = String.format("Successfully loaded %d records from GeoJSON in %dms", 
                                              totalRecords, (endTime - startTime));
                logger.info(message);
                
                return new LoadResult(true, totalRecords, endTime - startTime, message);
                
            } catch (Exception e) {
                long endTime = System.currentTimeMillis();
                String error = "Failed to load GeoJSON: " + e.getMessage();
                logger.error(error, e);
                return new LoadResult(false, 0, endTime - startTime, error);
            }
        });
    }
    
    /**
     * Process GeoJSON features array
     */
    private long processGeoJsonFeatures(JsonNode features, String collectionName) {
        Map<String, Tile38Object> batch = new HashMap<>();
        long totalRecords = 0;
        
        for (JsonNode feature : features) {
            try {
                long processed = processGeoJsonFeature(feature, collectionName, batch);
                totalRecords += processed;
                
                // Process batch when it reaches the limit
                if (batch.size() >= BATCH_SIZE) {
                    tile38Service.bulkSet(collectionName, batch);
                    logger.info("Processed batch of {} objects for collection '{}', total: {}", 
                              batch.size(), collectionName, totalRecords);
                    batch.clear();
                }
                
            } catch (Exception e) {
                logger.warn("Failed to process GeoJSON feature: {}", e.getMessage());
            }
        }
        
        // Process remaining batch
        if (!batch.isEmpty()) {
            tile38Service.bulkSet(collectionName, batch);
            logger.info("Processed final batch of {} objects for collection '{}'", 
                      batch.size(), collectionName);
        }
        
        return totalRecords;
    }
    
    /**
     * Process single GeoJSON feature
     */
    private long processGeoJsonFeature(JsonNode feature, String collectionName) {
        Map<String, Tile38Object> batch = new HashMap<>();
        long processed = processGeoJsonFeature(feature, collectionName, batch);
        if (!batch.isEmpty()) {
            tile38Service.bulkSet(collectionName, batch);
        }
        return processed;
    }
    
    /**
     * Process single GeoJSON feature into batch
     */
    private long processGeoJsonFeature(JsonNode feature, String collectionName, Map<String, Tile38Object> batch) {
        try {
            // Get feature properties
            Map<String, Object> fields = new HashMap<>();
            String id = null;
            
            if (feature.has("properties") && !feature.get("properties").isNull()) {
                JsonNode properties = feature.get("properties");
                properties.fields().forEachRemaining(entry -> {
                    JsonNode value = entry.getValue();
                    if (value.isTextual()) {
                        fields.put(entry.getKey(), value.asText());
                    } else if (value.isNumber()) {
                        fields.put(entry.getKey(), value.asDouble());
                    } else if (value.isBoolean()) {
                        fields.put(entry.getKey(), value.asBoolean());
                    } else if (!value.isNull()) {
                        fields.put(entry.getKey(), value.toString());
                    }
                });
                
                // Try to get ID from properties
                if (properties.has("id")) {
                    id = properties.get("id").asText();
                }
            }
            
            // Get ID from feature.id if not in properties
            if (id == null && feature.has("id")) {
                id = feature.get("id").asText();
            }
            
            // Generate ID if not found
            if (id == null) {
                id = "feature_" + System.nanoTime();
            }
            
            // Parse geometry
            JsonNode geometry = feature.get("geometry");
            if (geometry == null || geometry.isNull()) {
                logger.warn("Skipping feature with no geometry: {}", id);
                return 0;
            }
            
            org.locationtech.jts.geom.Geometry jtsGeometry = parseGeoJsonGeometry(geometry);
            if (jtsGeometry == null) {
                logger.warn("Failed to parse geometry for feature: {}", id);
                return 0;
            }
            
            Tile38Object tile38Object = Tile38Object.builder()
                .id(id)
                .geometry(jtsGeometry)
                .fields(fields)
                .timestamp(System.currentTimeMillis())
                .build();
            
            batch.put(id, tile38Object);
            return 1;
            
        } catch (Exception e) {
            logger.warn("Failed to process GeoJSON feature: {}", e.getMessage());
            return 0;
        }
    }
    
    /**
     * Parse GeoJSON geometry into JTS geometry
     */
    private org.locationtech.jts.geom.Geometry parseGeoJsonGeometry(JsonNode geometry) {
        try {
            String type = geometry.get("type").asText();
            JsonNode coordinates = geometry.get("coordinates");
            
            switch (type.toLowerCase()) {
                case "point":
                    if (coordinates.isArray() && coordinates.size() >= 2) {
                        double lon = coordinates.get(0).asDouble();
                        double lat = coordinates.get(1).asDouble();
                        return geometryFactory.createPoint(new Coordinate(lon, lat));
                    }
                    break;
                    
                case "linestring":
                    if (coordinates.isArray() && coordinates.size() >= 2) {
                        Coordinate[] coords = new Coordinate[coordinates.size()];
                        for (int i = 0; i < coordinates.size(); i++) {
                            JsonNode coord = coordinates.get(i);
                            if (coord.isArray() && coord.size() >= 2) {
                                coords[i] = new Coordinate(coord.get(0).asDouble(), coord.get(1).asDouble());
                            }
                        }
                        return geometryFactory.createLineString(coords);
                    }
                    break;
                    
                case "polygon":
                    if (coordinates.isArray() && coordinates.size() >= 1) {
                        JsonNode shell = coordinates.get(0);
                        if (shell.isArray() && shell.size() >= 4) {
                            Coordinate[] coords = new Coordinate[shell.size()];
                            for (int i = 0; i < shell.size(); i++) {
                                JsonNode coord = shell.get(i);
                                if (coord.isArray() && coord.size() >= 2) {
                                    coords[i] = new Coordinate(coord.get(0).asDouble(), coord.get(1).asDouble());
                                }
                            }
                            return geometryFactory.createPolygon(coords);
                        }
                    }
                    break;
                    
                default:
                    logger.warn("Unsupported GeoJSON geometry type: {}", type);
            }
        } catch (Exception e) {
            logger.warn("Error parsing GeoJSON geometry: {}", e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Load data from Shapefile
     */
    private CompletableFuture<LoadResult> loadFromShapefile(DataSource dataSource) {
        // TODO: Implement Shapefile loading using GeoTools
        return CompletableFuture.completedFuture(
            new LoadResult(false, 0, 0, "Shapefile loading not yet implemented"));
    }
}