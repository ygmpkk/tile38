package com.tile38.repository.data.impl;

import com.tile38.repository.data.DataRepository;
import com.tile38.repository.data.DataSource;
import com.tile38.loader.DataLoader.LoadResult;
import com.tile38.service.Tile38Service;
import com.tile38.model.Tile38Object;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation for database-based data sources (MySQL, MongoDB, Presto)
 */
@Component
public class DatabaseRepository implements DataRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseRepository.class);
    
    @Autowired
    public Tile38Service tile38Service;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    
    // Batch size for processing - tuned for memory efficiency
    private static final int BATCH_SIZE = 5000;
    
    private static final Set<DataSource.DataSourceType> SUPPORTED_TYPES = Set.of(
        DataSource.DataSourceType.DATABASE_MYSQL,
        DataSource.DataSourceType.DATABASE_MONGODB,
        DataSource.DataSourceType.DATABASE_PRESTO
    );
    
    @Override
    public CompletableFuture<LoadResult> loadData(DataSource dataSource) {
        return switch (dataSource.getType()) {
            case DATABASE_MYSQL -> loadFromMySQL(dataSource);
            case DATABASE_MONGODB -> loadFromMongoDB(dataSource);
            case DATABASE_PRESTO -> loadFromPresto(dataSource);
            default -> CompletableFuture.completedFuture(
                new LoadResult(false, 0, 0, "Unsupported database type: " + dataSource.getType()));
        };
    }
    
    @Override
    public boolean supports(DataSource.DataSourceType type) {
        return SUPPORTED_TYPES.contains(type);
    }
    
    @Override
    public CompletableFuture<Boolean> testConnection(DataSource dataSource) {
        return switch (dataSource.getType()) {
            case DATABASE_MYSQL -> testMySQLConnection(dataSource);
            case DATABASE_MONGODB -> testMongoDBConnection(dataSource);
            case DATABASE_PRESTO -> testPrestoConnection(dataSource);
            default -> CompletableFuture.completedFuture(false);
        };
    }
    
    /**
     * Load data from MySQL database
     */
    private CompletableFuture<LoadResult> loadFromMySQL(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting MySQL data load from: {}", dataSource.getLocation());
            long startTime = System.currentTimeMillis();
            
            try (Connection connection = DriverManager.getConnection(dataSource.getLocation(), 
                    dataSource.getProperties() != null ? dataSource.getProperties().get("username") : null, 
                    dataSource.getProperties() != null ? dataSource.getProperties().get("password") : null)) {
                
                String query = dataSource.getQuery();
                if (query == null || query.trim().isEmpty()) {
                    return new LoadResult(false, 0, 0, "Query is required for MySQL data source");
                }
                
                logger.info("Executing MySQL query: {}", query);
                
                try (PreparedStatement stmt = connection.prepareStatement(query);
                     ResultSet rs = stmt.executeQuery()) {
                    
                    ResultSetMetaData metadata = rs.getMetaData();
                    int columnCount = metadata.getColumnCount();
                    
                    // Find required columns
                    int idCol = -1, latCol = -1, lonCol = -1;
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = metadata.getColumnName(i).toLowerCase();
                        if (colName.equals("id")) idCol = i;
                        else if (colName.equals("lat") || colName.equals("latitude")) latCol = i;
                        else if (colName.equals("lon") || colName.equals("lng") || colName.equals("longitude")) lonCol = i;
                    }
                    
                    if (idCol == -1 || latCol == -1 || lonCol == -1) {
                        return new LoadResult(false, 0, 0, 
                            "Required columns not found. Need: id, lat/latitude, lon/lng/longitude");
                    }
                    
                    Map<String, Tile38Object> batch = new HashMap<>();
                    long totalRecords = 0;
                    String collectionName = dataSource.getCollectionName();
                    
                    while (rs.next()) {
                        try {
                            String id = rs.getString(idCol);
                            double lat = rs.getDouble(latCol);
                            double lon = rs.getDouble(lonCol);
                            
                            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                            
                            // Extract other fields
                            Map<String, Object> fields = new HashMap<>();
                            for (int i = 1; i <= columnCount; i++) {
                                if (i != idCol && i != latCol && i != lonCol) {
                                    String colName = metadata.getColumnName(i);
                                    Object value = rs.getObject(i);
                                    if (value != null) {
                                        fields.put(colName, value);
                                    }
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
                                logger.info("Processed MySQL batch of {} objects, total: {}", 
                                          batch.size(), totalRecords);
                                batch.clear();
                            }
                            
                        } catch (SQLException e) {
                            logger.warn("Error processing MySQL row: {}", e.getMessage());
                        }
                    }
                    
                    // Process remaining batch
                    if (!batch.isEmpty()) {
                        tile38Service.bulkSet(collectionName, batch);
                        logger.info("Processed final MySQL batch of {} objects", batch.size());
                    }
                    
                    long endTime = System.currentTimeMillis();
                    String message = String.format("Successfully loaded %d records from MySQL in %dms", 
                                                  totalRecords, (endTime - startTime));
                    logger.info(message);
                    
                    return new LoadResult(true, totalRecords, endTime - startTime, message);
                }
                
            } catch (SQLException e) {
                long endTime = System.currentTimeMillis();
                String error = "Failed to load from MySQL: " + e.getMessage();
                logger.error(error, e);
                return new LoadResult(false, 0, endTime - startTime, error);
            }
        });
    }
    
    /**
     * Load data from MongoDB database
     */
    private CompletableFuture<LoadResult> loadFromMongoDB(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            logger.info("Starting MongoDB data load from: {}", dataSource.getLocation());
            long startTime = System.currentTimeMillis();
            
            try (MongoClient mongoClient = MongoClients.create(dataSource.getLocation())) {
                
                String databaseName = dataSource.getProperties() != null ? 
                    dataSource.getProperties().get("database") : null;
                String collectionNameMongo = dataSource.getProperties() != null ? 
                    dataSource.getProperties().get("collection") : null;
                
                if (databaseName == null || collectionNameMongo == null) {
                    return new LoadResult(false, 0, 0, 
                        "MongoDB requires 'database' and 'collection' properties");
                }
                
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                MongoCollection<Document> collection = database.getCollection(collectionNameMongo);
                
                Map<String, Tile38Object> batch = new HashMap<>();
                long totalRecords = 0;
                String targetCollection = dataSource.getCollectionName();
                
                for (Document doc : collection.find()) {
                    try {
                        // Extract required fields
                        String id = doc.getString("_id");
                        if (id == null) {
                            id = doc.getObjectId("_id").toString();
                        }
                        
                        Double lat = null, lon = null;
                        
                        // Try different field names for coordinates
                        if (doc.containsKey("lat")) lat = doc.getDouble("lat");
                        else if (doc.containsKey("latitude")) lat = doc.getDouble("latitude");
                        
                        if (doc.containsKey("lon")) lon = doc.getDouble("lon");
                        else if (doc.containsKey("lng")) lon = doc.getDouble("lng");
                        else if (doc.containsKey("longitude")) lon = doc.getDouble("longitude");
                        
                        // Try GeoJSON format
                        if (lat == null || lon == null && doc.containsKey("location")) {
                            Document location = doc.get("location", Document.class);
                            if (location != null && "Point".equals(location.getString("type"))) {
                                @SuppressWarnings("unchecked")
                                java.util.List<Double> coordinates = (java.util.List<Double>) location.get("coordinates");
                                if (coordinates != null && coordinates.size() >= 2) {
                                    lon = coordinates.get(0);
                                    lat = coordinates.get(1);
                                }
                            }
                        }
                        
                        if (lat == null || lon == null) {
                            logger.warn("Skipping MongoDB document without valid coordinates: {}", id);
                            continue;
                        }
                        
                        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
                        
                        // Extract other fields
                        Map<String, Object> fields = new HashMap<>();
                        for (Map.Entry<String, Object> entry : doc.entrySet()) {
                            String key = entry.getKey();
                            if (!key.equals("_id") && !key.equals("lat") && !key.equals("lon") && 
                                !key.equals("latitude") && !key.equals("longitude") && !key.equals("lng") &&
                                !key.equals("location") && entry.getValue() != null) {
                                fields.put(key, entry.getValue());
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
                            tile38Service.bulkSet(targetCollection, batch);
                            logger.info("Processed MongoDB batch of {} objects, total: {}", 
                                      batch.size(), totalRecords);
                            batch.clear();
                        }
                        
                    } catch (Exception e) {
                        logger.warn("Error processing MongoDB document: {}", e.getMessage());
                    }
                }
                
                // Process remaining batch
                if (!batch.isEmpty()) {
                    tile38Service.bulkSet(targetCollection, batch);
                    logger.info("Processed final MongoDB batch of {} objects", batch.size());
                }
                
                long endTime = System.currentTimeMillis();
                String message = String.format("Successfully loaded %d records from MongoDB in %dms", 
                                              totalRecords, (endTime - startTime));
                logger.info(message);
                
                return new LoadResult(true, totalRecords, endTime - startTime, message);
                
            } catch (Exception e) {
                long endTime = System.currentTimeMillis();
                String error = "Failed to load from MongoDB: " + e.getMessage();
                logger.error(error, e);
                return new LoadResult(false, 0, endTime - startTime, error);
            }
        });
    }
    
    /**
     * Load data from Presto database
     */
    private CompletableFuture<LoadResult> loadFromPresto(DataSource dataSource) {
        // TODO: Implement Presto loading
        return CompletableFuture.completedFuture(
            new LoadResult(false, 0, 0, "Presto loading not yet implemented"));
    }
    
    /**
     * Test MySQL database connection
     */
    private CompletableFuture<Boolean> testMySQLConnection(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String connectionUrl = dataSource.getLocation();
                String username = dataSource.getProperties() != null ? 
                    dataSource.getProperties().get("username") : null;
                String password = dataSource.getProperties() != null ? 
                    dataSource.getProperties().get("password") : null;
                
                logger.info("Testing MySQL connection to: {}", connectionUrl);
                
                try (Connection connection = DriverManager.getConnection(connectionUrl, username, password)) {
                    // Test with a simple query
                    try (Statement stmt = connection.createStatement();
                         ResultSet rs = stmt.executeQuery("SELECT 1")) {
                        return rs.next();
                    }
                }
                
            } catch (SQLException e) {
                logger.warn("MySQL connection test failed: {}", e.getMessage());
                return false;
            }
        });
    }
    
    /**
     * Test MongoDB database connection
     */
    private CompletableFuture<Boolean> testMongoDBConnection(DataSource dataSource) {
        return CompletableFuture.supplyAsync(() -> {
            try (MongoClient mongoClient = MongoClients.create(dataSource.getLocation())) {
                
                logger.info("Testing MongoDB connection to: {}", dataSource.getLocation());
                
                // Test connection by listing databases
                mongoClient.listDatabaseNames().first();
                return true;
                
            } catch (Exception e) {
                logger.warn("MongoDB connection test failed: {}", e.getMessage());
                return false;
            }
        });
    }
    
    /**
     * Test Presto database connection
     */
    private CompletableFuture<Boolean> testPrestoConnection(DataSource dataSource) {
        // TODO: Implement Presto connection test
        return CompletableFuture.completedFuture(false);
    }
}