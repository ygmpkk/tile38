package com.tile38.service.persistence;

import com.tile38.model.Tile38Object;
import com.tile38.config.PersistenceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

import jakarta.annotation.PreDestroy;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for MySQL persistence
 * Handles real-time and batch persistence of spatial data to MySQL
 */
@Service
public class MysqlPersistenceService {
    
    private static final Logger logger = LoggerFactory.getLogger(MysqlPersistenceService.class);
    
    @Autowired
    private PersistenceProperties persistenceProperties;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    private final WKTWriter wktWriter = new WKTWriter();
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final BlockingQueue<PersistenceTask> taskQueue = new LinkedBlockingQueue<>();
    
    private Connection connection;
    private PreparedStatement insertStatement;
    private PreparedStatement updateStatement;
    private PreparedStatement deleteStatement;
    
    /**
     * Initialize MySQL persistence on application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeOnStartup() {
        if (!persistenceProperties.getMysql().isEnabled()) {
            logger.info("MySQL persistence is disabled");
            return;
        }
        
        initialize();
        startBackgroundProcessor();
    }
    
    /**
     * Initialize MySQL connection and create tables
     */
    public boolean initialize() {
        if (!persistenceProperties.getMysql().isEnabled()) {
            logger.warn("MySQL persistence is disabled");
            return false;
        }
        
        try {
            PersistenceProperties.Mysql config = persistenceProperties.getMysql();
            
            // Establish connection
            logger.info("Connecting to MySQL: {}", config.getUrl());
            connection = DriverManager.getConnection(
                config.getUrl(), 
                config.getUsername(), 
                config.getPassword()
            );
            connection.setAutoCommit(false);
            
            // Create tables if they don't exist
            createTables();
            
            // Prepare statements
            prepareStatements();
            
            isInitialized.set(true);
            logger.info("MySQL persistence initialized successfully");
            
            return true;
            
        } catch (SQLException e) {
            logger.error("Failed to initialize MySQL persistence: {}", e.getMessage(), e);
            isInitialized.set(false);
            return false;
        }
    }
    
    /**
     * Create required tables
     */
    private void createTables() throws SQLException {
        String tablePrefix = persistenceProperties.getMysql().getTablePrefix();
        
        String createObjectsTable = String.format("""
            CREATE TABLE IF NOT EXISTS %sobjects (
                collection_name VARCHAR(255) NOT NULL,
                object_id VARCHAR(255) NOT NULL,
                geometry_wkt TEXT NOT NULL,
                geometry_type VARCHAR(50) NOT NULL,
                latitude DOUBLE,
                longitude DOUBLE,
                fields JSON,
                timestamp_ms BIGINT NOT NULL,
                expires_at BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (collection_name, object_id),
                INDEX idx_location (latitude, longitude),
                INDEX idx_timestamp (timestamp_ms),
                INDEX idx_expires (expires_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, tablePrefix);
        
        String createCollectionsTable = String.format("""
            CREATE TABLE IF NOT EXISTS %scollections (
                collection_name VARCHAR(255) PRIMARY KEY,
                object_count BIGINT DEFAULT 0,
                min_lat DOUBLE,
                max_lat DOUBLE,
                min_lon DOUBLE,
                max_lon DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """, tablePrefix);
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createObjectsTable);
            stmt.execute(createCollectionsTable);
            connection.commit();
            logger.info("MySQL tables created/verified successfully");
        }
    }
    
    /**
     * Prepare SQL statements
     */
    private void prepareStatements() throws SQLException {
        String tablePrefix = persistenceProperties.getMysql().getTablePrefix();
        
        String insertSql = String.format("""
            INSERT INTO %sobjects 
            (collection_name, object_id, geometry_wkt, geometry_type, latitude, longitude, fields, timestamp_ms, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
            geometry_wkt = VALUES(geometry_wkt),
            geometry_type = VALUES(geometry_type),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            fields = VALUES(fields),
            timestamp_ms = VALUES(timestamp_ms),
            expires_at = VALUES(expires_at)
            """, tablePrefix);
        
        String updateSql = String.format("""
            UPDATE %sobjects SET
            geometry_wkt = ?, geometry_type = ?, latitude = ?, longitude = ?, 
            fields = ?, timestamp_ms = ?, expires_at = ?
            WHERE collection_name = ? AND object_id = ?
            """, tablePrefix);
        
        String deleteSql = String.format("""
            DELETE FROM %sobjects WHERE collection_name = ? AND object_id = ?
            """, tablePrefix);
        
        insertStatement = connection.prepareStatement(insertSql);
        updateStatement = connection.prepareStatement(updateSql);
        deleteStatement = connection.prepareStatement(deleteSql);
    }
    
    /**
     * Persist object to MySQL (async if real-time is enabled)
     */
    public void persistObject(String collection, String id, Tile38Object object) {
        if (!isInitialized.get()) {
            logger.warn("MySQL persistence not initialized, skipping persist operation");
            return;
        }
        
        PersistenceTask task = new PersistenceTask(PersistenceTask.Type.INSERT, collection, id, object);
        
        if (persistenceProperties.getMysql().isRealTime()) {
            // Add to queue for background processing
            taskQueue.offer(task);
        } else {
            // Process immediately
            processPersistenceTask(task);
        }
    }
    
    /**
     * Update object in MySQL
     */
    public void updateObject(String collection, String id, Tile38Object object) {
        if (!isInitialized.get()) {
            logger.warn("MySQL persistence not initialized, skipping update operation");
            return;
        }
        
        PersistenceTask task = new PersistenceTask(PersistenceTask.Type.UPDATE, collection, id, object);
        
        if (persistenceProperties.getMysql().isRealTime()) {
            taskQueue.offer(task);
        } else {
            processPersistenceTask(task);
        }
    }
    
    /**
     * Delete object from MySQL
     */
    public void deleteObject(String collection, String id) {
        if (!isInitialized.get()) {
            logger.warn("MySQL persistence not initialized, skipping delete operation");
            return;
        }
        
        PersistenceTask task = new PersistenceTask(PersistenceTask.Type.DELETE, collection, id, null);
        
        if (persistenceProperties.getMysql().isRealTime()) {
            taskQueue.offer(task);
        } else {
            processPersistenceTask(task);
        }
    }
    
    /**
     * Start background processor for real-time persistence
     */
    @Async
    public void startBackgroundProcessor() {
        if (!persistenceProperties.getMysql().isRealTime()) {
            logger.info("MySQL real-time persistence is disabled");
            return;
        }
        
        logger.info("Starting MySQL background processor");
        int batchSize = persistenceProperties.getMysql().getBatchSize();
        
        while (!isShutdown.get()) {
            try {
                // Process tasks in batches
                int processed = 0;
                while (processed < batchSize && !isShutdown.get()) {
                    PersistenceTask task = taskQueue.poll();
                    if (task == null) {
                        break;
                    }
                    
                    processPersistenceTask(task);
                    processed++;
                }
                
                if (processed > 0) {
                    connection.commit();
                    logger.debug("Committed {} MySQL persistence tasks", processed);
                }
                
                // Short sleep if no tasks processed
                if (processed == 0) {
                    Thread.sleep(100);
                }
                
            } catch (InterruptedException e) {
                logger.info("MySQL background processor interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in MySQL background processor: {}", e.getMessage(), e);
                try {
                    if (connection != null && !connection.isClosed()) {
                        connection.rollback();
                    }
                } catch (SQLException rollbackEx) {
                    logger.error("Failed to rollback MySQL transaction: {}", rollbackEx.getMessage());
                }
            }
        }
        
        logger.info("MySQL background processor stopped");
    }
    
    /**
     * Process a single persistence task
     */
    private void processPersistenceTask(PersistenceTask task) {
        try {
            switch (task.getType()) {
                case INSERT, UPDATE -> {
                    Tile38Object object = task.getObject();
                    if (object == null || object.getGeometry() == null) {
                        return;
                    }
                    
                    Geometry geometry = object.getGeometry();
                    String wkt = wktWriter.write(geometry);
                    String geometryType = geometry.getGeometryType();
                    
                    // Extract coordinates for indexing
                    double lat = 0, lon = 0;
                    if (geometry instanceof org.locationtech.jts.geom.Point point) {
                        lat = point.getY();
                        lon = point.getX();
                    } else {
                        // Use centroid for non-point geometries
                        org.locationtech.jts.geom.Point centroid = geometry.getCentroid();
                        lat = centroid.getY();
                        lon = centroid.getX();
                    }
                    
                    // Convert fields to JSON
                    String fieldsJson = null;
                    if (object.getFields() != null && !object.getFields().isEmpty()) {
                        fieldsJson = objectMapper.writeValueAsString(object.getFields());
                    }
                    
                    PreparedStatement stmt = task.getType() == PersistenceTask.Type.INSERT ? 
                                           insertStatement : updateStatement;
                    
                    if (task.getType() == PersistenceTask.Type.INSERT) {
                        stmt.setString(1, task.getCollection());
                        stmt.setString(2, task.getId());
                        stmt.setString(3, wkt);
                        stmt.setString(4, geometryType);
                        stmt.setDouble(5, lat);
                        stmt.setDouble(6, lon);
                        stmt.setString(7, fieldsJson);
                        stmt.setLong(8, object.getTimestamp());
                        stmt.setObject(9, object.getExpireAt() != null ? 
                                      object.getExpireAt().toEpochMilli() : null);
                    } else {
                        stmt.setString(1, wkt);
                        stmt.setString(2, geometryType);
                        stmt.setDouble(3, lat);
                        stmt.setDouble(4, lon);
                        stmt.setString(5, fieldsJson);
                        stmt.setLong(6, object.getTimestamp());
                        stmt.setObject(7, object.getExpireAt() != null ? 
                                      object.getExpireAt().toEpochMilli() : null);
                        stmt.setString(8, task.getCollection());
                        stmt.setString(9, task.getId());
                    }
                    
                    stmt.executeUpdate();
                }
                case DELETE -> {
                    deleteStatement.setString(1, task.getCollection());
                    deleteStatement.setString(2, task.getId());
                    deleteStatement.executeUpdate();
                }
            }
            
            if (!persistenceProperties.getMysql().isRealTime()) {
                connection.commit();
            }
            
        } catch (Exception e) {
            logger.error("Failed to process MySQL persistence task: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Shutdown cleanup
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down MySQL persistence service");
        isShutdown.set(true);
        
        // Process remaining tasks
        while (!taskQueue.isEmpty()) {
            PersistenceTask task = taskQueue.poll();
            if (task != null) {
                processPersistenceTask(task);
            }
        }
        
        // Close resources
        try {
            if (connection != null && !connection.isClosed()) {
                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("Error closing MySQL connection: {}", e.getMessage());
        }
        
        logger.info("MySQL persistence service shutdown complete");
    }
    
    /**
     * Check if MySQL persistence is initialized
     */
    public boolean isInitialized() {
        return isInitialized.get();
    }
    
    /**
     * Task for persistence operations
     */
    private static class PersistenceTask {
        public enum Type { INSERT, UPDATE, DELETE }
        
        private final Type type;
        private final String collection;
        private final String id;
        private final Tile38Object object;
        
        public PersistenceTask(Type type, String collection, String id, Tile38Object object) {
            this.type = type;
            this.collection = collection;
            this.id = id;
            this.object = object;
        }
        
        public Type getType() { return type; }
        public String getCollection() { return collection; }
        public String getId() { return id; }
        public Tile38Object getObject() { return object; }
    }
}