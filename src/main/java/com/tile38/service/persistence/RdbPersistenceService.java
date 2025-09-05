package com.tile38.service.persistence;

import com.tile38.model.Tile38Object;
import com.tile38.repository.SpatialRepository;
import com.tile38.service.Tile38Service;
import com.tile38.config.PersistenceProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for RDB (Redis Database) style persistence
 * Saves and loads spatial data snapshots to/from file
 */
@Service
public class RdbPersistenceService {
    
    private static final Logger logger = LoggerFactory.getLogger(RdbPersistenceService.class);
    
    @Autowired
    private SpatialRepository spatialRepository;
    
    @Autowired
    private Tile38Service tile38Service;
    
    @Autowired
    private PersistenceProperties persistenceProperties;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    private final AtomicBoolean isLoaded = new AtomicBoolean(false);
    
    /**
     * Load RDB data on application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void loadRdbOnStartup() {
        if (!persistenceProperties.getRdb().isEnabled() || 
            !persistenceProperties.getRdb().isLoadOnStartup()) {
            logger.info("RDB loading on startup is disabled");
            return;
        }
        
        loadFromRdb();
    }
    
    /**
     * Scheduled RDB save based on configuration
     */
    @Scheduled(fixedDelayString = "#{@persistenceProperties.rdb.saveInterval}")
    public void scheduledSave() {
        if (!persistenceProperties.getRdb().isEnabled()) {
            return;
        }
        
        saveToRdb();
    }
    
    /**
     * Save current spatial data to RDB file
     */
    public boolean saveToRdb() {
        if (!persistenceProperties.getRdb().isEnabled()) {
            logger.warn("RDB persistence is disabled");
            return false;
        }
        
        String filePath = persistenceProperties.getRdb().getFilePath();
        logger.info("Starting RDB save to: {}", filePath);
        long startTime = System.currentTimeMillis();
        
        try {
            // Create directory if it doesn't exist
            Path path = Paths.get(filePath);
            Files.createDirectories(path.getParent());
            
            // Collect all data from spatial repository
            Map<String, Map<String, Tile38Object>> allData = new HashMap<>();
            Set<String> keys = spatialRepository.keys();
            
            long totalObjects = 0;
            for (String key : keys) {
                Map<String, Tile38Object> collection = spatialRepository.getAll(key);
                if (!collection.isEmpty()) {
                    allData.put(key, collection);
                    totalObjects += collection.size();
                }
            }
            
            // Create RDB snapshot data
            RdbSnapshot snapshot = new RdbSnapshot();
            snapshot.setTimestamp(System.currentTimeMillis());
            snapshot.setVersion("1.0");
            snapshot.setCollections(allData);
            snapshot.setTotalObjects(totalObjects);
            
            // Write to temporary file first, then rename for atomic operation
            String tempFilePath = filePath + ".tmp";
            try (FileWriter writer = new FileWriter(tempFilePath)) {
                objectMapper.writeValue(writer, snapshot);
            }
            
            // Atomic move
            Files.move(Paths.get(tempFilePath), path, 
                      java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            long endTime = System.currentTimeMillis();
            logger.info("RDB save completed: {} collections, {} objects in {}ms", 
                       allData.size(), totalObjects, (endTime - startTime));
            
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to save RDB: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Load spatial data from RDB file
     */
    public boolean loadFromRdb() {
        if (!persistenceProperties.getRdb().isEnabled()) {
            logger.warn("RDB persistence is disabled");
            return false;
        }
        
        String filePath = persistenceProperties.getRdb().getFilePath();
        Path path = Paths.get(filePath);
        
        if (!Files.exists(path)) {
            logger.info("RDB file does not exist: {}", filePath);
            isLoaded.set(true);
            return true;
        }
        
        logger.info("Starting RDB load from: {}", filePath);
        long startTime = System.currentTimeMillis();
        
        try {
            RdbSnapshot snapshot;
            try (FileReader reader = new FileReader(filePath)) {
                snapshot = objectMapper.readValue(reader, RdbSnapshot.class);
            }
            
            logger.info("Loaded RDB snapshot: version={}, timestamp={}, totalObjects={}", 
                       snapshot.getVersion(), snapshot.getTimestamp(), snapshot.getTotalObjects());
            
            // Clear existing data
            spatialRepository.flushAll();
            
            // Load collections
            long totalLoaded = 0;
            for (Map.Entry<String, Map<String, Tile38Object>> entry : snapshot.getCollections().entrySet()) {
                String key = entry.getKey();
                Map<String, Tile38Object> objects = entry.getValue();
                
                if (!objects.isEmpty()) {
                    // Filter out expired objects
                    Map<String, Tile38Object> validObjects = new HashMap<>();
                    for (Map.Entry<String, Tile38Object> objEntry : objects.entrySet()) {
                        Tile38Object obj = objEntry.getValue();
                        if (!obj.isExpired()) {
                            validObjects.put(objEntry.getKey(), obj);
                        }
                    }
                    
                    if (!validObjects.isEmpty()) {
                        spatialRepository.bulkIndex(key, validObjects);
                        totalLoaded += validObjects.size();
                        logger.info("Loaded collection '{}': {} objects", key, validObjects.size());
                    }
                }
            }
            
            long endTime = System.currentTimeMillis();
            logger.info("RDB load completed: {} collections, {} objects in {}ms", 
                       snapshot.getCollections().size(), totalLoaded, (endTime - startTime));
            
            isLoaded.set(true);
            return true;
            
        } catch (Exception e) {
            logger.error("Failed to load RDB: {}", e.getMessage(), e);
            isLoaded.set(true); // Mark as loaded even if failed to prevent startup blocking
            return false;
        }
    }
    
    /**
     * Check if RDB data has been loaded
     */
    public boolean isLoaded() {
        return isLoaded.get();
    }
    
    /**
     * Data structure for RDB snapshots
     */
    public static class RdbSnapshot {
        private long timestamp;
        private String version;
        private Map<String, Map<String, Tile38Object>> collections;
        private long totalObjects;
        
        // Getters and setters
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        
        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }
        
        public Map<String, Map<String, Tile38Object>> getCollections() { return collections; }
        public void setCollections(Map<String, Map<String, Tile38Object>> collections) { this.collections = collections; }
        
        public long getTotalObjects() { return totalObjects; }
        public void setTotalObjects(long totalObjects) { this.totalObjects = totalObjects; }
    }
}