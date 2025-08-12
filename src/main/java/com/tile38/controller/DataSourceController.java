package com.tile38.controller;

import com.tile38.loader.DataLoader;
import com.tile38.loader.DataLoader.LoadResult;
import com.tile38.repository.data.DataSource;
import com.tile38.repository.data.DataSource.DataSourceType;
import com.tile38.repository.data.RepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST API controller for unified data loading from various sources
 */
@RestController
@RequestMapping("/api/v1/data")
public class DataSourceController {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceController.class);

    @Autowired
    private DataLoader dataLoader;

    @Autowired
    private RepositoryFactory repositoryFactory;

    /**
     * Load data from a unified data source
     */
    @PostMapping("/load")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> loadData(
            @RequestParam DataSourceType sourceType,
            @RequestParam String location,
            @RequestParam String collectionName,
            @RequestParam(required = false) String query,
            @RequestParam(required = false) Map<String, String> properties) {

        logger.info("Loading data from {} source: {}", sourceType, location);

        try {
            DataSource dataSource;
            if (sourceType.name().startsWith("FILE_")) {
                dataSource = DataSource.createFileSource(sourceType, location, collectionName);
            } else {
                dataSource = DataSource.createDatabaseSource(sourceType, location, collectionName, query, properties);
            }

            return dataLoader.loadFromDataSource(dataSource)
                    .thenApply(result -> {
                        if (result.isSuccess()) {
                            logger.info("Successfully loaded {} records in {}ms", 
                                       result.getRecordsLoaded(), result.getDurationMs());
                            return ResponseEntity.ok(Map.of(
                                "ok", true,
                                "records_loaded", result.getRecordsLoaded(),
                                "duration_ms", result.getDurationMs(),
                                "message", result.getMessage()
                            ));
                        } else {
                            logger.error("Failed to load data: {}", result.getMessage());
                            return ResponseEntity.badRequest().body(Map.of(
                                "ok", false,
                                "message", result.getMessage()
                            ));
                        }
                    });

        } catch (Exception e) {
            logger.error("Error loading data: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(Map.of(
                    "ok", false,
                    "message", "Error: " + e.getMessage()
                ))
            );
        }
    }

    /**
     * Test connection to a database data source
     */
    @PostMapping("/test-connection")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> testConnection(
            @RequestParam DataSourceType sourceType,
            @RequestParam String location,
            @RequestParam(required = false) Map<String, String> properties) {

        if (!sourceType.name().startsWith("DATABASE_")) {
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(Map.of(
                    "ok", false,
                    "message", "Connection test is only supported for database sources"
                ))
            );
        }

        try {
            DataSource dataSource = DataSource.createDatabaseSource(
                sourceType, location, "test", null, properties);
            
            var repository = repositoryFactory.getRepository(sourceType);
            
            return repository.testConnection(dataSource)
                    .thenApply(success -> ResponseEntity.ok(Map.of(
                        "ok", success,
                        "message", success ? "Connection successful" : "Connection failed"
                    )));

        } catch (Exception e) {
            logger.error("Error testing connection: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(Map.of(
                    "ok", false,
                    "message", "Error: " + e.getMessage()
                ))
            );
        }
    }

    /**
     * Get supported data source types
     */
    @GetMapping("/supported-types")
    public ResponseEntity<Map<String, Object>> getSupportedTypes() {
        return ResponseEntity.ok(Map.of(
            "file_sources", new String[]{
                "FILE_CSV", "FILE_JSON", "FILE_GEOJSON", "FILE_SHP"
            },
            "database_sources", new String[]{
                "DATABASE_MYSQL", "DATABASE_MONGODB", "DATABASE_PRESTO"
            }
        ));
    }

    /**
     * Check if a specific data source type is supported
     */
    @GetMapping("/is-supported")
    public ResponseEntity<Map<String, Object>> isSupported(@RequestParam DataSourceType sourceType) {
        boolean supported = repositoryFactory.isSupported(sourceType);
        return ResponseEntity.ok(Map.of(
            "source_type", sourceType.name(),
            "supported", supported
        ));
    }
}