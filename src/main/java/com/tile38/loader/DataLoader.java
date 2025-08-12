package com.tile38.loader;

import com.tile38.repository.data.DataSource;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for bulk data loading operations
 */
public interface DataLoader {
    
    /**
     * Load data from JSON file
     */
    CompletableFuture<LoadResult> loadFromJson(String filePath);
    
    /**
     * Load data from CSV file
     */
    CompletableFuture<LoadResult> loadFromCsv(String filePath);
    
    /**
     * Generate synthetic test data for performance testing
     */
    CompletableFuture<LoadResult> generateTestData(String collectionName, int numberOfRecords, 
                                                   double minLat, double maxLat, double minLon, double maxLon);
    
    /**
     * Load data from any supported data source using the unified repository abstraction
     */
    CompletableFuture<LoadResult> loadFromDataSource(DataSource dataSource);
    
    /**
     * Result of a data loading operation
     */
    class LoadResult {
        private final boolean success;
        private final long recordsLoaded;
        private final long durationMs;
        private final String message;
        
        public LoadResult(boolean success, long recordsLoaded, long durationMs, String message) {
            this.success = success;
            this.recordsLoaded = recordsLoaded;
            this.durationMs = durationMs;
            this.message = message;
        }
        
        // Getters
        public boolean isSuccess() { return success; }
        public long getRecordsLoaded() { return recordsLoaded; }
        public long getDurationMs() { return durationMs; }
        public String getMessage() { return message; }
        
        @Override
        public String toString() {
            return String.format("LoadResult{success=%s, records=%d, duration=%dms, message='%s'}", 
                               success, recordsLoaded, durationMs, message);
        }
    }
}