package com.tile38.repository.data;

import com.tile38.loader.DataLoader.LoadResult;

import java.util.concurrent.CompletableFuture;

/**
 * Unified interface for loading data from various sources
 */
public interface DataRepository {
    
    /**
     * Load data from the configured data source
     * 
     * @param dataSource Configuration for the data source
     * @return Future containing the load result
     */
    CompletableFuture<LoadResult> loadData(DataSource dataSource);
    
    /**
     * Check if this repository supports the given data source type
     * 
     * @param type The data source type to check
     * @return true if supported, false otherwise
     */
    boolean supports(DataSource.DataSourceType type);
    
    /**
     * Test connection to the data source (for database sources)
     * 
     * @param dataSource Configuration for the data source
     * @return Future containing connection test result
     */
    default CompletableFuture<Boolean> testConnection(DataSource dataSource) {
        return CompletableFuture.completedFuture(true);
    }
}