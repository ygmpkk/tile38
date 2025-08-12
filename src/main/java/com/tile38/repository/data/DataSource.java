package com.tile38.repository.data;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Represents a data source configuration for loading geospatial data
 */
@Data
@Builder
public class DataSource {
    
    /**
     * Type of data source
     */
    public enum DataSourceType {
        FILE_CSV,
        FILE_JSON,  
        FILE_GEOJSON,
        FILE_SHP,
        DATABASE_MYSQL,
        DATABASE_MONGODB,
        DATABASE_PRESTO
    }
    
    private DataSourceType type;
    private String location; // File path or database connection string
    private String collectionName; // Target collection name in Tile38
    private Map<String, String> properties; // Additional configuration properties
    private String query; // SQL query for database sources
    
    /**
     * Create a file-based data source
     */
    public static DataSource createFileSource(DataSourceType type, String filePath, String collectionName) {
        return DataSource.builder()
                .type(type)
                .location(filePath)
                .collectionName(collectionName)
                .build();
    }
    
    /**
     * Create a database-based data source
     */
    public static DataSource createDatabaseSource(DataSourceType type, String connectionString, 
                                                String collectionName, String query, 
                                                Map<String, String> properties) {
        return DataSource.builder()
                .type(type)
                .location(connectionString)
                .collectionName(collectionName)
                .query(query)
                .properties(properties)
                .build();
    }
}