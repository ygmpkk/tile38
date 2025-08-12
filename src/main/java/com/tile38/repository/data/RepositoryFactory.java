package com.tile38.repository.data;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Factory for creating appropriate data repository instances
 */
@Component
public class RepositoryFactory {
    
    @Autowired
    public List<DataRepository> repositories;
    
    /**
     * Get the appropriate repository for the given data source type
     * 
     * @param type The data source type
     * @return The repository instance that supports this type
     * @throws IllegalArgumentException if no repository supports the type
     */
    public DataRepository getRepository(DataSource.DataSourceType type) {
        return repositories.stream()
                .filter(repo -> repo.supports(type))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "No repository found for data source type: " + type));
    }
    
    /**
     * Check if any repository supports the given data source type
     * 
     * @param type The data source type to check
     * @return true if supported, false otherwise
     */
    public boolean isSupported(DataSource.DataSourceType type) {
        return repositories.stream()
                .anyMatch(repo -> repo.supports(type));
    }
}