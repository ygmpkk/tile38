package com.tile38.model.base;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Generic base class for search results
 * Provides standardized structure for search operations
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseSearchResult<T, ID> {
    
    /**
     * Result identifier
     */
    private ID id;
    
    /**
     * The actual search result entity
     */
    private T entity;
    
    /**
     * Search relevance score or distance
     */
    private Double score;
    
    /**
     * Additional metadata about the search result
     */
    private Object metadata;
    
    /**
     * Whether the result matches the search criteria exactly
     */
    private Boolean exact;
}