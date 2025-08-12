package com.tile38.model.base;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Generic base class for search criteria
 * Provides standardized structure for search parameters
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseSearchCriteria<T> {
    
    /**
     * Collection or key to search in
     */
    private String collection;
    
    /**
     * Optional filter criteria
     */
    private T filterCriteria;
    
    /**
     * Maximum number of results to return
     */
    private Integer limit;
    
    /**
     * Number of results to skip (for pagination)
     */
    private Integer offset;
    
    /**
     * Sort field and direction
     */
    private String sortBy;
    private String sortDirection;
}