package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tile38.model.FilterRequest;

/**
 * Parameter class for nearby search operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NearbySearchParam {
    
    private Double lat;
    
    private Double lon;
    
    private Double radius;
    
    /**
     * Simple string-based filter
     */
    private String filter;
    
    /**
     * Complex structured filter
     */
    private FilterRequest filterRequest;
    
    /**
     * Pagination parameters
     */
    private Integer limit;
    private Integer offset;
}