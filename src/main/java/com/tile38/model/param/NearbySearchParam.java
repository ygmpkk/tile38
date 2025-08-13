package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tile38.model.FilterRequest;
import org.locationtech.jts.geom.Point;

/**
 * Parameter class for nearby search operations on polygon data
 * Searches for polygon objects near a specified point
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NearbySearchParam {
    
    /**
     * Search center point (for finding nearby polygons)
     */
    private Point centerPoint;
    
    /**
     * Search radius in meters
     */
    private Double radius;
    
    /**
     * Simple string-based filter for KV data
     */
    private String filter;
    
    /**
     * Complex structured filter for KV data
     */
    private FilterRequest filterRequest;
    
    /**
     * Pagination parameters
     */
    private Integer limit;
    private Integer offset;
    
    /**
     * Check if parameters have valid center point
     */
    @JsonIgnore
    public boolean hasValidCenterPoint() {
        return centerPoint != null && !centerPoint.isEmpty();
    }
}