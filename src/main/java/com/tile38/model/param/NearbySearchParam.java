package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tile38.model.FilterRequest;
import com.tile38.model.LocationEntity;

/**
 * Parameter class for nearby search operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NearbySearchParam {
    
    /**
     * Unified location entity for consistent spatial handling
     */
    private LocationEntity location;
    
    /**
     * Legacy fields for backward compatibility - will be deprecated
     */
    @Deprecated
    private Double lat;
    
    @Deprecated
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
    
    /**
     * Get effective location entity (prioritizes unified location over legacy lat/lon)
     */
    @JsonIgnore
    public LocationEntity getEffectiveLocation() {
        if (location != null && location.isValid()) {
            return location;
        }
        
        // Fallback to legacy lat/lon for backward compatibility
        if (lat != null && lon != null) {
            return LocationEntity.of(lat, lon);
        }
        
        return null;
    }
    
    /**
     * Check if parameters have valid location data
     */
    @JsonIgnore
    public boolean hasValidLocation() {
        LocationEntity effectiveLocation = getEffectiveLocation();
        return effectiveLocation != null && effectiveLocation.isValid();
    }
    
    /**
     * Get effective latitude for backward compatibility
     */
    @JsonIgnore
    public Double getEffectiveLat() {
        LocationEntity effectiveLocation = getEffectiveLocation();
        return effectiveLocation != null ? effectiveLocation.getEffectiveLat() : null;
    }
    
    /**
     * Get effective longitude for backward compatibility
     */
    @JsonIgnore
    public Double getEffectiveLon() {
        LocationEntity effectiveLocation = getEffectiveLocation();
        return effectiveLocation != null ? effectiveLocation.getEffectiveLon() : null;
    }
}