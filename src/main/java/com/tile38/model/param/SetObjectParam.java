package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tile38.model.KVData;
import com.tile38.model.LocationEntity;

import java.util.Map;

/**
 * Parameter class for setting/storing objects
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SetObjectParam {
    
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
    
    /**
     * Legacy fields for backward compatibility
     */
    private Map<String, Object> fields;
    
    /**
     * Unified KV data entity
     */
    private KVData kvData;
    
    /**
     * Legacy structured KV data - will be deprecated
     */
    @Deprecated
    private Map<String, Object> tags;
    @Deprecated
    private Map<String, Object> attributes;
    
    /**
     * Expiration in seconds
     */
    private Long ex;
    
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
     * Get effective KV data (prioritizes unified kvData over legacy maps)
     */
    @JsonIgnore
    public KVData getEffectiveKVData() {
        if (kvData != null && !kvData.isEmpty()) {
            return kvData;
        }
        
        // Build KVData from legacy maps for backward compatibility
        if ((tags != null && !tags.isEmpty()) || (attributes != null && !attributes.isEmpty())) {
            KVData legacyKVData = new KVData();
            
            if (tags != null) {
                tags.forEach((k, v) -> legacyKVData.setTag(k, v != null ? v.toString() : null));
            }
            
            if (attributes != null) {
                attributes.forEach(legacyKVData::setAttribute);
            }
            
            return legacyKVData;
        }
        
        return null;
    }
    
    /**
     * Convert to KVData object - deprecated, use getEffectiveKVData instead
     */
    @Deprecated
    public KVData toKVData() {
        return getEffectiveKVData();
    }
}