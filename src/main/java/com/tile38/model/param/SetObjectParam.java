package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tile38.model.KVData;
import org.locationtech.jts.geom.Geometry;

import java.util.Map;

/**
 * Parameter class for setting/storing polygon objects
 * Focused on polygon-centric design with KV data as supplemental metadata
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SetObjectParam {
    
    /**
     * Geometry of the polygon object (core data)
     */
    private Geometry geometry;
    
    /**
     * Legacy fields for backward compatibility
     */
    private Map<String, Object> fields;
    
    /**
     * KV data entity for tags and attributes (supplemental to polygon)
     */
    private KVData kvData;
    
    /**
     * Expiration in seconds
     */
    private Long ex;
    
    /**
     * Check if parameters have valid geometry data
     */
    @JsonIgnore
    public boolean hasValidGeometry() {
        return geometry != null && !geometry.isEmpty();
    }
    
    /**
     * Get effective KV data (create empty if null)
     */
    @JsonIgnore
    public KVData getEffectiveKVData() {
        return kvData != null ? kvData : new KVData();
    }
}