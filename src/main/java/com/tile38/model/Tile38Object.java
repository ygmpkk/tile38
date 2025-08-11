package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.locationtech.jts.geom.Geometry;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Tile38 Object - represents a geospatial object in the database
 * Enhanced with structured KV data support for tags and attributes
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Tile38Object {
    private String id;
    private Geometry geometry;
    
    // Legacy fields for backward compatibility
    private Map<String, Object> fields;
    
    // New structured KV data for tags and attributes
    private KVData kvData;
    
    private Instant expireAt;
    private long timestamp;
    
    /**
     * Get the object as GeoJSON
     */
    public String toGeoJSON() {
        // TODO: Implement GeoJSON serialization
        return null;
    }
    
    /**
     * Check if object has expired
     */
    public boolean isExpired() {
        return expireAt != null && Instant.now().isAfter(expireAt);
    }
    
    /**
     * Get or create KV data
     */
    public KVData getKvData() {
        if (kvData == null) {
            kvData = new KVData();
        }
        return kvData;
    }
    
    /**
     * Set a tag value
     */
    public void setTag(String key, String value) {
        getKvData().setTag(key, value);
    }
    
    /**
     * Get a tag value
     */
    public String getTag(String key) {
        return kvData != null ? kvData.getTag(key) : null;
    }
    
    /**
     * Set an attribute value
     */
    public void setAttribute(String key, Object value) {
        getKvData().setAttribute(key, value);
    }
    
    /**
     * Get an attribute value
     */
    public Object getAttribute(String key) {
        return kvData != null ? kvData.getAttribute(key) : null;
    }
    
    /**
     * Update KV data from another KVData object
     */
    public void updateKVData(KVData newKvData) {
        if (newKvData != null) {
            getKvData().merge(newKvData);
        }
    }
    
    /**
     * Check if matches filter condition
     */
    public boolean matchesFilter(FilterCondition filter) {
        return filter == null || filter.matches(this);
    }
}