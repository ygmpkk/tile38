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
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Tile38Object {
    private String id;
    private Geometry geometry;
    private Map<String, Object> fields;
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
}