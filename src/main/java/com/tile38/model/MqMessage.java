package com.tile38.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.Map;

/**
 * Message format for MQ-based data ingestion
 * Supports SET, DEL, and DROP operations
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MqMessage {
    
    /**
     * Operation type: SET, DEL, DROP
     */
    private String operation;
    
    /**
     * Collection/key name
     */
    private String collection;
    
    /**
     * Object ID
     */
    private String id;
    
    /**
     * Geometry data (for SET operations)
     */
    private GeometryData geometry;
    
    /**
     * Additional fields
     */
    private Map<String, Object> fields;
    
    /**
     * Optional expiration time in milliseconds
     */
    private Long expireAt;
    
    /**
     * Geometry data structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GeometryData {
        /**
         * Geometry type: Point, LineString, Polygon, etc.
         */
        private String type;
        
        /**
         * Coordinates array
         * For Point: [lon, lat]
         * For LineString: [[lon1, lat1], [lon2, lat2], ...]
         * For Polygon: [[[lon1, lat1], [lon2, lat2], ...]]
         */
        private Object coordinates;
        
        /**
         * Optional WKT (Well-Known Text) representation
         */
        private String wkt;
    }
}
