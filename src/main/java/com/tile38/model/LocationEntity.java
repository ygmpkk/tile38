package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * Unified location entity for consistent spatial parameter handling
 * Encapsulates latitude and longitude coordinates with geometry conversion
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LocationEntity {
    
    /**
     * Latitude coordinate
     */
    private Double lat;
    
    /**
     * Longitude coordinate  
     */
    private Double lon;
    
    /**
     * Alternative field names for backward compatibility
     */
    private Double latitude;
    private Double longitude;
    
    /**
     * Get effective latitude value (lat takes precedence over latitude)
     */
    @JsonIgnore
    public Double getEffectiveLat() {
        return lat != null ? lat : latitude;
    }
    
    /**
     * Get effective longitude value (lon takes precedence over longitude)
     */
    @JsonIgnore
    public Double getEffectiveLon() {
        return lon != null ? lon : longitude;
    }
    
    /**
     * Check if location has valid coordinates
     */
    @JsonIgnore
    public boolean isValid() {
        Double effLat = getEffectiveLat();
        Double effLon = getEffectiveLon();
        return effLat != null && effLon != null && 
               effLat >= -90.0 && effLat <= 90.0 &&
               effLon >= -180.0 && effLon <= 180.0;
    }
    
    /**
     * Convert to JTS Point geometry
     */
    @JsonIgnore
    public Point toPoint() {
        if (!isValid()) {
            throw new IllegalStateException("Invalid coordinates: lat=" + getEffectiveLat() + ", lon=" + getEffectiveLon());
        }
        
        GeometryFactory factory = new GeometryFactory();
        return factory.createPoint(new Coordinate(getEffectiveLon(), getEffectiveLat()));
    }
    
    /**
     * Create LocationEntity from lat/lon values
     */
    public static LocationEntity of(double lat, double lon) {
        return LocationEntity.builder()
                .lat(lat)
                .lon(lon)
                .build();
    }
    
    /**
     * Create LocationEntity from JTS Point
     */
    public static LocationEntity fromPoint(Point point) {
        if (point == null) return null;
        
        return LocationEntity.builder()
                .lat(point.getY())
                .lon(point.getX())
                .build();
    }
    
    /**
     * Calculate distance to another location in meters (Haversine formula)
     */
    public double distanceTo(LocationEntity other) {
        if (!this.isValid() || !other.isValid()) {
            throw new IllegalArgumentException("Invalid coordinates for distance calculation");
        }
        
        final double R = 6371000; // Earth's radius in meters
        
        double lat1Rad = Math.toRadians(this.getEffectiveLat());
        double lat2Rad = Math.toRadians(other.getEffectiveLat());
        double deltaLatRad = Math.toRadians(other.getEffectiveLat() - this.getEffectiveLat());
        double deltaLonRad = Math.toRadians(other.getEffectiveLon() - this.getEffectiveLon());
        
        double a = Math.sin(deltaLatRad/2) * Math.sin(deltaLatRad/2) +
                   Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                   Math.sin(deltaLonRad/2) * Math.sin(deltaLonRad/2);
        
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        
        return R * c;
    }
    
    @Override
    public String toString() {
        return String.format("LocationEntity(lat=%.6f, lon=%.6f)", getEffectiveLat(), getEffectiveLon());
    }
}