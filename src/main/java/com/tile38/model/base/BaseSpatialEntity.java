package com.tile38.model.base;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.locationtech.jts.geom.Geometry;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Base spatial entity with geometry support using generics
 * Extends BaseEntity with geospatial capabilities
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseSpatialEntity<ID> extends BaseEntity<ID> {
    
    /**
     * Geometric representation of the entity
     */
    private Geometry geometry;
    
    /**
     * Get the center point coordinates for quick access
     */
    public double[] getCenterPoint() {
        if (geometry == null) return null;
        
        var centroid = geometry.getCentroid();
        return new double[]{centroid.getY(), centroid.getX()}; // [lat, lon]
    }
    
    /**
     * Check if this entity intersects with another geometry
     */
    public boolean intersects(Geometry other) {
        return geometry != null && other != null && geometry.intersects(other);
    }
    
    /**
     * Check if this entity is within the given geometry
     */
    public boolean within(Geometry other) {
        return geometry != null && other != null && geometry.within(other);
    }
}