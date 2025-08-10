package com.tile38.enterprise.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Represents a geospatial object in Tile38
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GeospatialObject {
    
    private String id;
    private String collection;
    private Object geometry; // Can be Point, Polygon, etc.
    private Map<String, Object> fields;
    private Instant timestamp;
    private Double distance; // For nearby queries
    
    public GeospatialObject() {}
    
    public GeospatialObject(String id, String collection, Object geometry) {
        this.id = id;
        this.collection = collection;
        this.geometry = geometry;
        this.timestamp = Instant.now();
    }
    
    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getCollection() { return collection; }
    public void setCollection(String collection) { this.collection = collection; }
    
    public Object getGeometry() { return geometry; }
    public void setGeometry(Object geometry) { this.geometry = geometry; }
    
    public Map<String, Object> getFields() { return fields; }
    public void setFields(Map<String, Object> fields) { this.fields = fields; }
    
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    
    public Double getDistance() { return distance; }
    public void setDistance(Double distance) { this.distance = distance; }
}