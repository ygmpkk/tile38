package com.tile38.enterprise.model;

/**
 * Represents a geographic point with latitude and longitude
 */
public class Point {
    
    private double latitude;
    private double longitude;
    private Double altitude; // Optional Z coordinate
    
    public Point() {}
    
    public Point(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }
    
    public Point(double latitude, double longitude, Double altitude) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
    }
    
    // Getters and setters
    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    
    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    
    public Double getAltitude() { return altitude; }
    public void setAltitude(Double altitude) { this.altitude = altitude; }
    
    @Override
    public String toString() {
        return altitude != null ? 
            String.format("POINT(%f %f %f)", longitude, latitude, altitude) :
            String.format("POINT(%f %f)", longitude, latitude);
    }
}