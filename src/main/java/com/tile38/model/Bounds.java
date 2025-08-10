package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * Bounds represents the bounding box of a collection
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Bounds {
    private double minX;
    private double minY;
    private double maxX;
    private double maxY;
    
    public boolean isEmpty() {
        return minX == 0 && minY == 0 && maxX == 0 && maxY == 0;
    }
    
    public void extend(double x, double y) {
        if (isEmpty()) {
            minX = maxX = x;
            minY = maxY = y;
        } else {
            minX = Math.min(minX, x);
            minY = Math.min(minY, y);
            maxX = Math.max(maxX, x);
            maxY = Math.max(maxY, y);
        }
    }
    
    public void extend(Bounds other) {
        if (other.isEmpty()) return;
        if (isEmpty()) {
            minX = other.minX;
            minY = other.minY;
            maxX = other.maxX;
            maxY = other.maxY;
        } else {
            minX = Math.min(minX, other.minX);
            minY = Math.min(minY, other.minY);
            maxX = Math.max(maxX, other.maxX);
            maxY = Math.max(maxY, other.maxY);
        }
    }
}