package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * Search result for spatial queries
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchResult {
    private String id;
    private Tile38Object object;
    private double distance;
    private boolean withinArea;
    
    public SearchResult(String id, Tile38Object object) {
        this.id = id;
        this.object = object;
    }
}