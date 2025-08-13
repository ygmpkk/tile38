package com.tile38.model;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.EqualsAndHashCode;
import com.tile38.model.base.BaseSearchResult;

/**
 * Search result for spatial queries
 * Now extends BaseSearchResult for generic capabilities
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class SearchResult extends BaseSearchResult<Tile38Object, String> {
    
    // Backward compatibility fields
    private double distance;
    private boolean withinArea;
    
    public SearchResult(String id, Tile38Object object) {
        super();
        setId(id);
        setEntity(object);
    }
    
    public SearchResult(String id, Tile38Object object, double distance) {
        super();
        setId(id);
        setEntity(object);
        setScore(distance);
        this.distance = distance;
    }
    
    // Backward compatibility methods
    public Tile38Object getObject() {
        return getEntity();
    }
    
    public void setObject(Tile38Object object) {
        setEntity(object);
    }
}