package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tile38.model.KVData;

/**
 * Parameter class for updating KV data of existing polygon objects
 * KV data (tags and attributes) is supplemental metadata attached to polygons by ID
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateKVParam {
    
    /**
     * KV data entity for tags and attributes
     */
    private KVData kvData;
    
    /**
     * Check if parameters have valid KV data
     */
    @JsonIgnore
    public boolean hasValidKVData() {
        return kvData != null && !kvData.isEmpty();
    }
}