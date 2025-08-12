package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tile38.model.KVData;

import java.util.Map;

/**
 * Parameter class for setting/storing objects
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SetObjectParam {
    
    private Double lat;
    
    private Double lon;
    
    /**
     * Legacy fields for backward compatibility
     */
    private Map<String, Object> fields;
    
    /**
     * Structured KV data
     */
    private Map<String, Object> tags;
    private Map<String, Object> attributes;
    
    /**
     * Expiration in seconds
     */
    private Long ex;
    
    /**
     * Convert to KVData object
     */
    public KVData toKVData() {
        KVData kvData = new KVData();
        
        if (tags != null) {
            tags.forEach((k, v) -> kvData.setTag(k, v != null ? v.toString() : null));
        }
        
        if (attributes != null) {
            attributes.forEach(kvData::setAttribute);
        }
        
        return kvData.isEmpty() ? null : kvData;
    }
}