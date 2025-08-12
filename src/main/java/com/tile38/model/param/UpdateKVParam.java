package com.tile38.model.param;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tile38.model.KVData;

import java.util.Map;

/**
 * Unified parameter class for KV data operations
 * Supports both structured KVData and legacy Map formats
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateKVParam {
    
    /**
     * Unified KV data entity
     */
    private KVData kvData;
    
    /**
     * Legacy fields for backward compatibility
     */
    private Map<String, Object> tags;
    private Map<String, Object> attributes;
    
    /**
     * Get effective KV data (prioritizes unified kvData over legacy maps)
     */
    @JsonIgnore
    public KVData getEffectiveKVData() {
        if (kvData != null && !kvData.isEmpty()) {
            return kvData;
        }
        
        // Build KVData from legacy maps for backward compatibility
        if ((tags != null && !tags.isEmpty()) || (attributes != null && !attributes.isEmpty())) {
            KVData legacyKVData = new KVData();
            
            if (tags != null) {
                tags.forEach((k, v) -> legacyKVData.setTag(k, v != null ? v.toString() : null));
            }
            
            if (attributes != null) {
                attributes.forEach(legacyKVData::setAttribute);
            }
            
            return legacyKVData;
        }
        
        return null;
    }
    
    /**
     * Check if parameters have valid KV data
     */
    @JsonIgnore
    public boolean hasValidKVData() {
        KVData effectiveKVData = getEffectiveKVData();
        return effectiveKVData != null && !effectiveKVData.isEmpty();
    }
}