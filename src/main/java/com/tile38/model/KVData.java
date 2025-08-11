package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

/**
 * Memory-optimized KV data structure for tags and attributes
 * Designed for million-level data with efficient memory layout
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KVData {
    
    // Tags: String key-value pairs (typically for categorization)
    @Builder.Default
    private Map<String, String> tags = new ConcurrentHashMap<>();
    
    // Attributes: Mixed type key-value pairs (for flexible data storage)
    @Builder.Default  
    private Map<String, Object> attributes = new ConcurrentHashMap<>();
    
    /**
     * Get a tag value by key
     */
    public String getTag(String key) {
        return tags.get(key);
    }
    
    /**
     * Set a tag value
     */
    public void setTag(String key, String value) {
        if (value == null) {
            tags.remove(key);
        } else {
            tags.put(key, value);
        }
    }
    
    /**
     * Get an attribute value by key
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }
    
    /**
     * Set an attribute value
     */
    public void setAttribute(String key, Object value) {
        if (value == null) {
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }
    
    /**
     * Get attribute as String
     */
    public String getAttributeAsString(String key) {
        Object value = attributes.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Get attribute as Number
     */
    public Number getAttributeAsNumber(String key) {
        Object value = attributes.get(key);
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Get attribute as Boolean
     */
    public Boolean getAttributeAsBoolean(String key) {
        Object value = attributes.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return null;
    }
    
    /**
     * Check if a tag exists
     */
    public boolean hasTag(String key) {
        return tags.containsKey(key);
    }
    
    /**
     * Check if an attribute exists
     */
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }
    
    /**
     * Remove a tag
     */
    public void removeTag(String key) {
        tags.remove(key);
    }
    
    /**
     * Remove an attribute
     */
    public void removeAttribute(String key) {
        attributes.remove(key);
    }
    
    /**
     * Clear all tags
     */
    public void clearTags() {
        tags.clear();
    }
    
    /**
     * Clear all attributes
     */
    public void clearAttributes() {
        attributes.clear();
    }
    
    /**
     * Clear all data
     */
    public void clear() {
        clearTags();
        clearAttributes();
    }
    
    /**
     * Check if KV data is empty
     */
    public boolean isEmpty() {
        return tags.isEmpty() && attributes.isEmpty();
    }
    
    /**
     * Get total number of KV pairs
     */
    public int size() {
        return tags.size() + attributes.size();
    }
    
    /**
     * Merge with another KVData (other values override existing ones)
     */
    public void merge(KVData other) {
        if (other != null) {
            if (other.tags != null) {
                this.tags.putAll(other.tags);
            }
            if (other.attributes != null) {
                this.attributes.putAll(other.attributes);
            }
        }
    }
    
    /**
     * Create a copy of this KVData
     */
    public KVData copy() {
        return KVData.builder()
                .tags(new HashMap<>(this.tags))
                .attributes(new HashMap<>(this.attributes))
                .build();
    }
}