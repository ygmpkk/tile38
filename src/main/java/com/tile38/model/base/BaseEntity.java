package com.tile38.model.base;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Base entity with common fields using generics
 * Provides standardized structure for all domain entities
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class BaseEntity<ID> {
    
    /**
     * Unique identifier for the entity
     */
    private ID id;
    
    /**
     * Creation timestamp
     */
    private long timestamp;
    
    /**
     * Optional expiration time
     */
    private Instant expireAt;
    
    /**
     * Check if entity has expired
     */
    public boolean isExpired() {
        return expireAt != null && Instant.now().isAfter(expireAt);
    }
    
    /**
     * Set expiration in seconds from now
     */
    public void setExpirationSeconds(long seconds) {
        this.expireAt = Instant.now().plusSeconds(seconds);
    }
}