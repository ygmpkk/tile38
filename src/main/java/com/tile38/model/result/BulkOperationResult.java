package com.tile38.model.result;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Result class for bulk operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BulkOperationResult {
    
    private Integer objectsLoaded;
    private Integer objectsGenerated;
    private Integer recordsLoaded;
    private Long durationMs;
    private String message;
    private Boolean success;
}