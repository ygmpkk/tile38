package com.tile38.model.result;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tile38.model.Bounds;

import java.util.List;

/**
 * Result class for collection operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CollectionResult {
    
    private List<String> keys;
    private Bounds bounds;
    private Integer dropped;
}