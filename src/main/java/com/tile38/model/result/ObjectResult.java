package com.tile38.model.result;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tile38.model.Tile38Object;

/**
 * Result class for object operations (set, get, etc.)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ObjectResult {
    
    private Tile38Object object;
    private Integer deleted;
    private Integer updated;
    private Boolean found;
}