package com.tile38.model.result;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.tile38.model.SearchResult;

import java.util.List;

/**
 * Result class for search operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SearchResultData {
    
    private Integer count;
    private List<SearchResult> objects;
    private String cursor;  // For pagination
}