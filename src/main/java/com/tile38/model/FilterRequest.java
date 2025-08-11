package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

/**
 * Request object for filter conditions in HTTP API
 * Simplifies JSON parsing of complex filter conditions
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FilterRequest {
    
    // Simple condition fields
    private String key;
    private String operator;  // EQUALS, NOT_EQUALS, GREATER_THAN, etc.
    private Object value;
    private List<Object> values;  // For IN/NOT_IN operations
    private String dataType;  // TAG or ATTRIBUTE
    
    // Complex condition fields
    private List<FilterRequest> conditions;
    private String logicalOperator;  // AND or OR
    
    /**
     * Convert to FilterCondition object
     */
    public FilterCondition toFilterCondition() {
        FilterCondition.FilterConditionBuilder builder = FilterCondition.builder();
        
        // Handle complex conditions
        if (conditions != null && !conditions.isEmpty()) {
            List<FilterCondition> subConditions = conditions.stream()
                    .map(FilterRequest::toFilterCondition)
                    .toList();
            
            builder.conditions(subConditions);
            
            if ("OR".equalsIgnoreCase(logicalOperator)) {
                builder.logicalOperator(FilterCondition.LogicalOperator.OR);
            } else {
                builder.logicalOperator(FilterCondition.LogicalOperator.AND);
            }
        } else {
            // Handle simple condition
            builder.key(key);
            builder.value(value);
            builder.values(values);
            
            // Parse operator
            if (operator != null) {
                try {
                    builder.operator(FilterCondition.Operator.valueOf(operator.toUpperCase()));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid operator: " + operator);
                }
            }
            
            // Parse data type
            if (dataType != null) {
                try {
                    builder.dataType(FilterCondition.DataType.valueOf(dataType.toUpperCase()));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid data type: " + dataType);
                }
            } else {
                // Default to ATTRIBUTE if not specified
                builder.dataType(FilterCondition.DataType.ATTRIBUTE);
            }
        }
        
        return builder.build();
    }
    
    /**
     * Create simple equality filter for tag
     */
    public static FilterRequest tagEquals(String key, String value) {
        return FilterRequest.builder()
                .key(key)
                .operator("EQUALS")
                .value(value)
                .dataType("TAG")
                .build();
    }
    
    /**
     * Create simple equality filter for attribute
     */
    public static FilterRequest attributeEquals(String key, Object value) {
        return FilterRequest.builder()
                .key(key)
                .operator("EQUALS")
                .value(value)
                .dataType("ATTRIBUTE")
                .build();
    }
}