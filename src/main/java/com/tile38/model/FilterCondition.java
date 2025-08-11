package com.tile38.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Filter condition for KV data queries
 * Supports various comparison operators and multi-condition filtering
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FilterCondition {
    
    public enum Operator {
        EQUALS,          // ==
        NOT_EQUALS,      // !=
        GREATER_THAN,    // >
        GREATER_EQUAL,   // >=
        LESS_THAN,       // <
        LESS_EQUAL,      // <=
        IN,              // in (list)
        NOT_IN,          // not in (list)
        CONTAINS,        // string contains
        NOT_CONTAINS,    // string not contains
        STARTS_WITH,     // string starts with
        ENDS_WITH,       // string ends with
        EXISTS,          // key exists
        NOT_EXISTS       // key does not exist
    }
    
    public enum LogicalOperator {
        AND,
        OR
    }
    
    public enum DataType {
        TAG,        // Filter on tags
        ATTRIBUTE   // Filter on attributes
    }
    
    private String key;                    // KV key to filter on
    private Operator operator;             // Comparison operator
    private Object value;                  // Single value for comparison
    private List<Object> values;           // Multiple values for IN/NOT_IN
    private DataType dataType;             // Whether to filter tags or attributes
    
    // For complex multi-condition filtering
    private List<FilterCondition> conditions;  // Sub-conditions
    private LogicalOperator logicalOperator;   // AND/OR for combining conditions
    
    /**
     * Check if this object matches the filter condition
     */
    public boolean matches(Tile38Object object) {
        if (object == null) {
            return false;
        }
        
        // Handle complex conditions with sub-conditions
        if (conditions != null && !conditions.isEmpty()) {
            return evaluateComplexCondition(object);
        }
        
        // Handle simple condition
        return evaluateSimpleCondition(object);
    }
    
    private boolean evaluateComplexCondition(Tile38Object object) {
        if (logicalOperator == LogicalOperator.AND) {
            return conditions.stream().allMatch(condition -> condition.matches(object));
        } else { // OR
            return conditions.stream().anyMatch(condition -> condition.matches(object));
        }
    }
    
    private boolean evaluateSimpleCondition(Tile38Object object) {
        KVData kvData = object.getKvData();
        if (kvData == null) {
            return operator == Operator.NOT_EXISTS;
        }
        
        Object actualValue = getActualValue(kvData);
        
        switch (operator) {
            case EXISTS:
                return actualValue != null;
                
            case NOT_EXISTS:
                return actualValue == null;
                
            case EQUALS:
                return objectsEqual(actualValue, value);
                
            case NOT_EQUALS:
                return !objectsEqual(actualValue, value);
                
            case GREATER_THAN:
                return compareNumbers(actualValue, value) > 0;
                
            case GREATER_EQUAL:
                return compareNumbers(actualValue, value) >= 0;
                
            case LESS_THAN:
                return compareNumbers(actualValue, value) < 0;
                
            case LESS_EQUAL:
                return compareNumbers(actualValue, value) <= 0;
                
            case IN:
                return values != null && values.stream().anyMatch(v -> objectsEqual(actualValue, v));
                
            case NOT_IN:
                return values == null || values.stream().noneMatch(v -> objectsEqual(actualValue, v));
                
            case CONTAINS:
                return actualValue != null && value != null && 
                       actualValue.toString().contains(value.toString());
                
            case NOT_CONTAINS:
                return actualValue == null || value == null || 
                       !actualValue.toString().contains(value.toString());
                
            case STARTS_WITH:
                return actualValue != null && value != null && 
                       actualValue.toString().startsWith(value.toString());
                
            case ENDS_WITH:
                return actualValue != null && value != null && 
                       actualValue.toString().endsWith(value.toString());
                
            default:
                return false;
        }
    }
    
    private Object getActualValue(KVData kvData) {
        if (dataType == DataType.TAG) {
            return kvData.getTag(key);
        } else {
            return kvData.getAttribute(key);
        }
    }
    
    private boolean objectsEqual(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        
        // Handle numeric comparisons
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
        }
        
        return a.toString().equals(b.toString());
    }
    
    private int compareNumbers(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot compare null values");
        }
        
        Number numA = convertToNumber(a);
        Number numB = convertToNumber(b);
        
        if (numA == null || numB == null) {
            throw new IllegalArgumentException("Cannot compare non-numeric values: " + a + ", " + b);
        }
        
        return Double.compare(numA.doubleValue(), numB.doubleValue());
    }
    
    private Number convertToNumber(Object obj) {
        if (obj instanceof Number) {
            return (Number) obj;
        }
        if (obj instanceof String) {
            try {
                return Double.parseDouble((String) obj);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    // Builder helper methods for common conditions
    
    public static FilterCondition tagEquals(String key, String value) {
        return FilterCondition.builder()
                .key(key)
                .operator(Operator.EQUALS)
                .value(value)
                .dataType(DataType.TAG)
                .build();
    }
    
    public static FilterCondition attributeEquals(String key, Object value) {
        return FilterCondition.builder()
                .key(key)
                .operator(Operator.EQUALS)
                .value(value)
                .dataType(DataType.ATTRIBUTE)
                .build();
    }
    
    public static FilterCondition tagIn(String key, List<String> values) {
        return FilterCondition.builder()
                .key(key)
                .operator(Operator.IN)
                .values(List.copyOf(values))
                .dataType(DataType.TAG)
                .build();
    }
    
    public static FilterCondition attributeGreaterThan(String key, Number value) {
        return FilterCondition.builder()
                .key(key)
                .operator(Operator.GREATER_THAN)
                .value(value)
                .dataType(DataType.ATTRIBUTE)
                .build();
    }
    
    public static FilterCondition tagExists(String key) {
        return FilterCondition.builder()
                .key(key)
                .operator(Operator.EXISTS)
                .dataType(DataType.TAG)
                .build();
    }
    
    public static FilterCondition and(List<FilterCondition> conditions) {
        return FilterCondition.builder()
                .conditions(conditions)
                .logicalOperator(LogicalOperator.AND)
                .build();
    }
    
    public static FilterCondition or(List<FilterCondition> conditions) {
        return FilterCondition.builder()
                .conditions(conditions)
                .logicalOperator(LogicalOperator.OR)
                .build();
    }
}