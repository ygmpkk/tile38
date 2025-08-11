package com.tile38.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Arrays;

/**
 * Unit tests for FilterCondition functionality
 */
public class FilterConditionTest {
    
    private GeometryFactory geometryFactory;
    private Tile38Object testObject;
    
    @BeforeEach
    public void setUp() {
        geometryFactory = new GeometryFactory();
        Point point = geometryFactory.createPoint(new Coordinate(-115.5, 33.5));
        
        KVData kvData = new KVData();
        kvData.setTag("category", "restaurant");
        kvData.setTag("cuisine", "italian");
        kvData.setAttribute("rating", 4.5);
        kvData.setAttribute("price_range", 25);
        kvData.setAttribute("open", true);
        
        testObject = Tile38Object.builder()
                .id("test-obj")
                .geometry(point)
                .kvData(kvData)
                .build();
    }
    
    @Test
    public void testTagEqualsFilter() {
        FilterCondition filter = FilterCondition.tagEquals("category", "restaurant");
        assertTrue(filter.matches(testObject));
        
        FilterCondition filterFalse = FilterCondition.tagEquals("category", "cafe");
        assertFalse(filterFalse.matches(testObject));
        
        FilterCondition filterNonExistent = FilterCondition.tagEquals("nonexistent", "value");
        assertFalse(filterNonExistent.matches(testObject));
    }
    
    @Test
    public void testAttributeEqualsFilter() {
        FilterCondition filter = FilterCondition.attributeEquals("rating", 4.5);
        assertTrue(filter.matches(testObject));
        
        FilterCondition filterFalse = FilterCondition.attributeEquals("rating", 5.0);
        assertFalse(filterFalse.matches(testObject));
    }
    
    @Test
    public void testNumericComparisons() {
        // Greater than
        FilterCondition gt = FilterCondition.attributeGreaterThan("rating", 4.0);
        assertTrue(gt.matches(testObject));
        
        FilterCondition gtFalse = FilterCondition.attributeGreaterThan("rating", 5.0);
        assertFalse(gtFalse.matches(testObject));
        
        // Less than
        FilterCondition lt = FilterCondition.builder()
                .key("price_range")
                .operator(FilterCondition.Operator.LESS_THAN)
                .value(30)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();
        assertTrue(lt.matches(testObject));
        
        // Greater or equal
        FilterCondition ge = FilterCondition.builder()
                .key("rating")
                .operator(FilterCondition.Operator.GREATER_EQUAL)
                .value(4.5)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();
        assertTrue(ge.matches(testObject));
        
        // Less or equal
        FilterCondition le = FilterCondition.builder()
                .key("price_range")
                .operator(FilterCondition.Operator.LESS_EQUAL)
                .value(25)
                .dataType(FilterCondition.DataType.ATTRIBUTE)
                .build();
        assertTrue(le.matches(testObject));
    }
    
    @Test
    public void testInFilter() {
        List<String> values = Arrays.asList("italian", "french", "mexican");
        FilterCondition filter = FilterCondition.tagIn("cuisine", values);
        assertTrue(filter.matches(testObject));
        
        List<String> valuesFalse = Arrays.asList("chinese", "thai", "indian");
        FilterCondition filterFalse = FilterCondition.tagIn("cuisine", valuesFalse);
        assertFalse(filterFalse.matches(testObject));
    }
    
    @Test
    public void testStringOperations() {
        // Contains
        FilterCondition contains = FilterCondition.builder()
                .key("cuisine")
                .operator(FilterCondition.Operator.CONTAINS)
                .value("ital")
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertTrue(contains.matches(testObject));
        
        // Starts with
        FilterCondition startsWith = FilterCondition.builder()
                .key("cuisine")
                .operator(FilterCondition.Operator.STARTS_WITH)
                .value("ita")
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertTrue(startsWith.matches(testObject));
        
        // Ends with
        FilterCondition endsWith = FilterCondition.builder()
                .key("cuisine")
                .operator(FilterCondition.Operator.ENDS_WITH)
                .value("lian")
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertTrue(endsWith.matches(testObject));
    }
    
    @Test
    public void testExistenceFilters() {
        FilterCondition exists = FilterCondition.tagExists("category");
        assertTrue(exists.matches(testObject));
        
        FilterCondition notExists = FilterCondition.builder()
                .key("nonexistent")
                .operator(FilterCondition.Operator.NOT_EXISTS)
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertTrue(notExists.matches(testObject));
        
        FilterCondition existsFalse = FilterCondition.tagExists("nonexistent");
        assertFalse(existsFalse.matches(testObject));
    }
    
    @Test
    public void testAndCondition() {
        FilterCondition condition1 = FilterCondition.tagEquals("category", "restaurant");
        FilterCondition condition2 = FilterCondition.attributeGreaterThan("rating", 4.0);
        
        FilterCondition andFilter = FilterCondition.and(Arrays.asList(condition1, condition2));
        assertTrue(andFilter.matches(testObject));
        
        FilterCondition condition3 = FilterCondition.tagEquals("category", "cafe");
        FilterCondition andFilterFalse = FilterCondition.and(Arrays.asList(condition1, condition3));
        assertFalse(andFilterFalse.matches(testObject));
    }
    
    @Test
    public void testOrCondition() {
        FilterCondition condition1 = FilterCondition.tagEquals("category", "cafe");
        FilterCondition condition2 = FilterCondition.attributeGreaterThan("rating", 4.0);
        
        FilterCondition orFilter = FilterCondition.or(Arrays.asList(condition1, condition2));
        assertTrue(orFilter.matches(testObject)); // Second condition is true
        
        FilterCondition condition3 = FilterCondition.tagEquals("category", "cafe");
        FilterCondition condition4 = FilterCondition.attributeGreaterThan("rating", 5.0);
        FilterCondition orFilterFalse = FilterCondition.or(Arrays.asList(condition3, condition4));
        assertFalse(orFilterFalse.matches(testObject)); // Both conditions are false
    }
    
    @Test
    public void testNotEqualsFilter() {
        FilterCondition filter = FilterCondition.builder()
                .key("category")
                .operator(FilterCondition.Operator.NOT_EQUALS)
                .value("cafe")
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertTrue(filter.matches(testObject));
        
        FilterCondition filterFalse = FilterCondition.builder()
                .key("category")
                .operator(FilterCondition.Operator.NOT_EQUALS)
                .value("restaurant")
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertFalse(filterFalse.matches(testObject));
    }
    
    @Test
    public void testEmptyObjectFilter() {
        Tile38Object emptyObject = Tile38Object.builder()
                .id("empty")
                .geometry(geometryFactory.createPoint(new Coordinate(0, 0)))
                .build();
        
        FilterCondition filter = FilterCondition.tagExists("category");
        assertFalse(filter.matches(emptyObject));
        
        FilterCondition notExists = FilterCondition.builder()
                .key("category")
                .operator(FilterCondition.Operator.NOT_EXISTS)
                .dataType(FilterCondition.DataType.TAG)
                .build();
        assertTrue(notExists.matches(emptyObject));
    }
}