package com.tile38.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Arrays;

/**
 * Unit tests for KVData functionality
 */
public class KVDataTest {
    
    private KVData kvData;
    
    @BeforeEach
    public void setUp() {
        kvData = new KVData();
    }
    
    @Test
    public void testTagOperations() {
        // Test setting and getting tags
        kvData.setTag("category", "restaurant");
        kvData.setTag("cuisine", "italian");
        
        assertEquals("restaurant", kvData.getTag("category"));
        assertEquals("italian", kvData.getTag("cuisine"));
        assertNull(kvData.getTag("nonexistent"));
        
        // Test tag existence
        assertTrue(kvData.hasTag("category"));
        assertFalse(kvData.hasTag("nonexistent"));
        
        // Test removing tags
        kvData.removeTag("cuisine");
        assertNull(kvData.getTag("cuisine"));
        assertFalse(kvData.hasTag("cuisine"));
        
        // Test setting null value (should remove)
        kvData.setTag("category", null);
        assertNull(kvData.getTag("category"));
    }
    
    @Test
    public void testAttributeOperations() {
        // Test different attribute types
        kvData.setAttribute("price", 25.50);
        kvData.setAttribute("rating", 4);
        kvData.setAttribute("open", true);
        kvData.setAttribute("name", "Test Restaurant");
        
        assertEquals(25.50, kvData.getAttribute("price"));
        assertEquals(4, kvData.getAttribute("rating"));
        assertEquals(true, kvData.getAttribute("open"));
        assertEquals("Test Restaurant", kvData.getAttribute("name"));
        
        // Test type-specific getters
        assertEquals("25.5", kvData.getAttributeAsString("price"));
        assertEquals(25.5, kvData.getAttributeAsNumber("price").doubleValue());
        assertEquals(4, kvData.getAttributeAsNumber("rating").intValue());
        assertTrue(kvData.getAttributeAsBoolean("open"));
        
        // Test attribute existence
        assertTrue(kvData.hasAttribute("price"));
        assertFalse(kvData.hasAttribute("nonexistent"));
        
        // Test removing attributes
        kvData.removeAttribute("rating");
        assertNull(kvData.getAttribute("rating"));
    }
    
    @Test
    public void testTypeConversions() {
        // Test string to number conversion
        kvData.setAttribute("stringNumber", "123.45");
        assertEquals(123.45, kvData.getAttributeAsNumber("stringNumber").doubleValue());
        
        // Test string to boolean conversion
        kvData.setAttribute("stringBool", "true");
        assertTrue(kvData.getAttributeAsBoolean("stringBool"));
        
        // Test invalid conversions
        kvData.setAttribute("invalidNumber", "not-a-number");
        assertNull(kvData.getAttributeAsNumber("invalidNumber"));
    }
    
    @Test
    public void testUtilityMethods() {
        assertTrue(kvData.isEmpty());
        assertEquals(0, kvData.size());
        
        kvData.setTag("category", "food");
        kvData.setAttribute("price", 10.0);
        
        assertFalse(kvData.isEmpty());
        assertEquals(2, kvData.size());
        
        // Test clear operations
        kvData.clearTags();
        assertEquals(1, kvData.size()); // Only attribute remains
        
        kvData.clearAttributes();
        assertTrue(kvData.isEmpty());
        
        // Test clear all
        kvData.setTag("test", "value");
        kvData.setAttribute("test", "value");
        kvData.clear();
        assertTrue(kvData.isEmpty());
    }
    
    @Test
    public void testMerge() {
        KVData other = new KVData();
        other.setTag("category", "restaurant");
        other.setAttribute("rating", 5);
        
        kvData.setTag("type", "food");
        kvData.setAttribute("price", 20.0);
        
        kvData.merge(other);
        
        // Original data should remain
        assertEquals("food", kvData.getTag("type"));
        assertEquals(20.0, kvData.getAttribute("price"));
        
        // Merged data should be added
        assertEquals("restaurant", kvData.getTag("category"));
        assertEquals(5, kvData.getAttribute("rating"));
    }
    
    @Test
    public void testCopy() {
        kvData.setTag("category", "restaurant");
        kvData.setAttribute("rating", 4);
        
        KVData copy = kvData.copy();
        
        // Verify copy has same data
        assertEquals("restaurant", copy.getTag("category"));
        assertEquals(4, copy.getAttribute("rating"));
        
        // Verify it's actually a copy (modifying original doesn't affect copy)
        kvData.setTag("category", "modified");
        assertEquals("restaurant", copy.getTag("category"));
    }
}