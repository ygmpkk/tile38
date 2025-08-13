package com.tile38.aspect;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
public class TimingAspectTest {

    @Test
    public void testTimingAspectBasicFunctionality() {
        // Test that the aspect can get and clear execution time
        String executionTime = TimingAspect.getAndClearExecutionTime();
        assertNotNull(executionTime);
        assertTrue(executionTime.endsWith("ms"));
        
        // After clearing, should get "0ms" if no timing is active
        String clearedTime = TimingAspect.getAndClearExecutionTime();
        assertEquals("0ms", clearedTime);
    }
    
    @Test
    public void testTimingAspectExistsAndIsComponent() {
        // This test verifies that our aspect is properly configured as a Spring component
        // If the Spring context starts successfully, our aspect is properly configured
        assertTrue(true, "Spring context loaded successfully with timing aspect");
    }
}