package com.tile38.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * Timing aspect that automatically measures execution time for annotated methods
 * Replaces manual timing code throughout the application
 */
@Aspect
@Component
@Slf4j
public class TimingAspect {

    // ThreadLocal to store timing information for the current thread
    private static final ThreadLocal<Long> EXECUTION_TIME = new ThreadLocal<>();

    @Around("@annotation(timed)")
    public Object measureExecutionTime(ProceedingJoinPoint joinPoint, Timed timed) throws Throwable {
        long startTime = System.currentTimeMillis();
        
        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - startTime;
            
            // Store duration in ThreadLocal for controller use
            EXECUTION_TIME.set(duration);
            
            String methodName = joinPoint.getSignature().getName();
            String className = joinPoint.getSignature().getDeclaringType().getSimpleName();
            
            if (timed.logLevel() == Timed.LogLevel.DEBUG) {
                log.debug("{}#{} executed in {}ms", className, methodName, duration);
            } else if (timed.logLevel() == Timed.LogLevel.INFO) {
                log.info("{}#{} executed in {}ms", className, methodName, duration);
            }
            
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            EXECUTION_TIME.set(duration);
            log.error("{}#{} failed after {}ms", 
                joinPoint.getSignature().getDeclaringType().getSimpleName(),
                joinPoint.getSignature().getName(), 
                duration, e);
            throw e;
        }
    }
    
    /**
     * Get the execution time for the current thread and clear it
     */
    public static String getAndClearExecutionTime() {
        Long duration = EXECUTION_TIME.get();
        EXECUTION_TIME.remove(); // Clean up ThreadLocal
        return duration != null ? duration + "ms" : "0ms";
    }
}