package com.tile38.aspect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods for automatic timing measurement
 * Replaces manual timing code in service and controller layers
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Timed {
    
    /**
     * Description of the operation being timed
     */
    String value() default "";
    
    /**
     * Log level for timing output
     */
    LogLevel logLevel() default LogLevel.DEBUG;
    
    enum LogLevel {
        DEBUG, INFO, WARN
    }
}