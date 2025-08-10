package com.tile38.enterprise;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Tile38 Enterprise Integration Application
 * 
 * This Spring Boot application provides enterprise-grade integration capabilities
 * for the Tile38 geospatial database, including:
 * - Advanced geospatial query APIs
 * - Enterprise security and authentication
 * - Monitoring and metrics collection
 * - Caching and performance optimization
 * - ZGC garbage collection optimization
 */
@SpringBootApplication
@EnableCaching
@EnableAsync
@EnableScheduling
public class Tile38EnterpriseApplication {
    
    public static void main(String[] args) {
        // Configure ZGC and JVM options for optimal geospatial performance
        System.setProperty("java.awt.headless", "true");
        System.setProperty("file.encoding", "UTF-8");
        
        // Enable ZGC if not already enabled via JVM args
        if (!isZGCEnabled()) {
            System.err.println("Warning: ZGC is not enabled. For optimal performance, start with:");
            System.err.println("-XX:+UnlockExperimentalVMOptions -XX:+UseZGC");
        }
        
        SpringApplication application = new SpringApplication(Tile38EnterpriseApplication.class);
        application.run(args);
    }
    
    private static boolean isZGCEnabled() {
        return "ZGC".equals(System.getProperty("java.vm.name")) || 
               System.getProperty("java.vm.version", "").contains("ZGC");
    }
}