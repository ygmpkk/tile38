package com.tile38;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;

/**
 * Tile38 Server - Java implementation
 * A geospatial database server with HTTP and DUBBO protocol support
 */
@SpringBootApplication
@EnableDubbo
public class Tile38Application {
    public static void main(String[] args) {
        SpringApplication.run(Tile38Application.class, args);
    }
}