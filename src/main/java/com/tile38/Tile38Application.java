package com.tile38;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * Tile38 Server - Java implementation
 * A geospatial database server with HTTP protocol support
 */
@SpringBootApplication
@EnableScheduling
@EnableAsync
public class Tile38Application {
    public static void main(String[] args) {
        SpringApplication.run(Tile38Application.class, args);
    }
}