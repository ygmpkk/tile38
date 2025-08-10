package com.tile38;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Tile38 Server - Java implementation
 * A geospatial database server with HTTP protocol support
 */
@SpringBootApplication
public class Tile38Application {
    public static void main(String[] args) {
        SpringApplication.run(Tile38Application.class, args);
    }
}