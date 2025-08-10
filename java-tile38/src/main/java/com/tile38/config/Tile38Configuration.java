package com.tile38.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Application configuration for Tile38
 */
@Configuration
public class Tile38Configuration {
    
    @Bean
    public GeometryFactory geometryFactory() {
        return new GeometryFactory();
    }
    
    @Bean
    public WKTReader wktReader(GeometryFactory geometryFactory) {
        return new WKTReader(geometryFactory);
    }
    
    @Bean
    public WKTWriter wktWriter() {
        return new WKTWriter();
    }
    
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}