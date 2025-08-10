package com.tile38.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tile38.config.serializer.GeometrySerializer;

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
        
        // Register custom serializer for Geometry objects
        SimpleModule geometryModule = new SimpleModule();
        geometryModule.addSerializer(Geometry.class, new GeometrySerializer());
        mapper.registerModule(geometryModule);
        
        return mapper;
    }
}