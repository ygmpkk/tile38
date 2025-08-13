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
import com.tile38.config.serializer.GeometryDeserializer;
import com.tile38.config.serializer.SetObjectParamDeserializer;
import com.tile38.config.serializer.UpdateKVParamDeserializer;
import com.tile38.config.serializer.NearbySearchParamDeserializer;
import com.tile38.model.param.SetObjectParam;
import com.tile38.model.param.UpdateKVParam;
import com.tile38.model.param.NearbySearchParam;

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
        
        // Register custom serializer and deserializer for Geometry objects
        SimpleModule geometryModule = new SimpleModule();
        geometryModule.addSerializer(Geometry.class, new GeometrySerializer());
        geometryModule.addDeserializer(Geometry.class, new GeometryDeserializer());
        geometryModule.addDeserializer(SetObjectParam.class, new SetObjectParamDeserializer());
        geometryModule.addDeserializer(UpdateKVParam.class, new UpdateKVParamDeserializer());
        geometryModule.addDeserializer(NearbySearchParam.class, new NearbySearchParamDeserializer());
        mapper.registerModule(geometryModule);
        
        return mapper;
    }
}