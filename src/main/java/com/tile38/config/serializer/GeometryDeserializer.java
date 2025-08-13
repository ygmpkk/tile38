package com.tile38.config.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;

/**
 * Custom Jackson deserializer for JTS Geometry objects
 * Supports both lat/lon format and WKT format
 */
public class GeometryDeserializer extends JsonDeserializer<Geometry> {
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    
    @Override
    public Geometry deserialize(JsonParser parser, DeserializationContext context) 
            throws IOException, JsonProcessingException {
        JsonNode node = parser.getCodec().readTree(parser);
        
        if (node.isNull()) {
            return null;
        }
        
        // Handle lat/lon object format
        if (node.has("lat") && node.has("lon")) {
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();
            return geometryFactory.createPoint(new Coordinate(lon, lat));
        }
        
        // Handle WKT format
        if (node.has("wkt")) {
            String wkt = node.get("wkt").asText();
            try {
                return wktReader.read(wkt);
            } catch (ParseException e) {
                throw new IOException("Invalid WKT format: " + wkt, e);
            }
        }
        
        throw new IOException("Unsupported geometry format. Expected lat/lon or WKT format.");
    }
}