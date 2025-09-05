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
 */
public class GeometryDeserializer extends JsonDeserializer<Geometry> {
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    
    @Override
    public Geometry deserialize(JsonParser p, DeserializationContext ctxt) 
            throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        
        if (node.isNull()) {
            return null;
        }
        
        if (node.has("lat") && node.has("lon")) {
            // Point format: {"lat": 33.5, "lon": -115.5, "type": "Point"}
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();
            return geometryFactory.createPoint(new Coordinate(lon, lat));
        } else if (node.has("wkt")) {
            // WKT format: {"wkt": "POINT(-115.5 33.5)", "type": "Point"}
            String wkt = node.get("wkt").asText();
            try {
                return wktReader.read(wkt);
            } catch (ParseException e) {
                throw new IOException("Failed to parse WKT: " + wkt, e);
            }
        } else {
            throw new IOException("Unknown geometry format: " + node.toString());
        }
    }
}