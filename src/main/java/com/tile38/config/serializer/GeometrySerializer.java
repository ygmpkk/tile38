package com.tile38.config.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTWriter;

import java.io.IOException;

/**
 * Custom Jackson serializer for JTS Geometry objects
 */
public class GeometrySerializer extends JsonSerializer<Geometry> {
    
    private final WKTWriter wktWriter = new WKTWriter();
    
    @Override
    public void serialize(Geometry geometry, JsonGenerator gen, SerializerProvider serializers) 
            throws IOException {
        if (geometry == null) {
            gen.writeNull();
            return;
        }
        
        // For simple points, write as lat/lon object for easier consumption
        if (geometry instanceof Point) {
            Point point = (Point) geometry;
            gen.writeStartObject();
            gen.writeNumberField("lon", point.getX());
            gen.writeNumberField("lat", point.getY());
            gen.writeStringField("type", "Point");
            gen.writeEndObject();
        } else {
            // For other geometry types, use WKT format
            gen.writeStartObject();
            gen.writeStringField("wkt", wktWriter.write(geometry));
            gen.writeStringField("type", geometry.getGeometryType());
            gen.writeEndObject();
        }
    }
}