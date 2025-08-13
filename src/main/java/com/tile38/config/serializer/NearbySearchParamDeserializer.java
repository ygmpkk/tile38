package com.tile38.config.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tile38.model.param.NearbySearchParam;
import com.tile38.model.FilterRequest;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;

/**
 * Custom Jackson deserializer for NearbySearchParam
 * Supports legacy lat/lon format and geometry objects
 */
public class NearbySearchParamDeserializer extends JsonDeserializer<NearbySearchParam> {
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public NearbySearchParam deserialize(JsonParser parser, DeserializationContext context) 
            throws IOException, JsonProcessingException {
        JsonNode node = parser.getCodec().readTree(parser);
        
        NearbySearchParam.NearbySearchParamBuilder builder = NearbySearchParam.builder();
        
        // Handle centerPoint field directly
        if (node.has("centerPoint")) {
            JsonNode centerPointNode = node.get("centerPoint");
            Point centerPoint = (Point) parseGeometry(centerPointNode);
            builder.centerPoint(centerPoint);
        }
        // Handle legacy lat/lon format
        else if (node.has("lat") && node.has("lon")) {
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();
            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
            builder.centerPoint(point);
        }
        
        // Handle radius
        if (node.has("radius")) {
            builder.radius(node.get("radius").asDouble());
        }
        
        // Handle simple filter
        if (node.has("filter")) {
            builder.filter(node.get("filter").asText());
        }
        
        // Handle complex filter request
        if (node.has("filterRequest")) {
            FilterRequest filterRequest = objectMapper.convertValue(node.get("filterRequest"), FilterRequest.class);
            builder.filterRequest(filterRequest);
        }
        // Handle legacy structure where filter fields are at the top level
        else if (node.has("conditions") || node.has("logicalOperator")) {
            FilterRequest filterRequest = objectMapper.convertValue(node, FilterRequest.class);
            builder.filterRequest(filterRequest);
        }
        
        // Handle pagination
        if (node.has("limit")) {
            builder.limit(node.get("limit").asInt());
        }
        if (node.has("offset")) {
            builder.offset(node.get("offset").asInt());
        }
        
        return builder.build();
    }
    
    private Geometry parseGeometry(JsonNode geometryNode) throws IOException {
        // Handle lat/lon object format
        if (geometryNode.has("lat") && geometryNode.has("lon")) {
            double lat = geometryNode.get("lat").asDouble();
            double lon = geometryNode.get("lon").asDouble();
            return geometryFactory.createPoint(new Coordinate(lon, lat));
        }
        
        // Handle WKT format
        if (geometryNode.has("wkt")) {
            String wkt = geometryNode.get("wkt").asText();
            try {
                return wktReader.read(wkt);
            } catch (ParseException e) {
                throw new IOException("Invalid WKT format: " + wkt, e);
            }
        }
        
        throw new IOException("Unsupported geometry format in centerPoint field");
    }
}