package com.tile38.config.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tile38.model.param.SetObjectParam;
import com.tile38.model.KVData;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;
import java.util.Map;

/**
 * Custom Jackson deserializer for SetObjectParam
 * Supports legacy lat/lon format and converts to polygon-centric structure
 */
public class SetObjectParamDeserializer extends JsonDeserializer<SetObjectParam> {
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public SetObjectParam deserialize(JsonParser parser, DeserializationContext context) 
            throws IOException, JsonProcessingException {
        JsonNode node = parser.getCodec().readTree(parser);
        
        SetObjectParam.SetObjectParamBuilder builder = SetObjectParam.builder();
        
        // Handle geometry field directly
        if (node.has("geometry")) {
            JsonNode geometryNode = node.get("geometry");
            Geometry geometry = parseGeometry(geometryNode);
            builder.geometry(geometry);
        }
        // Handle legacy lat/lon format
        else if (node.has("lat") && node.has("lon")) {
            double lat = node.get("lat").asDouble();
            double lon = node.get("lon").asDouble();
            Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
            builder.geometry(point);
        }
        
        // Handle fields
        if (node.has("fields")) {
            Map<String, Object> fields = objectMapper.convertValue(node.get("fields"), Map.class);
            builder.fields(fields);
        }
        
        // Handle KV data
        KVData kvData = new KVData();
        boolean hasKVData = false;
        
        if (node.has("tags")) {
            JsonNode tagsNode = node.get("tags");
            if (tagsNode.isObject()) {
                tagsNode.fields().forEachRemaining(entry -> {
                    kvData.setTag(entry.getKey(), entry.getValue().asText());
                });
                hasKVData = true;
            }
        }
        
        if (node.has("attributes")) {
            JsonNode attributesNode = node.get("attributes");
            if (attributesNode.isObject()) {
                attributesNode.fields().forEachRemaining(entry -> {
                    JsonNode valueNode = entry.getValue();
                    Object value;
                    if (valueNode.isNumber()) {
                        value = valueNode.numberValue();
                    } else if (valueNode.isBoolean()) {
                        value = valueNode.booleanValue();
                    } else {
                        value = valueNode.asText();
                    }
                    kvData.setAttribute(entry.getKey(), value);
                });
                hasKVData = true;
            }
        }
        
        if (node.has("kvData")) {
            KVData existingKvData = objectMapper.convertValue(node.get("kvData"), KVData.class);
            if (existingKvData != null) {
                builder.kvData(existingKvData);
            }
        } else if (hasKVData) {
            builder.kvData(kvData);
        }
        
        // Handle expiration
        if (node.has("ex")) {
            builder.ex(node.get("ex").asLong());
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
        
        throw new IOException("Unsupported geometry format in geometry field");
    }
}