package com.tile38.service.mq;

import com.tile38.model.MqMessage;
import com.tile38.model.Tile38Object;
import com.tile38.service.Tile38Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Service to parse and process MQ messages into Tile38 operations
 */
@Service
public class MqMessageProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(MqMessageProcessor.class);
    
    @Autowired
    private Tile38Service tile38Service;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKTReader wktReader = new WKTReader(geometryFactory);
    
    /**
     * Process a message from MQ
     */
    public void processMessage(String messageJson) {
        try {
            MqMessage message = objectMapper.readValue(messageJson, MqMessage.class);
            processMessage(message);
        } catch (Exception e) {
            logger.error("Failed to parse MQ message: {}", e.getMessage(), e);
            throw new RuntimeException("Invalid message format", e);
        }
    }
    
    /**
     * Process a parsed MQ message
     */
    public void processMessage(MqMessage message) {
        try {
            String operation = message.getOperation();
            if (operation == null) {
                logger.warn("Message missing operation type");
                return;
            }
            
            switch (operation.toUpperCase()) {
                case "SET" -> processSetOperation(message);
                case "DEL" -> processDelOperation(message);
                case "DROP" -> processDropOperation(message);
                default -> logger.warn("Unknown operation type: {}", operation);
            }
            
        } catch (Exception e) {
            logger.error("Failed to process MQ message: {}", e.getMessage(), e);
            throw new RuntimeException("Message processing failed", e);
        }
    }
    
    /**
     * Process SET operation
     */
    private void processSetOperation(MqMessage message) throws Exception {
        if (message.getCollection() == null || message.getId() == null) {
            throw new IllegalArgumentException("SET operation requires collection and id");
        }
        
        if (message.getGeometry() == null) {
            throw new IllegalArgumentException("SET operation requires geometry data");
        }
        
        Geometry geometry = parseGeometry(message.getGeometry());
        
        Tile38Object tile38Object = new Tile38Object();
        tile38Object.setGeometry(geometry);
        tile38Object.setFields(message.getFields());
        tile38Object.setTimestamp(System.currentTimeMillis());
        
        if (message.getExpireAt() != null) {
            tile38Object.setExpireAt(Instant.ofEpochMilli(message.getExpireAt()));
        }
        
        tile38Service.set(message.getCollection(), message.getId(), tile38Object);
        
        logger.debug("Processed SET: collection={}, id={}, type={}", 
                    message.getCollection(), message.getId(), geometry.getGeometryType());
    }
    
    /**
     * Process DEL operation
     */
    private void processDelOperation(MqMessage message) {
        if (message.getCollection() == null || message.getId() == null) {
            throw new IllegalArgumentException("DEL operation requires collection and id");
        }
        
        boolean deleted = tile38Service.del(message.getCollection(), message.getId());
        
        logger.debug("Processed DEL: collection={}, id={}, deleted={}", 
                    message.getCollection(), message.getId(), deleted);
    }
    
    /**
     * Process DROP operation
     */
    private void processDropOperation(MqMessage message) {
        if (message.getCollection() == null) {
            throw new IllegalArgumentException("DROP operation requires collection");
        }
        
        boolean dropped = tile38Service.drop(message.getCollection());
        
        logger.debug("Processed DROP: collection={}, dropped={}", 
                    message.getCollection(), dropped);
    }
    
    /**
     * Parse geometry from message data
     */
    private Geometry parseGeometry(MqMessage.GeometryData geometryData) throws Exception {
        // If WKT is provided, use it directly
        if (geometryData.getWkt() != null && !geometryData.getWkt().isEmpty()) {
            return wktReader.read(geometryData.getWkt());
        }
        
        // Otherwise, parse from type and coordinates
        String type = geometryData.getType();
        Object coordinates = geometryData.getCoordinates();
        
        if (type == null || coordinates == null) {
            throw new IllegalArgumentException("Geometry requires either WKT or type+coordinates");
        }
        
        return switch (type.toUpperCase()) {
            case "POINT" -> parsePoint(coordinates);
            case "LINESTRING" -> parseLineString(coordinates);
            case "POLYGON" -> parsePolygon(coordinates);
            case "MULTIPOINT" -> parseMultiPoint(coordinates);
            case "MULTILINESTRING" -> parseMultiLineString(coordinates);
            case "MULTIPOLYGON" -> parseMultiPolygon(coordinates);
            default -> throw new IllegalArgumentException("Unsupported geometry type: " + type);
        };
    }
    
    /**
     * Parse Point geometry
     */
    private Point parsePoint(Object coordinates) {
        if (!(coordinates instanceof List)) {
            throw new IllegalArgumentException("Point coordinates must be a List");
        }
        List<?> coords = (List<?>) coordinates;
        if (coords.size() < 2) {
            throw new IllegalArgumentException("Point requires at least 2 coordinates");
        }
        if (!(coords.get(0) instanceof Number) || !(coords.get(1) instanceof Number)) {
            throw new IllegalArgumentException("Point coordinates must be numbers");
        }
        double lon = ((Number) coords.get(0)).doubleValue();
        double lat = ((Number) coords.get(1)).doubleValue();
        return geometryFactory.createPoint(new Coordinate(lon, lat));
    }
    
    /**
     * Parse LineString geometry
     */
    private LineString parseLineString(Object coordinates) {
        if (!(coordinates instanceof List)) {
            throw new IllegalArgumentException("LineString coordinates must be a List");
        }
        List<?> coordsList = (List<?>) coordinates;
        if (coordsList.isEmpty()) {
            throw new IllegalArgumentException("LineString must have at least one point");
        }
        
        Coordinate[] coords = new Coordinate[coordsList.size()];
        
        for (int i = 0; i < coordsList.size(); i++) {
            Object pointObj = coordsList.get(i);
            if (!(pointObj instanceof List)) {
                throw new IllegalArgumentException("LineString point must be a List");
            }
            List<?> point = (List<?>) pointObj;
            if (point.size() < 2) {
                throw new IllegalArgumentException("LineString point requires at least 2 coordinates");
            }
            if (!(point.get(0) instanceof Number) || !(point.get(1) instanceof Number)) {
                throw new IllegalArgumentException("LineString coordinates must be numbers");
            }
            double lon = ((Number) point.get(0)).doubleValue();
            double lat = ((Number) point.get(1)).doubleValue();
            coords[i] = new Coordinate(lon, lat);
        }
        
        return geometryFactory.createLineString(coords);
    }
    
    /**
     * Parse Polygon geometry
     */
    private Polygon parsePolygon(Object coordinates) {
        if (!(coordinates instanceof List)) {
            throw new IllegalArgumentException("Polygon coordinates must be a List");
        }
        List<?> rings = (List<?>) coordinates;
        if (rings.isEmpty()) {
            throw new IllegalArgumentException("Polygon must have at least one ring");
        }
        
        // Exterior ring
        Object exteriorObj = rings.get(0);
        if (!(exteriorObj instanceof List)) {
            throw new IllegalArgumentException("Polygon ring must be a List");
        }
        List<?> exteriorList = (List<?>) exteriorObj;
        Coordinate[] exteriorCoords = new Coordinate[exteriorList.size()];
        for (int i = 0; i < exteriorList.size(); i++) {
            Object pointObj = exteriorList.get(i);
            if (!(pointObj instanceof List)) {
                throw new IllegalArgumentException("Polygon point must be a List");
            }
            List<?> point = (List<?>) pointObj;
            if (point.size() < 2) {
                throw new IllegalArgumentException("Polygon point requires at least 2 coordinates");
            }
            if (!(point.get(0) instanceof Number) || !(point.get(1) instanceof Number)) {
                throw new IllegalArgumentException("Polygon coordinates must be numbers");
            }
            double lon = ((Number) point.get(0)).doubleValue();
            double lat = ((Number) point.get(1)).doubleValue();
            exteriorCoords[i] = new Coordinate(lon, lat);
        }
        LinearRing exteriorRing = geometryFactory.createLinearRing(exteriorCoords);
        
        // Interior rings (holes)
        LinearRing[] holes = null;
        if (rings.size() > 1) {
            holes = new LinearRing[rings.size() - 1];
            for (int i = 1; i < rings.size(); i++) {
                Object holeObj = rings.get(i);
                if (!(holeObj instanceof List)) {
                    throw new IllegalArgumentException("Polygon hole must be a List");
                }
                List<?> holeList = (List<?>) holeObj;
                Coordinate[] holeCoords = new Coordinate[holeList.size()];
                for (int j = 0; j < holeList.size(); j++) {
                    Object pointObj = holeList.get(j);
                    if (!(pointObj instanceof List)) {
                        throw new IllegalArgumentException("Polygon point must be a List");
                    }
                    List<?> point = (List<?>) pointObj;
                    if (point.size() < 2) {
                        throw new IllegalArgumentException("Polygon point requires at least 2 coordinates");
                    }
                    if (!(point.get(0) instanceof Number) || !(point.get(1) instanceof Number)) {
                        throw new IllegalArgumentException("Polygon coordinates must be numbers");
                    }
                    double lon = ((Number) point.get(0)).doubleValue();
                    double lat = ((Number) point.get(1)).doubleValue();
                    holeCoords[j] = new Coordinate(lon, lat);
                }
                holes[i - 1] = geometryFactory.createLinearRing(holeCoords);
            }
        }
        
        return geometryFactory.createPolygon(exteriorRing, holes);
    }
    
    /**
     * Parse MultiPoint geometry
     */
    private MultiPoint parseMultiPoint(Object coordinates) {
        List<?> pointsList = (List<?>) coordinates;
        Point[] points = new Point[pointsList.size()];
        
        for (int i = 0; i < pointsList.size(); i++) {
            points[i] = parsePoint(pointsList.get(i));
        }
        
        return geometryFactory.createMultiPoint(points);
    }
    
    /**
     * Parse MultiLineString geometry
     */
    private MultiLineString parseMultiLineString(Object coordinates) {
        List<?> linesList = (List<?>) coordinates;
        LineString[] lines = new LineString[linesList.size()];
        
        for (int i = 0; i < linesList.size(); i++) {
            lines[i] = parseLineString(linesList.get(i));
        }
        
        return geometryFactory.createMultiLineString(lines);
    }
    
    /**
     * Parse MultiPolygon geometry
     */
    private MultiPolygon parseMultiPolygon(Object coordinates) {
        List<?> polygonsList = (List<?>) coordinates;
        Polygon[] polygons = new Polygon[polygonsList.size()];
        
        for (int i = 0; i < polygonsList.size(); i++) {
            polygons[i] = parsePolygon(polygonsList.get(i));
        }
        
        return geometryFactory.createMultiPolygon(polygons);
    }
}
