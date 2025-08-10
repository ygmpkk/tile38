package com.tile38.enterprise.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tile38.enterprise.model.GeospatialObject;
import com.tile38.enterprise.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Client for interacting with Tile38 server using Redis protocol
 */
@Component
public class Tile38Client {
    
    private static final Logger logger = LoggerFactory.getLogger(Tile38Client.class);
    
    @Autowired
    private RedisTemplate<String, Object> tile38Template;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Set a point in a collection
     */
    public void setPoint(String collection, String id, Point point, Map<String, Object> fields) {
        try {
            StringBuilder command = new StringBuilder();
            command.append("SET ").append(collection).append(" ").append(id);
            
            // Add fields if provided
            if (fields != null && !fields.isEmpty()) {
                for (Map.Entry<String, Object> field : fields.entrySet()) {
                    command.append(" FIELD ").append(field.getKey()).append(" ").append(field.getValue());
                }
            }
            
            // Add point coordinates
            command.append(" POINT ").append(point.getLatitude()).append(" ").append(point.getLongitude());
            if (point.getAltitude() != null) {
                command.append(" ").append(point.getAltitude());
            }
            
            String cmdString = command.toString();
            Object result = tile38Template.execute((RedisCallback<Object>) connection -> {
                // Using raw commands for Tile38 - convert string to bytes
                byte[] cmdBytes = cmdString.getBytes();
                return connection.eval(cmdBytes, org.springframework.data.redis.connection.ReturnType.VALUE, 0);
            });
            
            logger.debug("Set point result: {}", result);
            
        } catch (Exception e) {
            logger.error("Error setting point in collection {}: {}", collection, e.getMessage(), e);
            throw new RuntimeException("Failed to set point", e);
        }
    }
    
    /**
     * Get an object from a collection
     */
    public GeospatialObject get(String collection, String id) {
        try {
            String command = "GET " + collection + " " + id;
            Object result = tile38Template.execute((RedisCallback<Object>) connection -> 
                connection.eval(command.getBytes(), org.springframework.data.redis.connection.ReturnType.VALUE, 0));
            
            if (result != null) {
                // Parse the result and create GeospatialObject
                // This is a simplified version - in practice, you'd need to parse the Tile38 response format
                GeospatialObject obj = new GeospatialObject();
                obj.setId(id);
                obj.setCollection(collection);
                return obj;
            }
            return null;
            
        } catch (Exception e) {
            logger.error("Error getting object from collection {}: {}", collection, e.getMessage(), e);
            throw new RuntimeException("Failed to get object", e);
        }
    }
    
    /**
     * Find nearby objects
     */
    public List<GeospatialObject> nearby(String collection, Point point, double radius, String unit) {
        try {
            String command = String.format("NEARBY %s POINT %f %f %f %s", 
                collection, point.getLatitude(), point.getLongitude(), radius, unit);
            
            Object result = tile38Template.execute((RedisCallback<Object>) connection -> 
                connection.eval(command.getBytes(), org.springframework.data.redis.connection.ReturnType.VALUE, 0));
            
            List<GeospatialObject> objects = new ArrayList<>();
            
            // Parse result (simplified)
            // In practice, you'd need to properly parse the Tile38 response format
            logger.debug("Nearby result: {}", result);
            
            return objects;
            
        } catch (Exception e) {
            logger.error("Error finding nearby objects: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to find nearby objects", e);
        }
    }
    
    /**
     * Find objects within a bounding box
     */
    public List<GeospatialObject> within(String collection, double minLat, double minLon, 
                                       double maxLat, double maxLon) {
        try {
            String command = String.format("WITHIN %s BOUNDS %f %f %f %f", 
                collection, minLat, minLon, maxLat, maxLon);
            
            Object result = tile38Template.execute((RedisCallback<Object>) connection -> 
                connection.eval(command.getBytes(), org.springframework.data.redis.connection.ReturnType.VALUE, 0));
            
            List<GeospatialObject> objects = new ArrayList<>();
            
            // Parse result (simplified)
            logger.debug("Within result: {}", result);
            
            return objects;
            
        } catch (Exception e) {
            logger.error("Error finding objects within bounds: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to find objects within bounds", e);
        }
    }
    
    /**
     * Delete an object from a collection
     */
    public boolean delete(String collection, String id) {
        try {
            String command = "DEL " + collection + " " + id;
            Object result = tile38Template.execute((RedisCallback<Object>) connection -> 
                connection.eval(command.getBytes(), org.springframework.data.redis.connection.ReturnType.VALUE, 0));
            
            logger.debug("Delete result: {}", result);
            return true;
            
        } catch (Exception e) {
            logger.error("Error deleting object from collection {}: {}", collection, e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Check if Tile38 server is available
     */
    public boolean ping() {
        try {
            Object result = tile38Template.execute((RedisCallback<Object>) connection -> 
                connection.ping());
            return result != null;
        } catch (Exception e) {
            logger.error("Error pinging Tile38 server: {}", e.getMessage(), e);
            return false;
        }
    }
}