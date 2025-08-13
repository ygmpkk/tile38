package com.tile38.config.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tile38.model.param.UpdateKVParam;
import com.tile38.model.KVData;

import java.io.IOException;

/**
 * Custom Jackson deserializer for UpdateKVParam
 * Supports legacy tags/attributes format and converts to KVData structure
 */
public class UpdateKVParamDeserializer extends JsonDeserializer<UpdateKVParam> {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public UpdateKVParam deserialize(JsonParser parser, DeserializationContext context) 
            throws IOException, JsonProcessingException {
        JsonNode node = parser.getCodec().readTree(parser);
        
        UpdateKVParam.UpdateKVParamBuilder builder = UpdateKVParam.builder();
        
        // Handle direct kvData field
        if (node.has("kvData")) {
            KVData kvData = objectMapper.convertValue(node.get("kvData"), KVData.class);
            builder.kvData(kvData);
        } else {
            // Handle legacy tags/attributes format
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
            
            if (hasKVData) {
                builder.kvData(kvData);
            }
        }
        
        return builder.build();
    }
}