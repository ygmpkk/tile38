package com.tile38.service.mq;

import com.tile38.config.PersistenceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka consumer service for real-time data ingestion into Tile38
 */
@Service
@ConditionalOnProperty(name = {"tile38.persistence.mq.enabled", "tile38.persistence.mq.type"}, 
                       havingValue = "kafka", matchIfMissing = false)
public class KafkaConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    
    @Autowired
    private MqMessageProcessor messageProcessor;
    
    @Autowired
    private PersistenceProperties persistenceProperties;
    
    /**
     * Consume messages from Kafka topic
     */
    @KafkaListener(
        topics = "#{@persistenceProperties.mq.kafka.topic}",
        groupId = "#{@persistenceProperties.mq.kafka.groupId}",
        concurrency = "#{@persistenceProperties.mq.kafka.concurrency}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        try {
            logger.debug("Received message from Kafka: topic={}, partition={}, offset={}", 
                        topic, partition, offset);
            
            // Process the message
            messageProcessor.processMessage(message);
            
            // Acknowledge the message
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            
            logger.debug("Successfully processed Kafka message: offset={}", offset);
            
        } catch (Exception e) {
            logger.error("Failed to process Kafka message: topic={}, partition={}, offset={}, error={}", 
                        topic, partition, offset, e.getMessage(), e);
            
            // In production, you might want to:
            // 1. Send to a dead letter queue (DLQ)
            // 2. Implement retry logic
            // 3. Store failed messages for later processing
            
            throw new RuntimeException("Kafka message processing failed", e);
        }
    }
}
