package com.tile38.service.mq;

import com.tile38.config.PersistenceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * RabbitMQ consumer service for real-time data ingestion into Tile38
 */
@Service
@ConditionalOnProperty(name = {"tile38.persistence.mq.enabled", "tile38.persistence.mq.type"}, 
                       havingValue = "rabbitmq", matchIfMissing = false)
public class RabbitMqConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumerService.class);
    
    @Autowired
    private MqMessageProcessor messageProcessor;
    
    @Autowired
    private PersistenceProperties persistenceProperties;
    
    /**
     * Consume messages from RabbitMQ queue
     */
    @RabbitListener(
        queues = "#{@persistenceProperties.mq.rabbitmq.queue}",
        concurrency = "#{@persistenceProperties.mq.rabbitmq.concurrency}",
        containerFactory = "rabbitListenerContainerFactory"
    )
    public void consume(
            @Payload String message,
            @Header(AmqpHeaders.CONSUMER_TAG) String consumerTag,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
        
        try {
            logger.debug("Received message from RabbitMQ: consumerTag={}, deliveryTag={}", 
                        consumerTag, deliveryTag);
            
            // Process the message
            messageProcessor.processMessage(message);
            
            logger.debug("Successfully processed RabbitMQ message: deliveryTag={}", deliveryTag);
            
        } catch (Exception e) {
            logger.error("Failed to process RabbitMQ message: consumerTag={}, deliveryTag={}, error={}", 
                        consumerTag, deliveryTag, e.getMessage(), e);
            
            // In production, you might want to:
            // 1. Send to a dead letter exchange (DLX)
            // 2. Implement retry logic with exponential backoff
            // 3. Store failed messages for later processing
            
            throw new RuntimeException("RabbitMQ message processing failed", e);
        }
    }
}
