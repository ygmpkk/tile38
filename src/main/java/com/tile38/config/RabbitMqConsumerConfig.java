package com.tile38.config;

import com.tile38.config.PersistenceProperties;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ configuration for consuming messages
 */
@Configuration
@EnableRabbit
@ConditionalOnProperty(name = {"tile38.persistence.mq.enabled", "tile38.persistence.mq.type"}, 
                       havingValue = "rabbitmq", matchIfMissing = false)
public class RabbitMqConsumerConfig {
    
    @Autowired
    private PersistenceProperties persistenceProperties;
    
    /**
     * Connection factory configuration
     */
    @Bean
    public ConnectionFactory connectionFactory() {
        PersistenceProperties.Mq.RabbitMq rabbitConfig = persistenceProperties.getMq().getRabbitmq();
        
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setHost(rabbitConfig.getHost());
        factory.setPort(rabbitConfig.getPort());
        factory.setUsername(rabbitConfig.getUsername());
        factory.setPassword(rabbitConfig.getPassword());
        
        return factory;
    }
    
    /**
     * Queue configuration
     */
    @Bean
    public Queue queue() {
        String queueName = persistenceProperties.getMq().getRabbitmq().getQueue();
        return new Queue(queueName, true); // durable queue
    }
    
    /**
     * Message converter for JSON
     */
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }
    
    /**
     * RabbitMQ template
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }
    
    /**
     * Listener container factory
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        factory.setPrefetchCount(10);
        
        // Error handling
        factory.setDefaultRequeueRejected(false);
        
        return factory;
    }
}
