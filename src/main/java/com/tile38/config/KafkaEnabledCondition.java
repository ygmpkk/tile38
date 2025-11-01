package com.tile38.config;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Custom condition that checks if MQ is enabled and type is Kafka
 */
public class KafkaEnabledCondition extends SpringBootCondition {
    
    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String enabled = context.getEnvironment().getProperty("tile38.persistence.mq.enabled");
        String type = context.getEnvironment().getProperty("tile38.persistence.mq.type");
        
        if ("true".equalsIgnoreCase(enabled) && "kafka".equalsIgnoreCase(type)) {
            return ConditionOutcome.match("Kafka MQ is enabled");
        }
        
        return ConditionOutcome.noMatch("Kafka MQ is not enabled");
    }
}
