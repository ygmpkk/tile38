package com.tile38.enterprise.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Configuration for connecting to Tile38 server.
 * Tile38 uses Redis-compatible protocol, so we can use Redis clients.
 */
@Configuration
@ConfigurationProperties(prefix = "tile38")
public class Tile38Configuration {
    
    @Value("${tile38.host:localhost}")
    private String host;
    
    @Value("${tile38.port:9851}")
    private int port;
    
    @Value("${tile38.password:}")
    private String password;
    
    @Value("${tile38.database:0}")
    private int database;
    
    @Value("${tile38.timeout:5000}")
    private int timeout;
    
    @Value("${tile38.pool.max-total:50}")
    private int poolMaxTotal;
    
    @Value("${tile38.pool.max-idle:20}")
    private int poolMaxIdle;
    
    @Value("${tile38.pool.min-idle:5}")
    private int poolMinIdle;
    
    @Bean
    public JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(poolMaxTotal);
        config.setMaxIdle(poolMaxIdle);
        config.setMinIdle(poolMinIdle);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setBlockWhenExhausted(true);
        return config;
    }
    
    @Bean
    public RedisConnectionFactory tile38ConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(host);
        config.setPort(port);
        config.setDatabase(database);
        
        if (password != null && !password.trim().isEmpty()) {
            config.setPassword(password);
        }
        
        JedisConnectionFactory factory = new JedisConnectionFactory(config);
        factory.setPoolConfig(jedisPoolConfig());
        factory.setTimeout(timeout);
        return factory;
    }
    
    @Bean
    public RedisTemplate<String, Object> tile38Template() {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(tile38ConnectionFactory());
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }
    
    // Getters and setters
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    
    public int getDatabase() { return database; }
    public void setDatabase(int database) { this.database = database; }
    
    public int getTimeout() { return timeout; }
    public void setTimeout(int timeout) { this.timeout = timeout; }
    
    public int getPoolMaxTotal() { return poolMaxTotal; }
    public void setPoolMaxTotal(int poolMaxTotal) { this.poolMaxTotal = poolMaxTotal; }
    
    public int getPoolMaxIdle() { return poolMaxIdle; }
    public void setPoolMaxIdle(int poolMaxIdle) { this.poolMaxIdle = poolMaxIdle; }
    
    public int getPoolMinIdle() { return poolMinIdle; }
    public void setPoolMinIdle(int poolMinIdle) { this.poolMinIdle = poolMinIdle; }
}