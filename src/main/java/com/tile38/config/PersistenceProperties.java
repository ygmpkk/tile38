package com.tile38.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import lombok.Data;

/**
 * Configuration properties for Tile38 persistence
 */
@Configuration
@ConfigurationProperties(prefix = "tile38.persistence")
@Data
public class PersistenceProperties {
    
    private Rdb rdb = new Rdb();
    private Mysql mysql = new Mysql();
    
    @Data
    public static class Rdb {
        private boolean enabled = true;
        private long saveInterval = 300000; // 5 minutes in milliseconds
        private String filePath = "./data/tile38.rdb";
        private boolean loadOnStartup = true;
    }
    
    @Data
    public static class Mysql {
        private boolean enabled = false;
        private String url = "jdbc:mysql://localhost:3306/tile38";
        private String username = "tile38_user";
        private String password = "tile38_pass";
        private String tablePrefix = "tile38_";
        private int batchSize = 1000;
        private boolean realTime = true;
    }
}