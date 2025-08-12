package com.tile38.repository.data;

import com.tile38.repository.data.DataSource.DataSourceType;
import com.tile38.repository.data.impl.FileDataRepository;
import com.tile38.repository.data.impl.DatabaseRepository;
import com.tile38.service.Tile38Service;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the unified data repository abstraction
 */
class DataRepositoryTest {

    @Mock
    private Tile38Service tile38Service;
    
    private RepositoryFactory repositoryFactory;
    private FileDataRepository fileRepository;
    private DatabaseRepository databaseRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        fileRepository = new FileDataRepository();
        fileRepository.tile38Service = tile38Service;
        
        databaseRepository = new DatabaseRepository();
        
        repositoryFactory = new RepositoryFactory();
        repositoryFactory.repositories = List.of(fileRepository, databaseRepository);
    }

    @Test
    void testDataSourceCreation() {
        // Test file data source creation
        DataSource csvSource = DataSource.createFileSource(
            DataSourceType.FILE_CSV, "/path/to/data.csv", "test_collection");
        
        assertEquals(DataSourceType.FILE_CSV, csvSource.getType());
        assertEquals("/path/to/data.csv", csvSource.getLocation());
        assertEquals("test_collection", csvSource.getCollectionName());
        assertNull(csvSource.getQuery());

        // Test database data source creation
        DataSource mysqlSource = DataSource.createDatabaseSource(
            DataSourceType.DATABASE_MYSQL, 
            "jdbc:mysql://localhost:3306/test", 
            "geo_data", 
            "SELECT id, lat, lon FROM locations",
            null);
        
        assertEquals(DataSourceType.DATABASE_MYSQL, mysqlSource.getType());
        assertEquals("jdbc:mysql://localhost:3306/test", mysqlSource.getLocation());
        assertEquals("geo_data", mysqlSource.getCollectionName());
        assertEquals("SELECT id, lat, lon FROM locations", mysqlSource.getQuery());
    }

    @Test
    void testRepositoryFactory() {
        // Test file repository lookup
        DataRepository csvRepo = repositoryFactory.getRepository(DataSourceType.FILE_CSV);
        assertTrue(csvRepo instanceof FileDataRepository);
        assertTrue(csvRepo.supports(DataSourceType.FILE_CSV));
        
        DataRepository geoJsonRepo = repositoryFactory.getRepository(DataSourceType.FILE_GEOJSON);
        assertTrue(geoJsonRepo instanceof FileDataRepository);
        assertTrue(geoJsonRepo.supports(DataSourceType.FILE_GEOJSON));

        // Test database repository lookup
        DataRepository mysqlRepo = repositoryFactory.getRepository(DataSourceType.DATABASE_MYSQL);
        assertTrue(mysqlRepo instanceof DatabaseRepository);
        assertTrue(mysqlRepo.supports(DataSourceType.DATABASE_MYSQL));

        // Test unsupported type would be tested with actual unsupported enum
        // but since we support all types in current implementation, we'll test null handling
        // This tests the IllegalArgumentException in the factory when no repo supports null
        Exception exception = assertThrows(NullPointerException.class, 
            () -> repositoryFactory.getRepository(null));
        // NPE is expected due to enum handling
    }

    @Test
    void testSupportedTypes() {
        // File repository should support file types
        assertTrue(fileRepository.supports(DataSourceType.FILE_CSV));
        assertTrue(fileRepository.supports(DataSourceType.FILE_JSON));
        assertTrue(fileRepository.supports(DataSourceType.FILE_GEOJSON));
        assertTrue(fileRepository.supports(DataSourceType.FILE_SHP));
        assertFalse(fileRepository.supports(DataSourceType.DATABASE_MYSQL));

        // Database repository should support database types
        assertTrue(databaseRepository.supports(DataSourceType.DATABASE_MYSQL));
        assertTrue(databaseRepository.supports(DataSourceType.DATABASE_MONGODB));
        assertTrue(databaseRepository.supports(DataSourceType.DATABASE_PRESTO));
        assertFalse(databaseRepository.supports(DataSourceType.FILE_CSV));

        // Test repository factory support check
        assertTrue(repositoryFactory.isSupported(DataSourceType.FILE_CSV));
        assertTrue(repositoryFactory.isSupported(DataSourceType.DATABASE_MYSQL));
    }

    @Test
    void testUnimplementedFunctionality() {
        DataSource geoJsonSource = DataSource.createFileSource(
            DataSourceType.FILE_GEOJSON, "/path/to/data.geojson", "test");
        
        // GeoJSON loading should return not implemented message
        var result = fileRepository.loadData(geoJsonSource).join();
        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("GeoJSON loading not yet implemented"));

        DataSource mysqlSource = DataSource.createDatabaseSource(
            DataSourceType.DATABASE_MYSQL, "jdbc:mysql://localhost:3306/test", 
            "geo_data", "SELECT * FROM locations", null);

        // MySQL loading should return not implemented message
        var mysqlResult = databaseRepository.loadData(mysqlSource).join();
        assertFalse(mysqlResult.isSuccess());
        assertTrue(mysqlResult.getMessage().contains("MySQL loading not yet implemented"));

        // Connection test should return false for unimplemented
        var connectionResult = databaseRepository.testConnection(mysqlSource).join();
        assertFalse(connectionResult);
    }
}