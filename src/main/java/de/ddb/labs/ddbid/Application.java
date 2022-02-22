package de.ddb.labs.ddbid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class Application {

    @Value("${ddbid.database}")
    private String database;

    private HikariDataSource dataSource;

    private final static String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS main;";

    private final static String CREATE_DB = "CREATE TABLE IF NOT EXISTS main.\"data\" (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(32) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "dataset_id VARCHAR(128),\n"
            + "label VARCHAR(1024),\n"
            + "provider_id VARCHAR(256),\n"
            + "supplier_id VARCHAR(128),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";
    private final static String CREATE_SEARCH_INDEX_1 = "CREATE INDEX IF NOT EXISTS data_timestamp_IDX ON main.\"data\" (\"timestamp\");";
    private final static String CREATE_SEARCH_INDEX_2 = "CREATE INDEX IF NOT EXISTS data_status_IDX ON main.\"data\" (\"status\");";
    private final static String CREATE_SEARCH_INDEX_3 = "CREATE INDEX IF NOT EXISTS data_id_IDX ON main.\"data\" (\"id\");";

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        final JdbcTemplate duckdb = new JdbcTemplate();
        duckdb.setDataSource(dataSource);
        duckdb.execute(CREATE_SCHEMA);
        duckdb.execute(CREATE_DB);
        duckdb.execute(CREATE_SEARCH_INDEX_1);
        duckdb.execute(CREATE_SEARCH_INDEX_2);
        duckdb.execute(CREATE_SEARCH_INDEX_3);
        return duckdb;

    }

    @PreDestroy
    public void destroy() {
        log.info("Destroy callback triggered: Closing database...");
        try {
            dataSource.close();
        } catch (Exception e) {
            log.error("Could not close connection to database. {}", e.getMessage());
        }
    }

    /**
     *
     * @return
     */
    @Bean
    public DataSource dataSource() {
        final HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.duckdb.DuckDBDriver");
        config.setMaximumPoolSize(10);
        //config.setMaxLifetime(3);
        config.setJdbcUrl("jdbc:duckdb:" + database);
        dataSource = new HikariDataSource(config);
        return dataSource;
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public OkHttpClient httpClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(0, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS)
                .build();
    }

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        SpringApplication.run(Application.class, args);
    }
}
