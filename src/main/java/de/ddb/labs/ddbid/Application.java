/* 
 * Copyright 2022 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.labs.ddbid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    @Value("${ddbid.database.table.item}")
    private String itemTableName;

    @Value("${ddbid.database.table.person}")
    private String personTableName;

    @Value("${ddbid.database.table.organization}")
    private String organizationTableName;

    @Value("${ddbid.datapath.item}")
    private String dataPathItem;

    @Value("${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value("${ddbid.datapath.organization}")
    private String dataPathOrganization;

    private final static String SET_TIMEZONE = "Set TimeZone='UTC';";

    private final static String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS main;";

    private final static String CREATE_ITEM_TABLE = "CREATE TABLE IF NOT EXISTS main.item (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(32) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "dataset_id VARCHAR(128),\n"
            + "label VARCHAR(1024),\n"
            + "provider_id VARCHAR(256),\n"
            + "supplier_id VARCHAR(128),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";
    private final static String CREATE_PERSON_TABLE = "CREATE TABLE IF NOT EXISTS main.person (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(64) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "variant_id VARCHAR(256),\n"
            + "preferredName VARCHAR(1024),\n"
            + "type VARCHAR(32),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";

    private final static String CREATE_ORGANIZATION_TABLE = "CREATE TABLE IF NOT EXISTS main.organization (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(64) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "variant_id VARCHAR(256),\n"
            + "preferredName VARCHAR(1024),\n"
            + "type VARCHAR(32),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";

    private final static String CREATE_SEARCH_INDEX_1 = "CREATE INDEX IF NOT EXISTS data_timestamp_IDX ON main.{} (\"timestamp\");";
    private final static String CREATE_SEARCH_INDEX_2 = "CREATE INDEX IF NOT EXISTS data_status_IDX ON main.{} (\"status\");";
    private final static String CREATE_SEARCH_INDEX_3 = "CREATE INDEX IF NOT EXISTS data_id_IDX ON main.{} (\"id\");";

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) throws IOException {
        final JdbcTemplate duckdb = new JdbcTemplate();
        duckdb.setDataSource(dataSource);
        duckdb.execute(SET_TIMEZONE);
        duckdb.execute(CREATE_SCHEMA);
        // item
        duckdb.execute(CREATE_ITEM_TABLE);
        duckdb.execute(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", itemTableName));
        duckdb.execute(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", itemTableName));
        duckdb.execute(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", itemTableName));
        //person
        duckdb.execute(CREATE_PERSON_TABLE);
        duckdb.execute(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", personTableName));
        duckdb.execute(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", personTableName));
        duckdb.execute(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", personTableName));
        //person
        duckdb.execute(CREATE_ORGANIZATION_TABLE);
        duckdb.execute(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", organizationTableName));
        duckdb.execute(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", organizationTableName));
        duckdb.execute(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", organizationTableName));

        // create dirs
        if (!Files.exists(Path.of(dataPathItem))) {
            Files.createDirectories(Path.of(dataPathItem));
        }
        if (!Files.exists(Path.of(dataPathPerson))) {
            Files.createDirectories(Path.of(dataPathPerson));
        }
        if (!Files.exists(Path.of(dataPathOrganization))) {
            Files.createDirectories(Path.of(dataPathOrganization));
        }

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

    @Bean
    public DataSource dataSource() {
        final HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.duckdb.DuckDBDriver");
        config.setConnectionTestQuery("SELECT 1");
        config.addDataSourceProperty("duckdb.read_only", "false");
        config.setReadOnly(false);
        config.setMaximumPoolSize(16);
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
