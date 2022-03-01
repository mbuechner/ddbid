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
import de.ddb.labs.ddbid.database.Database;
import java.io.IOException;
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

    private final static String SET_TIMEZONE = "Set TimeZone='UTC';";

    @Value("${ddbid.database}")
    private String databaseName;

    private HikariDataSource dataSource;
    
    private Database database; // for write access

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) throws IOException {
        final JdbcTemplate duckdb = new JdbcTemplate();
        duckdb.setDataSource(dataSource);
        duckdb.execute(SET_TIMEZONE);
        return duckdb;
    }

    @PreDestroy
    public void destroy() {
        log.info("Destroy callback triggered: Closing database...");
        try {
            dataSource.close();
            database.close();
        } catch (Exception e) {
            log.error("Could not close connection to database. {}", e.getMessage());
        }
    }
    
    @Bean
    public Database database() {
        database = new Database(databaseName);
        return database;
    }

    @Bean
    public DataSource dataSource() {
        final HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.duckdb.DuckDBDriver");
        config.setConnectionTestQuery("SELECT 1");
        config.addDataSourceProperty("duckdb.read_only", "true");
        config.setReadOnly(true);
        config.setMaximumPoolSize(16);
        //config.setMaxLifetime(3);
        config.setJdbcUrl("jdbc:duckdb:" + databaseName);

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
