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
package de.ddb.labs.ddbid.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class Database<T> {
    
    private final HikariConfig config;
    private final String database;
    private JdbcTemplate duckdb;
    @Getter
    private HikariDataSource dataSource;
    @Getter
    private Connection connection;
    
    public Database(String database) {
        this.database = database;
        
        config = new HikariConfig();
        config.setDriverClassName("org.h2.Driver");
        config.setConnectionTestQuery("SELECT 1");
        config.setReadOnly(false);
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(8);
        config.setConnectionTimeout(600000); // 10min.
        config.setJdbcUrl("jdbc:h2:" + new File(database).getAbsolutePath());
        // config.setUsername("test");
        // config.setPassword("test");

    }
    
    public void init() {
        if (dataSource == null || dataSource.isClosed()) {
            log.info("Initialize database at {}...", database);
            dataSource = new HikariDataSource(config);
            try {
                connection = dataSource.getConnection();
            } catch (SQLException ex) {
                log.warn("Could not cloade DB connection. {}", ex.getMessage());
            }
            duckdb = new JdbcTemplate(dataSource);
//            duckdb.execute("SET memory_limit='1GB';");
//            duckdb.execute("SET threads TO 1;");
//            duckdb.execute("SET checkpoint_threshold='1MB';");
        }
    }
   
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Like to expose intenal rep. Only one DB connection/ template available")
    public JdbcTemplate getJdbcTemplate() {
        init();
        return duckdb;
    }
    
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            try {
                connection.close();
            } catch (SQLException ex) {
                log.warn("Could not close DB connection. {}", ex.getMessage());
            }
            log.info("Database closed");
        }
    }
}
