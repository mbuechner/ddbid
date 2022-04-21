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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class Database<T> {

    private final HikariConfig config;
    private final String database;
    private JdbcTemplate duckdb;
    private HikariDataSource dataSource;

    public Database(String database) {
        this.database = database;

        config = new HikariConfig();
        config.setDriverClassName("org.duckdb.DuckDBDriver");
        config.setConnectionTestQuery("SELECT 1");
        config.addDataSourceProperty("duckdb.read_only", "false");
        config.setReadOnly(false);
        config.setMaximumPoolSize(16);
        config.setMinimumIdle(8);
        config.setConnectionTimeout(600000); // 10min.
        config.setJdbcUrl("jdbc:duckdb:" + database);
    }

    public void commit() {
        if (dataSource == null || dataSource.isClosed()) {
            try {
                dataSource.getConnection().commit();
            } catch (SQLException ex) {
                log.warn("Could not commit to database. {}", ex.getMessage());
            }
        }
    }

    public void init() {
        if (dataSource == null || dataSource.isClosed()) {
            log.info("Initialize database at {}...", database);
            dataSource = new HikariDataSource(config);
            duckdb = new JdbcTemplate(dataSource);
        }
    }
    
    /**
     * Delete database and init an fresh one
     * @throws java.io.IOException
     */
    public void delete() throws IOException {
        close();
        Files.deleteIfExists(Path.of(database));
        init();
    }

    public JdbcTemplate getJdbcTemplate() {
        init();
        return duckdb;
    }

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("Database closed");
        }
    }
}
