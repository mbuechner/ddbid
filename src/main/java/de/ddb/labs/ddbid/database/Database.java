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
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class Database<T> {

    private final static String SET_TIMEZONE = "Set TimeZone='UTC';";
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
        //config.setMaxLifetime(3);
        config.setJdbcUrl("jdbc:duckdb:" + database);
    }

    public void init() {
        if (dataSource == null || dataSource.isClosed()) {
            log.info("Initialize database at {}...", database);
            dataSource = new HikariDataSource(config);
            duckdb = new JdbcTemplate(dataSource);
            duckdb.execute(SET_TIMEZONE);
        }
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
