/*
 * Copyright 2022-2026 Michael Büchner, Deutsche Digitale Bibliothek
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
import java.util.Locale;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
public class Database {
    
    private final String databaseType;
    private final String database;
    private final String databaseUrl;
    private final String databaseUser;
    private final String databasePassword;
    private final int queryTimeoutSeconds;
    private HikariConfig config;
    private JdbcTemplate jdbcTemplate;
    @Getter
    private HikariDataSource dataSource;
    
    public Database(String databaseType, String database, String databaseUrl, String databaseUser, String databasePassword, int queryTimeoutSeconds) {
        this.databaseType = databaseType;
        this.database = database;
        this.databaseUrl = databaseUrl;
        this.databaseUser = databaseUser;
        this.databasePassword = databasePassword;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }
    
    public void init() {
        if (dataSource == null || dataSource.isClosed()) {
            final DatabaseType type = DatabaseType.from(databaseType);
            config = createConfig();
            log.info("Initialize {} database at {}...", type.getType(), config.getJdbcUrl());
            dataSource = new HikariDataSource(config);
            jdbcTemplate = new JdbcTemplate(dataSource);
            if (queryTimeoutSeconds > 0) {
                jdbcTemplate.setQueryTimeout(queryTimeoutSeconds);
            }
        }
    }

    private HikariConfig createConfig() {
        final DatabaseType type = DatabaseType.from(databaseType);
        final HikariConfig hikariConfig = new HikariConfig();
        configureDefaults(hikariConfig);
        switch (type) {
            case H2 -> configureH2(hikariConfig);
            case POSTGRES -> configurePostgres(hikariConfig);
            default -> throw new IllegalStateException("Unsupported database type: " + type);
        }
        return hikariConfig;
    }

    private void configureDefaults(HikariConfig hikariConfig) {
        final int connectionTimeout = 600000; // 10min.
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setReadOnly(false);
        hikariConfig.setMaximumPoolSize(16);
        hikariConfig.setMinimumIdle(8);
        hikariConfig.setConnectionTimeout(connectionTimeout);
    }

    private void configureH2(HikariConfig hikariConfig) {
        final String databaseName = (database == null || database.isBlank()) ? "data/ddbid_duckdb_DO_NOT_DELETE_ITS_IMPORTANT.db" : database;
        hikariConfig.setDriverClassName("org.h2.Driver");
        if (databaseName.startsWith("jdbc:h2:")) {
            hikariConfig.setJdbcUrl(addH2LockTimeout(databaseName));
        } else {
            hikariConfig.setJdbcUrl(addH2LockTimeout("jdbc:h2:" + new File(databaseName).getAbsolutePath()));
        }
    }

    private static String addH2LockTimeout(String jdbcUrl) {
        if (jdbcUrl.toUpperCase(Locale.ROOT).contains("LOCK_TIMEOUT=")) {
            return jdbcUrl;
        }
        return jdbcUrl + ";LOCK_TIMEOUT=360000";
    }

    private void configurePostgres(HikariConfig hikariConfig) {
        final String jdbcUrl;
        if (databaseUrl != null && !databaseUrl.isBlank()) {
            jdbcUrl = databaseUrl;
        } else if (database != null && database.startsWith("jdbc:postgresql:")) {
            jdbcUrl = database;
        } else {
            throw new IllegalStateException("PostgreSQL requires ddbid.database.url or DDBID_DATABASE_URL.");
        }

        hikariConfig.setDriverClassName("org.postgresql.Driver");
        hikariConfig.setJdbcUrl(jdbcUrl);
        if (databaseUser != null && !databaseUser.isBlank()) {
            hikariConfig.setUsername(databaseUser);
        }
        if (databasePassword != null && !databasePassword.isBlank()) {
            hikariConfig.setPassword(databasePassword);
        }
    }
   
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Like to expose intenal rep. Only one DB connection/ template available")
    public JdbcTemplate getJdbcTemplate() {
        init();
        return jdbcTemplate;
    }

    public Connection getConnection() throws SQLException {
        init();
        final Connection newConnection = dataSource.getConnection();
        newConnection.setAutoCommit(false);
        return newConnection;
    }

    public boolean isPostgres() {
        return DatabaseType.from(databaseType) == DatabaseType.POSTGRES;
    }
    
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("Database closed");
        }
    }

    private enum DatabaseType {
        H2("h2"),
        POSTGRES("postgres");

        @Getter
        private final String type;

        DatabaseType(String type) {
            this.type = type;
        }

        private static DatabaseType from(String value) {
            if (value == null || value.isBlank()) {
                return H2;
            }
            final String normalized = value.trim().toLowerCase();
            if ("postgres".equals(normalized) || "postgresql".equals(normalized) || "pg".equals(normalized)) {
                return POSTGRES;
            }
            if ("h2".equals(normalized)) {
                return H2;
            }
            throw new IllegalArgumentException("Unsupported database type: " + value);
        }
    }
}
