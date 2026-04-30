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
package de.ddb.labs.ddbid.service;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

final class JdbcQueryTimer {

    private JdbcQueryTimer() {
    }

    static <T> T queryForObject(Logger log, JdbcTemplate jdbcTemplate, String label, String sql, Class<T> requiredType, Object... args) {
        final long start = System.nanoTime();
        try {
            return jdbcTemplate.queryForObject(sql, requiredType, args);
        } finally {
            logQuery(log, label, sql, args, start);
        }
    }

    static <T> List<T> query(Logger log, JdbcTemplate jdbcTemplate, String label, String sql, RowMapper<T> rowMapper, Object... args) {
        final long start = System.nanoTime();
        try {
            return args == null || args.length == 0
                    ? jdbcTemplate.query(sql, rowMapper)
                    : jdbcTemplate.query(sql, rowMapper, args);
        } finally {
            logQuery(log, label, sql, args, start);
        }
    }

    static <T> T query(Logger log, JdbcTemplate jdbcTemplate, String label, String sql, ResultSetExtractor<T> resultSetExtractor) {
        final long start = System.nanoTime();
        try {
            return jdbcTemplate.query(sql, resultSetExtractor);
        } finally {
            logQuery(log, label, sql, new Object[0], start);
        }
    }

    static <T> List<T> queryForList(Logger log, JdbcTemplate jdbcTemplate, String label, String sql, Class<T> elementType, Object... args) {
        final long start = System.nanoTime();
        try {
            return args == null || args.length == 0
                    ? jdbcTemplate.queryForList(sql, elementType)
                    : jdbcTemplate.queryForList(sql, elementType, args);
        } finally {
            logQuery(log, label, sql, args, start);
        }
    }

    private static void logQuery(Logger log, String label, String sql, Object[] args, long start) {
        if (!log.isDebugEnabled()) {
            return;
        }

        final double elapsedMs = (System.nanoTime() - start) / 1_000_000.0;
        log.debug("{} SQL [{}] params={} took {} ms", label, sql, Arrays.toString(args == null ? new Object[0] : args), String.format("%.2f", elapsedMs));
    }
}
