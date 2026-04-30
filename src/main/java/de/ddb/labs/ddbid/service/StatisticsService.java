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

import de.ddb.labs.ddbid.database.Database;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StatisticsService {

    private static final int PROVIDER_LIMIT = 50;

    private final Object cacheLock = new Object();
    private final Calendar cal = Calendar.getInstance(Locale.GERMANY);
    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE;

    @Autowired
    private Database database;

    @Value("${ddbid.database.table.item}")
    private String itemTableName;

    @Value("${ddbid.database.table.person}")
    private String personTableName;

    @Value("${ddbid.database.table.organization}")
    private String organizationTableName;

    @Value("${ddbid.database.indexes.auto:false}")
    private boolean autoEnsureIndexes;

    private volatile CacheEntry cacheEntry;

    public StatisticsData getStatisticsData() {
        return getStatisticsData(false);
    }

    public StatisticsData getStatisticsData(boolean refresh) {
        final Instant now = Instant.now();
        CacheEntry current = cacheEntry;
        if (!refresh && current != null && current.isValid()) {
            return current.data();
        }

        synchronized (cacheLock) {
            current = cacheEntry;
            if (!refresh && current != null && current.isValid()) {
                return current.data();
            }

            final StatisticsData data = loadStatisticsData();
            data.setGeneratedAt(now);
            cacheEntry = new CacheEntry(LocalDate.now(), data);
            return data;
        }
    }

    public void clearCache() {
        cacheEntry = null;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void afterStartup() {
        if (autoEnsureIndexes) {
            ensureDatabaseIndexes();
        } else {
            log.info("Automatic database index creation is disabled. Use /maintenance/indexes to create missing indexes.");
        }
    }

    @Scheduled(cron = "${ddbid.statistics.cache.refresh-cron:0 15 2 * * *}")
    public void refreshDailyCache() {
        try {
            getStatisticsData(true);
            log.info("Daily statistics cache refreshed.");
        } catch (RuntimeException e) {
            log.warn("Could not refresh daily statistics cache. {}", e.getMessage());
        }
    }

    private StatisticsData loadStatisticsData() {
        final StatisticsData data = JdbcQueryTimer.query(log, database.getJdbcTemplate(), "statistics.summary", summaryQuery(), new ResultSetExtractor<StatisticsData>() {
            @Override
            public StatisticsData extractData(ResultSet rs) throws SQLException {
                if (!rs.next()) {
                    return new StatisticsData();
                }
                final StatisticsData result = new StatisticsData();
                result.setMissingItemsCount(rs.getInt("missing_count"));
                result.setNewItemsCount(rs.getInt("new_count"));
                result.setMinTimestampLabel(formatTimestamp(rs.getTimestamp("min_ts")));
                result.setLatestTimestampLabel(formatTimestamp(rs.getTimestamp("max_ts")));
                return result;
            }
        });

        final Map<Timestamp, StatusCounts> itemStatus = JdbcQueryTimer.query(log, database.getJdbcTemplate(), "statistics.itemStatus", itemStatusQuery(), new ResultSetExtractor<Map<Timestamp, StatusCounts>>() {
            @Override
            public Map<Timestamp, StatusCounts> extractData(ResultSet rs) throws SQLException {
                final Map<Timestamp, StatusCounts> result = new LinkedHashMap<>();
                while (rs.next()) {
                    result.put(rs.getTimestamp("timestamp"), new StatusCounts(rs.getInt("missing_count"), rs.getInt("new_count")));
                }
                return result;
            }
        });
        data.setItemStatusKeys(makeLabels(itemStatus));
        data.setItemMissingValues(itemStatus.values().stream().map(StatusCounts::missing).toList());
        data.setItemNewValues(itemStatus.values().stream().map(StatusCounts::fresh).toList());
        if (!itemStatus.isEmpty()) {
            final StatusCounts latest = itemStatus.values().stream().reduce((first, second) -> second).orElse(new StatusCounts(0, 0));
            data.setLatestMissingItemsCount(latest.missing());
            data.setLatestNewItemsCount(latest.fresh());
        }

        final Map<String, Integer> missingByProvider = queryStringIntegerMap("statistics.missingByProvider", missingByProviderQuery());
        data.setMissingByProviderIdKeys(new ArrayList<>(missingByProvider.keySet()));
        data.setMissingByProviderIdValues(new ArrayList<>(missingByProvider.values()));

        final Map<String, Integer> missingBySector = queryStringIntegerMap("statistics.missingBySector", missingBySectorQuery());
        data.setMissingBySectorFctKeys(new ArrayList<>(missingBySector.keySet()));
        data.setMissingBySectorFctValues(new ArrayList<>(missingBySector.values()));

        return data;
    }

    public void ensureDatabaseIndexes() {
        try {
            log.info("Ensure database indexes...");
            database.getJdbcTemplate().execute("""
                                               CREATE INDEX IF NOT EXISTS "item_timestamp" ON "{{item}}"("timestamp");
                                               CREATE INDEX IF NOT EXISTS "item_status" ON "{{item}}"("status");
                                               CREATE INDEX IF NOT EXISTS "item_status_timestamp" ON "{{item}}"("status", "timestamp");
                                               CREATE INDEX IF NOT EXISTS "item_status_provider" ON "{{item}}"("status", "provider_id");
                                               CREATE INDEX IF NOT EXISTS "item_status_sector" ON "{{item}}"("status", "sector_fct");
                                               CREATE INDEX IF NOT EXISTS "item_filter_id" ON "{{item}}"("status", "timestamp", "id");
                                               CREATE INDEX IF NOT EXISTS "item_filter_provider_item" ON "{{item}}"("status", "timestamp", "provider_item_id");
                                               CREATE INDEX IF NOT EXISTS "item_filter_dataset" ON "{{item}}"("status", "timestamp", "dataset_id");
                                               CREATE INDEX IF NOT EXISTS "item_filter_provider" ON "{{item}}"("status", "timestamp", "provider_id");
                                               CREATE INDEX IF NOT EXISTS "item_filter_sector" ON "{{item}}"("status", "timestamp", "sector_fct");
                                               CREATE INDEX IF NOT EXISTS "item_filter_supplier" ON "{{item}}"("status", "timestamp", "supplier_id");
                                               CREATE INDEX IF NOT EXISTS "person_timestamp" ON "{{person}}"("timestamp");
                                               CREATE INDEX IF NOT EXISTS "person_status" ON "{{person}}"("status");
                                               CREATE INDEX IF NOT EXISTS "person_status_timestamp" ON "{{person}}"("status", "timestamp");
                                               CREATE INDEX IF NOT EXISTS "person_filter_id" ON "{{person}}"("status", "timestamp", "id");
                                               CREATE INDEX IF NOT EXISTS "person_filter_variant" ON "{{person}}"("status", "timestamp", "variant_id");
                                               CREATE INDEX IF NOT EXISTS "person_filter_type" ON "{{person}}"("status", "timestamp", "type");
                                               CREATE INDEX IF NOT EXISTS "organization_timestamp" ON "{{organization}}"("timestamp");
                                               CREATE INDEX IF NOT EXISTS "organization_status" ON "{{organization}}"("status");
                                               CREATE INDEX IF NOT EXISTS "organization_status_timestamp" ON "{{organization}}"("status", "timestamp");
                                               CREATE INDEX IF NOT EXISTS "organization_filter_id" ON "{{organization}}"("status", "timestamp", "id");
                                               CREATE INDEX IF NOT EXISTS "organization_filter_variant" ON "{{organization}}"("status", "timestamp", "variant_id");
                                               CREATE INDEX IF NOT EXISTS "organization_filter_type" ON "{{organization}}"("status", "timestamp", "type");
                                               """
                    .replace("{{item}}", itemTableName)
                    .replace("{{person}}", personTableName)
                    .replace("{{organization}}", organizationTableName));
            if (database.isPostgres()) {
                ensurePostgresSearchIndexes();
            }
            log.info("Database indexes are available.");
        } catch (RuntimeException e) {
            log.warn("Could not ensure database indexes. {}", e.getMessage());
        }
    }

    private void ensurePostgresSearchIndexes() {
        database.getJdbcTemplate().execute("""
                                           CREATE EXTENSION IF NOT EXISTS pg_trgm;
                                           CREATE INDEX IF NOT EXISTS "item_trgm_id" ON "{{item}}" USING GIN ("id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "item_trgm_provider_item" ON "{{item}}" USING GIN ("provider_item_id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "item_trgm_dataset" ON "{{item}}" USING GIN ("dataset_id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "item_trgm_label" ON "{{item}}" USING GIN ("label" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "item_trgm_provider" ON "{{item}}" USING GIN ("provider_id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "item_trgm_supplier" ON "{{item}}" USING GIN ("supplier_id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "person_trgm_id" ON "{{person}}" USING GIN ("id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "person_trgm_variant" ON "{{person}}" USING GIN ("variant_id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "person_trgm_name" ON "{{person}}" USING GIN ("preferredName" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "organization_trgm_id" ON "{{organization}}" USING GIN ("id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "organization_trgm_variant" ON "{{organization}}" USING GIN ("variant_id" gin_trgm_ops);
                                           CREATE INDEX IF NOT EXISTS "organization_trgm_name" ON "{{organization}}" USING GIN ("preferredName" gin_trgm_ops);
                                           """
                .replace("{{item}}", itemTableName)
                .replace("{{person}}", personTableName)
                .replace("{{organization}}", organizationTableName));
    }

    private Map<String, Integer> queryStringIntegerMap(String label, String sql) {
        return JdbcQueryTimer.query(log, database.getJdbcTemplate(), label, sql, new ResultSetExtractor<Map<String, Integer>>() {
            @Override
            public Map<String, Integer> extractData(ResultSet rs) throws SQLException {
                final Map<String, Integer> result = new LinkedHashMap<>();
                while (rs.next()) {
                    result.put(rs.getString("label"), rs.getInt("count"));
                }
                return result;
            }
        });
    }

    private String summaryQuery() {
        return """
               SELECT
                 SUM(CASE WHEN "status" = 'MISSING' THEN 1 ELSE 0 END) AS "missing_count",
                 SUM(CASE WHEN "status" = 'NEW' THEN 1 ELSE 0 END) AS "new_count",
                 MIN("timestamp") AS "min_ts",
                 MAX("timestamp") AS "max_ts"
               FROM "{{item}}";
               """.replace("{{item}}", itemTableName);
    }

    private String missingByProviderQuery() {
        return """
               SELECT "provider_id" AS "label", COUNT("id") AS "count"
               FROM "{{item}}"
               WHERE "status" = 'MISSING'
               GROUP BY "provider_id"
               ORDER BY COUNT("id") DESC
               LIMIT {{limit}};
               """.replace("{{item}}", itemTableName).replace("{{limit}}", Integer.toString(PROVIDER_LIMIT));
    }

    private String missingBySectorQuery() {
        return """
               SELECT "sector_fct" AS "label", COUNT("id") AS "count"
               FROM "{{item}}"
               WHERE "status" = 'MISSING'
               GROUP BY "sector_fct"
               ORDER BY COUNT("id") DESC;
               """.replace("{{item}}", itemTableName);
    }

    private String itemStatusQuery() {
        return """
               SELECT
                 "timestamp",
                 SUM(CASE WHEN "status" = 'MISSING' THEN 1 ELSE 0 END) AS "missing_count",
                 SUM(CASE WHEN "status" = 'NEW' THEN 1 ELSE 0 END) AS "new_count"
               FROM "{{item}}"
               GROUP BY "timestamp"
               ORDER BY "timestamp" ASC;
               """.replace("{{item}}", itemTableName);
    }

    private String formatTimestamp(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }
        cal.setTime(timestamp);
        return dtf.format(timestamp.toLocalDateTime()) + " (CW" + cal.get(Calendar.WEEK_OF_YEAR) + ")";
    }

    private List<String> makeLabels(Map<Timestamp, ?> ts) {
        final List<String> result = new ArrayList<>();
        for (Timestamp timestamp : ts.keySet()) {
            result.add(formatTimestamp(timestamp));
        }
        return result;
    }

    private record CacheEntry(LocalDate createdAt, StatisticsData data) {

        private boolean isValid() {
            return createdAt.equals(LocalDate.now());
        }
    }

    private record StatusCounts(int missing, int fresh) {
    }

    @Getter
    public static class StatisticsData {

        private int missingItemsCount;
        private int newItemsCount;
        private int latestMissingItemsCount;
        private int latestNewItemsCount;
        private String minTimestampLabel;
        private String latestTimestampLabel;
        private Instant generatedAt;
        private List<String> missingByProviderIdKeys = List.of();
        private List<Integer> missingByProviderIdValues = List.of();
        private List<String> missingBySectorFctKeys = List.of();
        private List<Integer> missingBySectorFctValues = List.of();
        private List<String> itemStatusKeys = List.of();
        private List<Integer> itemMissingValues = List.of();
        private List<Integer> itemNewValues = List.of();

        private void setMissingItemsCount(int missingItemsCount) {
            this.missingItemsCount = missingItemsCount;
        }

        private void setNewItemsCount(int newItemsCount) {
            this.newItemsCount = newItemsCount;
        }

        private void setLatestMissingItemsCount(int latestMissingItemsCount) {
            this.latestMissingItemsCount = latestMissingItemsCount;
        }

        private void setLatestNewItemsCount(int latestNewItemsCount) {
            this.latestNewItemsCount = latestNewItemsCount;
        }

        private void setMinTimestampLabel(String minTimestampLabel) {
            this.minTimestampLabel = minTimestampLabel;
        }

        private void setLatestTimestampLabel(String latestTimestampLabel) {
            this.latestTimestampLabel = latestTimestampLabel;
        }

        private void setGeneratedAt(Instant generatedAt) {
            this.generatedAt = generatedAt;
        }

        private void setMissingByProviderIdKeys(List<String> missingByProviderIdKeys) {
            this.missingByProviderIdKeys = missingByProviderIdKeys;
        }

        private void setMissingByProviderIdValues(List<Integer> missingByProviderIdValues) {
            this.missingByProviderIdValues = missingByProviderIdValues;
        }

        private void setMissingBySectorFctKeys(List<String> missingBySectorFctKeys) {
            this.missingBySectorFctKeys = missingBySectorFctKeys;
        }

        private void setMissingBySectorFctValues(List<Integer> missingBySectorFctValues) {
            this.missingBySectorFctValues = missingBySectorFctValues;
        }

        private void setItemStatusKeys(List<String> itemStatusKeys) {
            this.itemStatusKeys = itemStatusKeys;
        }

        private void setItemMissingValues(List<Integer> itemMissingValues) {
            this.itemMissingValues = itemMissingValues;
        }

        private void setItemNewValues(List<Integer> itemNewValues) {
            this.itemNewValues = itemNewValues;
        }
    }
}
