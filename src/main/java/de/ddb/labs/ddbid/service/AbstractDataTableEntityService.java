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
import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;

abstract class AbstractDataTableEntityService<T> {

    // Keep cached results available between daily refreshes.
    private static final long CACHE_TTL_MILLIS = 90_000_000;

    private final Calendar calendar = Calendar.getInstance(Locale.GERMANY);
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE;

    private volatile CacheEntry<Timestamp> latestTimestampCache;
    private volatile CacheEntry<Map<String, Timestamp>> timestampsCache;
    private volatile CacheEntry<Map<String, List<String>>> filterOptionsCache;

    public final Page<T> getDdbIds(PagingRequest pagingRequest) {
        return DataTablePageQueryHelper.page(
                logger(),
                database(),
                entityLabel(),
                tableName(),
                fields(),
                rowType(),
                pagingRequest,
                this::latestTimestamp);
    }

    public final Map<String, Timestamp> getTimestamps() {
        CacheEntry<Map<String, Timestamp>> cached = timestampsCache;
        if (cached != null && cached.isValid()) {
            return cached.value();
        }
        synchronized (this) {
            cached = timestampsCache;
            if (cached != null && cached.isValid()) {
                return cached.value();
            }
            final Map<String, Timestamp> timestamps = DataTableTimestampSqlHelper.timestamps(
                    logger(),
                    database(),
                    entityLabel(),
                    tableName(),
                    calendar,
                    dateTimeFormatter);
            timestampsCache = new CacheEntry<>(timestamps);
            return timestamps;
        }
    }

    public final void warmUp() {
        try {
            logger().info("Warming up {} caches ...", entityLabel());
            getTimestamps();
            getFilterOptions();
            logger().info("{} caches warmed up.", entityLabel());
        } catch (RuntimeException e) {
            logger().warn("Could not warm up {} caches: {}", entityLabel(), e.getMessage());
        }
    }

    @Scheduled(cron = "${ddbid.cache.refresh-cron:0 30 2 * * *}")
    public final void refreshDailyCaches() {
        try {
            logger().info("Refreshing {} caches ...", entityLabel());
            clearTimestampCache();
            getTimestamps();
            getFilterOptions();
            logger().info("{} caches refreshed.", entityLabel());
        } catch (RuntimeException e) {
            logger().warn("Could not refresh {} caches: {}", entityLabel(), e.getMessage());
        }
    }

    public final synchronized void clearTimestampCache() {
        latestTimestampCache = null;
        timestampsCache = null;
        filterOptionsCache = null;
    }

    public final Map<String, List<String>> getFilterOptions() {
        CacheEntry<Map<String, List<String>>> cached = filterOptionsCache;
        if (cached != null && cached.isValid()) {
            return cached.value();
        }
        synchronized (this) {
            cached = filterOptionsCache;
            if (cached != null && cached.isValid()) {
                return cached.value();
            }
            final Map<String, List<String>> options = DataTableFilterOptionsHelper.copyOptions(loadFilterOptions(latestTimestamp()));
            filterOptionsCache = new CacheEntry<>(options);
            return options;
        }
    }

    protected abstract Logger logger();

    protected abstract Database database();

    protected abstract String entityLabel();

    protected abstract String tableName();

    protected abstract List<String> fields();

    protected abstract Class<T> rowType();

    protected abstract Map<String, List<String>> loadFilterOptions(Timestamp latestTimestamp);

    private Timestamp latestTimestamp() {
        CacheEntry<Timestamp> cached = latestTimestampCache;
        if (cached != null && cached.isValid()) {
            return cached.value();
        }
        synchronized (this) {
            cached = latestTimestampCache;
            if (cached != null && cached.isValid()) {
                return cached.value();
            }
            final Timestamp timestamp = DataTableTimestampSqlHelper.latestTimestamp(logger(), database(), entityLabel(), tableName());
            if (timestamp != null) {
                latestTimestampCache = new CacheEntry<>(timestamp);
            }
            return timestamp;
        }
    }

    private record CacheEntry<T>(T value, long expiresAt) {

        CacheEntry(T value) {
            this(value, System.currentTimeMillis() + CACHE_TTL_MILLIS);
        }

        boolean isValid() {
            return System.currentTimeMillis() < expiresAt;
        }
    }
}
