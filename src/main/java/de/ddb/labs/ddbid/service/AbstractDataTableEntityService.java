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
import org.springframework.dao.EmptyResultDataAccessException;

abstract class AbstractDataTableEntityService<T> {

    private static final long CACHE_TTL_MILLIS = 600_000;

    private final Calendar calendar = Calendar.getInstance(Locale.GERMANY);
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE;

    private volatile Timestamp latestTimestampCache;
    private volatile long latestTimestampCacheUntil;
    private volatile Map<String, Timestamp> timestampsCache;
    private volatile long timestampsCacheUntil;
    private volatile Map<String, List<String>> filterOptionsCache;
    private volatile long filterOptionsCacheUntil;

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
        try {
            return timestamps();
        } catch (EmptyResultDataAccessException e) {
            logger().debug("No record found in database for timestamp", e);
            return null;
        }
    }

    public final void clearTimestampCache() {
        latestTimestampCache = null;
        latestTimestampCacheUntil = 0;
        timestampsCache = null;
        timestampsCacheUntil = 0;
        filterOptionsCache = null;
        filterOptionsCacheUntil = 0;
    }

    public final Map<String, List<String>> getFilterOptions() {
        long now = System.currentTimeMillis();
        Map<String, List<String>> cached = filterOptionsCache;
        if (cached != null && now < filterOptionsCacheUntil) {
            return DataTableFilterOptionsHelper.copyOptions(cached);
        }
        synchronized (this) {
            now = System.currentTimeMillis();
            cached = filterOptionsCache;
            if (cached != null && now < filterOptionsCacheUntil) {
                return DataTableFilterOptionsHelper.copyOptions(cached);
            }
            cached = loadFilterOptions(latestTimestamp());
            filterOptionsCache = DataTableFilterOptionsHelper.copyOptions(cached);
            filterOptionsCacheUntil = now + CACHE_TTL_MILLIS;
            return DataTableFilterOptionsHelper.copyOptions(filterOptionsCache);
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
        long now = System.currentTimeMillis();
        Timestamp cached = latestTimestampCache;
        if (cached != null && now < latestTimestampCacheUntil) {
            return cached;
        }
        synchronized (this) {
            now = System.currentTimeMillis();
            cached = latestTimestampCache;
            if (cached != null && now < latestTimestampCacheUntil) {
                return cached;
            }
            cached = DataTableTimestampSqlHelper.latestTimestamp(logger(), database(), entityLabel(), tableName());
            latestTimestampCache = cached;
            latestTimestampCacheUntil = now + CACHE_TTL_MILLIS;
            return cached;
        }
    }

    private Map<String, Timestamp> timestamps() {
        long now = System.currentTimeMillis();
        Map<String, Timestamp> cached = timestampsCache;
        if (cached != null && now < timestampsCacheUntil) {
            return cached;
        }
        synchronized (this) {
            now = System.currentTimeMillis();
            cached = timestampsCache;
            if (cached != null && now < timestampsCacheUntil) {
                return cached;
            }
            cached = DataTableTimestampSqlHelper.timestamps(
                    logger(),
                    database(),
                    entityLabel(),
                    tableName(),
                    calendar,
                    dateTimeFormatter);
            timestampsCache = cached;
            timestampsCacheUntil = now + CACHE_TTL_MILLIS;
            return cached;
        }
    }
}