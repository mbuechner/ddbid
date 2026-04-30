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
import de.ddb.labs.ddbid.model.person.Person;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
public class PersonService {

    private static final long TIMESTAMP_CACHE_TTL_MILLIS = 600_000;

    private final Calendar cal = Calendar.getInstance(Locale.GERMANY);
    private final DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE;
    private volatile Timestamp latestTimestampCache;
    private volatile long latestTimestampCacheUntil;
    private volatile Map<String, Timestamp> timestampsCache;
    private volatile long timestampsCacheUntil;

    @Autowired
    private Database database;
    @Value("${ddbid.database.table.person}")
    private String tableName;

    public Page<Person> getDdbIds(PagingRequest pagingRequest) {
        return DataTablePageQueryHelper.page(
                log,
                database,
                "person",
                tableName,
                PersonDoc.getStaticHeader(),
                Person.class,
                pagingRequest,
                this::latestTimestamp);
    }

    public Map<String, Timestamp> getTimestamps() {
        try {
            return timestamps();
        } catch (EmptyResultDataAccessException e) {
            log.debug("No record found in database for timestamp", e);
            return null;
        }
    }

    public void clearTimestampCache() {
        latestTimestampCache = null;
        latestTimestampCacheUntil = 0;
        timestampsCache = null;
        timestampsCacheUntil = 0;
    }

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
            cached = DataTableTimestampSqlHelper.latestTimestamp(log, database, "person", tableName);
            latestTimestampCache = cached;
            latestTimestampCacheUntil = now + TIMESTAMP_CACHE_TTL_MILLIS;
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
            cached = DataTableTimestampSqlHelper.timestamps(log, database, "person", tableName, cal, dtf);
            timestampsCache = cached;
            timestampsCacheUntil = now + TIMESTAMP_CACHE_TTL_MILLIS;
            return cached;
        }
    }
}
