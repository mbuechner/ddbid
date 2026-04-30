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
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;

final class DataTableTimestampSqlHelper {

    private DataTableTimestampSqlHelper() {
    }

    static Timestamp latestTimestamp(Logger log, Database database, String label, String tableName) {
        return JdbcQueryTimer.queryForObject(
                log,
                database.getJdbcTemplate(),
                label + ".selectedTimestamp",
                "SELECT \"timestamp\" FROM \"" + tableName + "\" ORDER BY \"timestamp\" DESC LIMIT 1",
                Timestamp.class);
    }

    static Map<String, Timestamp> timestamps(Logger log, Database database, String label, String tableName, Calendar cal, DateTimeFormatter dtf) {
        final List<Timestamp> ts = JdbcQueryTimer.queryForList(
                log,
                database.getJdbcTemplate(),
                label + ".timestamps",
                "SELECT \"timestamp\" FROM \"" + tableName + "\" GROUP BY \"timestamp\" ORDER BY \"timestamp\"",
                Timestamp.class);
        final Map<String, Timestamp> result = new TreeMap<>();
        for (Timestamp t : ts) {
            cal.setTime(t);
            result.put(dtf.format(t.toLocalDateTime()) + " (CW" + cal.get(Calendar.WEEK_OF_YEAR) + ")", t);
        }
        return result;
    }
}
