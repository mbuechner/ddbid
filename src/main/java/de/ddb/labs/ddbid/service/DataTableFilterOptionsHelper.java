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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

final class DataTableFilterOptionsHelper {

    private static final int MAX_OPTIONS = 5000;

    private DataTableFilterOptionsHelper() {
    }

    static Map<String, List<String>> itemOptions(Logger log, Database database, String tableName) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        result.put("status", List.of("MISSING", "NEW", "FOUND", "ALL"));
        result.put("dataset_id", distinctValues(log, database, tableName, "dataset_id"));
        result.put("provider_id", distinctValues(log, database, tableName, "provider_id"));
        result.put("sector_fct", distinctValues(log, database, tableName, "sector_fct"));
        result.put("supplier_id", distinctValues(log, database, tableName, "supplier_id"));
        return result;
    }

    private static List<String> distinctValues(Logger log, Database database, String tableName, String columnName) {
        String sql = "SELECT \"" + columnName + "\" FROM \"" + tableName + "\" "
                + "WHERE \"" + columnName + "\" IS NOT NULL AND \"" + columnName + "\"<>'' "
                + "GROUP BY \"" + columnName + "\" "
                + "ORDER BY \"" + columnName + "\" "
                + "LIMIT ?";
        return JdbcQueryTimer.queryForList(
                log,
                database.getJdbcTemplate(),
                "item.filterOptions." + columnName,
                sql,
                String.class,
                MAX_OPTIONS);
    }
}
