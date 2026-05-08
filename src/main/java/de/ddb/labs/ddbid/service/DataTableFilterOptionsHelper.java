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
import lombok.experimental.UtilityClass;
import org.slf4j.Logger;

@UtilityClass
final class DataTableFilterOptionsHelper {

    private static final int MAX_OPTIONS = 5000;

    static Map<String, List<String>> itemOptions(Logger log, Database database, String tableName) {
        return options(log, database, tableName, "item", Map.of(
                "dataset_id", "dataset_id",
                "provider_id", "provider_id",
                "sector_fct", "sector_fct",
                "supplier_id", "supplier_id"));
    }

    static Map<String, List<String>> personOptions(Logger log, Database database, String tableName) {
        return options(log, database, tableName, "person", Map.of(
                "variant_id", "variant_id",
                "type", "type"));
    }

    static Map<String, List<String>> organizationOptions(Logger log, Database database, String tableName) {
        return options(log, database, tableName, "organization", Map.of(
                "variant_id", "variant_id",
                "type", "type"));
    }

    private static Map<String, List<String>> options(Logger log, Database database, String tableName, String label, Map<String, String> fields) {
        Map<String, List<String>> result = new LinkedHashMap<>();
        result.put("status", List.of("MISSING", "NEW", "FOUND", "ALL"));
        fields.forEach((optionKey, columnName) -> {
            result.put(optionKey, distinctValues(log, database, label, tableName, columnName));
        });
        return result;
    }

    private static List<String> distinctValues(Logger log, Database database, String label, String tableName, String columnName) {
        String sql = "SELECT \"" + columnName + "\" FROM \"" + tableName + "\" "
                + "WHERE \"" + columnName + "\" IS NOT NULL AND \"" + columnName + "\"<>'' "
                + "GROUP BY \"" + columnName + "\" "
                + "ORDER BY \"" + columnName + "\" "
                + "LIMIT ?";
        return JdbcQueryTimer.queryForList(
                log,
                database.getJdbcTemplate(),
                label + ".filterOptions." + columnName,
                sql,
                String.class,
                MAX_OPTIONS);
    }

    static Map<String, List<String>> copyOptions(Map<String, List<String>> filterOptions) {
        final Map<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : filterOptions.entrySet()) {
            copy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(copy);
    }
}
