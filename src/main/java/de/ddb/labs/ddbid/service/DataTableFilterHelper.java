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

import de.ddb.labs.ddbid.model.paging.Column;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import de.ddb.labs.ddbid.model.paging.Search;
import java.util.List;
import java.util.Set;

final class DataTableFilterHelper {

    private DataTableFilterHelper() {
    }

    static boolean appendColumnFilters(PagingRequest request, Set<String> allowedFields, StringBuilder where, List<Object> values) {
        if (request.getColumns() == null) {
            return false;
        }

        boolean active = false;
        for (Column column : request.getColumns()) {
            if (column == null || column.getData() == null || !allowedFields.contains(column.getData()) || isExternallyControlled(column.getData())) {
                continue;
            }

            ColumnFilter filter = columnFilter(column);
            if (filter == null || !filter.hasCondition()) {
                continue;
            }

            appendAnd(where);
            appendCondition(where, column.getData(), filter.value(), values);
            active = true;
        }
        return active;
    }

    private static boolean isExternallyControlled(String field) {
        return "timestamp".equals(field) || "status".equals(field);
    }

    private static ColumnFilter columnFilter(Column column) {
        Search search = column.getSearch();
        if (search != null) {
            return new ColumnFilter(search.getValue());
        }
        return null;
    }

    private static void appendCondition(StringBuilder where, String field, String value, List<Object> values) {
        String expression = expression(field);
        where.append(expression).append(" ILIKE ? ESCAPE '\\' AND ");
        values.add("%" + escapeLike(value) + "%");
    }

    private static void appendAnd(StringBuilder where) {
        String text = where.toString();
        if (!text.endsWith("WHERE ") && !text.endsWith("AND ")) {
            where.append("AND ");
        }
    }

    private static String quote(String field) {
        return "\"" + field + "\"";
    }

    private static String expression(String field) {
        String column = quote(field);
        if ("timestamp".equals(field)) {
            return "CAST(" + column + " AS VARCHAR)";
        }
        return column;
    }

    private static String escapeLike(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_");
    }

    private record ColumnFilter(String value) {

        boolean hasCondition() {
            return value != null && !value.isBlank();
        }

        @Override
        public String value() {
            return value == null ? "" : value;
        }
    }
}
