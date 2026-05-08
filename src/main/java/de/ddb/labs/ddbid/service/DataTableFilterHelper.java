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
import de.ddb.labs.ddbid.model.paging.ColumnControl;
import de.ddb.labs.ddbid.model.paging.ColumnControlSearch;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import de.ddb.labs.ddbid.model.paging.Search;
import java.util.ArrayList;
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
            appendCondition(where, column.getData(), filter, values);
            active = true;
        }
        return active;
    }

    private static boolean isExternallyControlled(String field) {
        return "timestamp".equals(field) || "status".equals(field);
    }

    private static ColumnFilter columnFilter(Column column) {
        ColumnControl columnControl = column.getColumnControl();
        if (columnControl != null && columnControl.getSearch() != null) {
            ColumnControlSearch search = columnControl.getSearch();
            return new ColumnFilter(search.getValue(), normalizedMode(search.getLogic()));
        }

        Search search = column.getSearch();
        if (search != null) {
            return new ColumnFilter(search.getValue(), search.getMode());
        }
        return null;
    }

    private static String normalizedMode(String logic) {
        if (logic == null) {
            return "";
        }

        if ("equal".equalsIgnoreCase(logic)) {
            return "exact";
        }
        if ("contains".equalsIgnoreCase(logic)) {
            return "contains";
        }
        return logic;
    }

    private static void appendCondition(StringBuilder where, String field, ColumnFilter filter, List<Object> values) {
        FilterCondition condition = condition(field, filter.value(), filter.mode());
        where.append(condition.sql()).append(" AND ");
        values.addAll(condition.values());
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

    private static FilterCondition condition(String field, String value, String mode) {
        String trimmed = value == null ? "" : value.trim();
        String expression = expression(field);
        if (isExactMode(mode, field, trimmed)) {
            return new FilterCondition(expression + "=?", List.of(trimmed));
        }
        return new FilterCondition(
                expression + " ILIKE ? ESCAPE '\\'",
                List.of("%" + escapeLike(trimmed) + "%"));
    }

    private static boolean isExactMode(String mode, String field, String value) {
        if ("exact".equalsIgnoreCase(mode)) {
            return true;
        }
        if ("contains".equalsIgnoreCase(mode)) {
            return false;
        }
        return isLegacyExactMatchField(field) && looksLikeIdentifier(value);
    }

    private static boolean isLegacyExactMatchField(String field) {
        return "id".equals(field)
                || "provider_item_id".equals(field)
                || "dataset_id".equals(field)
                || "provider_id".equals(field)
                || "supplier_id".equals(field)
                || "variant_id".equals(field)
                || "type".equals(field);
    }

    private static boolean looksLikeIdentifier(String value) {
        return !value.isBlank() && value.matches("[\\p{Alnum}_:./\\-]{3,}");
    }

    private static String escapeLike(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_");
    }

    private record FilterCondition(String sql, List<Object> values) {

        private FilterCondition {
            values = List.copyOf(new ArrayList<>(values));
        }
    }

    private record ColumnFilter(String value, String mode) {

        boolean hasCondition() {
            return value != null && !value.isBlank();
        }

        @Override
        public String value() {
            return value == null ? "" : value;
        }

        @Override
        public String mode() {
            return mode == null ? "" : mode;
        }
    }
}
