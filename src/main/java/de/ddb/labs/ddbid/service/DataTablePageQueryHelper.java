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
import de.ddb.labs.ddbid.model.paging.Column;
import de.ddb.labs.ddbid.model.paging.Order;
import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

final class DataTablePageQueryHelper {

    private DataTablePageQueryHelper() {
    }

    static <T> Page<T> page(
            Logger log,
            Database database,
            String label,
            String tableName,
            List<String> fields,
            Class<T> rowType,
            PagingRequest pagingRequest,
            Supplier<Timestamp> latestTimestamp) {

        log.debug("Request received: {}", pagingRequest);

        final String status = DataTableStatusHelper.status(pagingRequest.getStatus());
        final DataTableTimestampHelper.TimestampFilter timestampFilter =
                DataTableTimestampHelper.filter(pagingRequest.getTimestamp(), latestTimestamp);
        final String searchValue = searchValue(pagingRequest);
        final Set<String> allowedFields = new HashSet<>(fields);

        final List<Object> whereValues = new ArrayList<>();
        final StringBuilder where = new StringBuilder("WHERE ");
        appendStatusFilter(where, whereValues, status);
        appendTimestampFilter(where, whereValues, timestampFilter);

        final int totalCount = JdbcQueryTimer.queryForObject(
                log,
                database.getJdbcTemplate(),
                label + ".totalCount",
                "SELECT count(*) FROM \"" + tableName + "\" " + normalizedWhere(where),
                Integer.class,
                whereValues.toArray());

        appendGlobalSearch(where, whereValues, fields, searchValue);
        final boolean columnFilterActive = DataTableFilterHelper.appendColumnFilters(
                pagingRequest,
                allowedFields,
                where,
                whereValues);

        final String normalizedWhere = normalizedWhere(where);
        if (pagingRequest.getTimestamp() != null) {
            log.debug("Timestamp filter: {}", timestampFilter);
        }

        final int filteredCount = searchValue.isEmpty() && !columnFilterActive
                ? totalCount
                : JdbcQueryTimer.queryForObject(
                        log,
                        database.getJdbcTemplate(),
                        label + ".filteredCount",
                        "SELECT count(*) FROM \"" + tableName + "\" " + normalizedWhere,
                        Integer.class,
                        whereValues.toArray());

        final StringBuilder query = new StringBuilder("SELECT ")
                .append(DataTableSqlHelper.selectColumns(fields))
                .append(" FROM \"")
                .append(tableName)
                .append("\" ")
                .append(normalizedWhere);

        appendOrderBy(query, pagingRequest, allowedFields);

        final List<Object> values = new ArrayList<>(whereValues);
        appendLimitAndOffset(query, values, pagingRequest);

        List<T> rows = JdbcQueryTimer.query(
                log,
                database.getJdbcTemplate(),
                label + ".page",
                query.toString(),
                new BeanPropertyRowMapper<>(rowType),
                values.toArray());

        final Page<T> page = new Page<>(rows);
        rows = null; // allow large result pages to be reclaimed early
        page.setRecordsFiltered(filteredCount);
        page.setRecordsTotal(totalCount);
        page.setDraw(pagingRequest.getDraw());

        log.debug("Sending page: {}", page);
        return page;
    }

    private static String searchValue(PagingRequest pagingRequest) {
        return pagingRequest.getSearch() == null || pagingRequest.getSearch().getValue() == null
                ? ""
                : pagingRequest.getSearch().getValue();
    }

    private static void appendStatusFilter(StringBuilder where, List<Object> values, String status) {
        if (!DataTableStatusHelper.ALL.equals(status)) {
            where.append("\"status\"=? AND ");
            values.add(status);
        }
    }

    private static void appendTimestampFilter(
            StringBuilder where,
            List<Object> values,
            DataTableTimestampHelper.TimestampFilter timestampFilter) {
        if (!timestampFilter.active()) {
            return;
        }
        if (timestampFilter.range()) {
            where.append("\"timestamp\">=? AND \"timestamp\"<? AND ");
            values.add(timestampFilter.startInclusive());
            values.add(timestampFilter.endExclusive());
        } else {
            where.append("\"timestamp\"=? AND ");
            values.add(timestampFilter.startInclusive());
        }
    }

    private static void appendGlobalSearch(
            StringBuilder where,
            List<Object> values,
            List<String> fields,
            String searchValue) {
        if (searchValue.isEmpty()) {
            return;
        }

        where.append("(");
        for (String field : fields) {
            if (!"status".equals(field) && !"timestamp".equals(field)) {
                where.append(DataTableSqlHelper.quote(field)).append(" ILIKE ? OR ");
                values.add("%" + searchValue + "%");
            }
        }
        where.setLength(where.length() - 3);
        where.append(") ");
    }

    private static void appendOrderBy(StringBuilder query, PagingRequest pagingRequest, Set<String> allowedFields) {
        if (pagingRequest.getOrder() == null || pagingRequest.getOrder().isEmpty()) {
            return;
        }

        final StringBuilder order = new StringBuilder();
        for (Order o : pagingRequest.getOrder()) {
            if (o.getColumn() == null
                    || o.getDir() == null
                    || pagingRequest.getColumns() == null
                    || o.getColumn() < 0
                    || o.getColumn() >= pagingRequest.getColumns().size()) {
                continue;
            }

            final List<Column> columns = pagingRequest.getColumns();
            final String columnName = columns.get(o.getColumn()).getData();
            if (!allowedFields.contains(columnName)) {
                continue;
            }
            order.append(DataTableSqlHelper.quote(columnName))
                    .append(" ")
                    .append(o.getDir())
                    .append(", ");
        }

        if (!order.isEmpty()) {
            order.setLength(order.length() - 2);
            query.append("ORDER BY ").append(order).append(" ");
        }
    }

    private static void appendLimitAndOffset(StringBuilder query, List<Object> values, PagingRequest pagingRequest) {
        if (pagingRequest.getLength() > 0) {
            query.append("LIMIT ? OFFSET ?");
            values.add(pagingRequest.getLength());
            values.add(pagingRequest.getStart());
        }
    }

    private static String normalizedWhere(StringBuilder where) {
        String whereClause = where.toString();
        if (whereClause.endsWith("AND ")) {
            return whereClause.substring(0, whereClause.length() - 4);
        }
        if (whereClause.endsWith("WHERE ")) {
            return whereClause.substring(0, whereClause.length() - 6);
        }
        return whereClause;
    }
}
