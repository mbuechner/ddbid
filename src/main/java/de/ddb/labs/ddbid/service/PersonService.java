/*
 * Copyright 2022 Michael Büchner, Deutsche Digitale Bibliothek
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
import de.ddb.labs.ddbid.model.Status;
import de.ddb.labs.ddbid.model.paging.Column;
import de.ddb.labs.ddbid.model.paging.Order;
import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import de.ddb.labs.ddbid.model.person.Person;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PersonService {

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    @Autowired
    private Database database;
    @Value("${ddbid.database.table.person}")
    private String tableName;

    public Page<Person> getDdbIds(PagingRequest pagingRequest) {

        log.debug("Request received: {}", pagingRequest.toString());

        String status = null;
        if (pagingRequest.getStatus() == null) {
            status = "MISSING";
        } else {
            final List<String> s = Arrays.stream(Status.values()).map(Enum::name).collect(Collectors.toList());
            if (s.contains(pagingRequest.getStatus()) || pagingRequest.getStatus().equals("ALL")) {
                status = pagingRequest.getStatus();
            }
        }

        final StringBuilder query = new StringBuilder("SELECT * FROM main." + tableName + " ");

        // WHERE (Search)
        final List<Object> whereValues = new ArrayList<>();
        final StringBuilder where = new StringBuilder("WHERE ");

        // status (NEW, MISSING, ALL -> null)
        if (status != null && !status.equals("ALL")) {
            where.append("status=? AND ");
            whereValues.add(status);
        }

        // timestamp (null -> show latest, -1 -> show all, value)
        if (pagingRequest.getTimestamp() == null) {
            where.append("timestamp=(SELECT MAX(timestamp) FROM main.");
            where.append(tableName);
            where.append(") AND ");
        } else if (pagingRequest.getTimestamp() == -1) {
        } else {
            where.append("timestamp=?::TIMESTAMP AND ");
            whereValues.add(new Timestamp(pagingRequest.getTimestamp()));
        }

        // query for totalCount
        String whereClause = where.toString();
        // no where clauses? remove it
        if (whereClause.endsWith("AND ")) {
            whereClause = whereClause.substring(0, whereClause.length() - 4);
        } else if (where.toString().endsWith("WHERE ")) {
            whereClause = whereClause.substring(0, whereClause.length() - 6);
        }
        final int totalCount = database.getJdbcTemplate().queryForObject("SELECT count(*) FROM main." + tableName + " " + whereClause, Integer.class, whereValues.toArray());
        // totalCount end

        // with search
        if (!pagingRequest.getSearch().getValue().isEmpty()) {
            where.append("(");
            for (String field : PersonDoc.getStaticHeader()) {
                if (!field.equals("status") || !field.equals("timestamp")) {
                    where.append(field);
                    where.append(" ILIKE ? OR ");
                    whereValues.add("%" + pagingRequest.getSearch().getValue() + "%");
                }
            }
            where.setLength(where.length() - 3); // remove last 'OR '
            where.append(") "); // close )
        }

        // no where clauses? remove it
        if (where.toString().endsWith("AND ")) {
            where.setLength(where.length() - 4);
        } else if (where.toString().endsWith("WHERE ")) {
            where.setLength(where.length() - 6);
        }

        query.append(where);

        if (pagingRequest.getTimestamp() != null) {
            log.debug("Timestamp: {}", new Timestamp(pagingRequest.getTimestamp()));
        }

        final String filteredCountQuery = query.toString().replaceFirst("\\*", "count(*)");
        final int filteredCount = database.getJdbcTemplate().queryForObject(filteredCountQuery, Integer.class, whereValues.toArray());

        // ORDER BY
        if (!pagingRequest.getOrder().isEmpty()) {
            final StringBuilder order = new StringBuilder("ORDER BY ");
            for (Order o : pagingRequest.getOrder()) {
                List<Column> columns = pagingRequest.getColumns();
                final String columnName = columns.get(o.getColumn()).getData();
                if (!PersonDoc.getStaticHeader().contains(columnName)) {
                    continue; // prevent sql injection
                }
                order.append(columnName);
                order.append(" ");
                order.append(o.getDir().toString());
                order.append(", ");
            }
            order.setLength(order.length() - 2); // remove ', '
            order.append(' '); //add ' '
            query.append(order);
        }

        // LIMIT and OFFSET (Paging)
        final List<String> limitValues = new ArrayList<>();
        if (pagingRequest.getLength() > 0) {
            query.append("LIMIT ? OFFSET ?");
            limitValues.add(Integer.toString(pagingRequest.getLength())); // LIMIT
            limitValues.add(Integer.toString(pagingRequest.getStart())); // OFFSET
        }

        //collect values
        final List<Object> values = new ArrayList<>();
        values.addAll(whereValues);
        values.addAll(limitValues);

        List<Person> ddbIds;
        if (values.isEmpty()) {
            ddbIds = database.getJdbcTemplate().query(query.toString(), new BeanPropertyRowMapper(Person.class));
        } else {
            ddbIds = database.getJdbcTemplate().query(query.toString(), new BeanPropertyRowMapper(Person.class), values.toArray());
        }

        final Page<Person> page = new Page<>(ddbIds);
        ddbIds = null; // free memory
        page.setRecordsFiltered(filteredCount);
        page.setRecordsTotal(totalCount);
        page.setDraw(pagingRequest.getDraw());

        log.debug("Sending page: {}", page);
        return page;
    }

    public Map<String, Timestamp> getTimestamps() {
        try {
            final List<Timestamp> ts = database.getJdbcTemplate().queryForList("SELECT DISTINCT \"timestamp\" FROM main." + tableName, Timestamp.class);
            final Map<String, Timestamp> m = new HashMap<>();
            for (Timestamp t : ts) {
                m.put(sdf.format(t), t);
            }
            return m;
        } catch (EmptyResultDataAccessException e) {
            log.debug("No record found in database for timestamp", e);
            return null;
        }
    }
}
