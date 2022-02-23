package de.ddb.labs.ddbid.service;

import java.util.List;
import org.springframework.stereotype.Service;
import de.ddb.labs.ddbid.model.DdbId;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.Doc.Status;
import de.ddb.labs.ddbid.model.paging.Column;
import de.ddb.labs.ddbid.model.paging.Order;
import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
@Service
public class DdbIdService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public Page<DdbId> getDdbIds(PagingRequest pagingRequest) {

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

        final StringBuilder query = new StringBuilder("SELECT * FROM main.\"data\" ");

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
            where.append("timestamp=(SELECT MAX(timestamp) FROM main.\"data\") AND ");
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
        final int totalCount = jdbcTemplate.queryForObject("SELECT count(*) FROM main.\"data\" " + whereClause, Integer.class, whereValues.toArray());
        // totalCount end

        // with search
        if (!pagingRequest.getSearch().getValue().isEmpty()) {
            where.append("(");
            for (String field : Doc.getHeader()) {
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

        query.append(where.toString());

        if (pagingRequest.getTimestamp() != null) {
            log.debug("Timestamp: {}", new Timestamp(pagingRequest.getTimestamp()));
        }

        final String filteredCountQuery = query.toString().replaceFirst("\\*", "count(*)");
        final int filteredCount = jdbcTemplate.queryForObject(filteredCountQuery, Integer.class, whereValues.toArray());

        // ORDER BY
        if (!pagingRequest.getOrder().isEmpty()) {
            final StringBuilder order = new StringBuilder("ORDER BY ");
            for (Order o : pagingRequest.getOrder()) {
                List<Column> columns = pagingRequest.getColumns();
                final String columnName = columns.get(o.getColumn()).getData();
                if (!Doc.getHeader().contains(columnName)) {
                    continue; // prevent sql injection
                }
                order.append(columnName);
                order.append(" ");
                order.append(o.getDir().toString());
                order.append(", ");
            }
            order.setLength(order.length() - 2); // remove ', '
            order.append(' '); //add ' '
            query.append(order.toString());
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

        List<DdbId> ddbIds;
        if (values.isEmpty()) {
            ddbIds = jdbcTemplate.query(query.toString(), new BeanPropertyRowMapper(DdbId.class));
        } else {
            ddbIds = jdbcTemplate.query(query.toString(), new BeanPropertyRowMapper(DdbId.class), values.toArray());
        }

        final Page<DdbId> page = new Page<>(ddbIds);
        page.setRecordsFiltered(filteredCount);
        page.setRecordsTotal(totalCount);
        page.setDraw(pagingRequest.getDraw());

        log.debug("Sending page: {}", page.toString());
        return page;
    }

    public Map<String, Timestamp> getTimestamps() {
        try {
            final List<Timestamp> ts = jdbcTemplate.queryForList("SELECT DISTINCT \"timestamp\" FROM main.\"data\"", Timestamp.class);
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
