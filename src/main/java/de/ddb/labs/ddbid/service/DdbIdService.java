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

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    public Page<DdbId> getDdbIds(PagingRequest pagingRequest) {

        String status = null;
        final List<String> s = Arrays.stream(Status.values()).map(Enum::name).collect(Collectors.toList());
        if (s.contains(pagingRequest.getStatus())) {
            status = pagingRequest.getStatus();
        }

        int totalCount;
        if (status != null) {
            totalCount = jdbcTemplate.queryForObject("SELECT count(*) FROM main.\"data\" WHERE status='" + status + "'", Integer.class);
        } else {
            totalCount = jdbcTemplate.queryForObject("SELECT count(*) FROM main.\"data\"", Integer.class);
        }

        final StringBuilder query = new StringBuilder("SELECT * FROM main.\"data\" ");

        // WHERE (Search)
        final List<String> whereValues = new ArrayList<>();
        if (!pagingRequest.getSearch().getValue().isEmpty()) {
            final StringBuilder where = new StringBuilder("WHERE (");
            for (String field : Doc.getHeader()) {
                if (!field.equals("status")) {
                    where.append(field);
                    where.append(" ILIKE ? OR ");
                    whereValues.add("%" + pagingRequest.getSearch().getValue() + "%");
                }
            }
            where.setLength(where.length() - 3); // remove last 'OR '
            where.append(") "); // close )
            if (status != null) {
                where.append("AND status=? ");
                whereValues.add(status);
            }
            query.append(where.toString());
        } else {
            query.append("WHERE status=? ");
            whereValues.add(status);
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
        final List<String> values = new ArrayList<>();
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

        return page;
    }

    public Map<Timestamp, String> getTimestamps() {
        try {
            final Timestamp[] ts = jdbcTemplate.queryForObject("SELECT DISTINCT \"timestamp\" FROM main.\"data\"", Timestamp[].class);
            final Map<Timestamp, String> m = new HashMap<>();
            for (Timestamp t : ts) {
                m.put(t, sdf.format(t));
            }
            return m;
        } catch (EmptyResultDataAccessException e) {
            log.debug("No record found in database for timestamp", e);
            return null;
        }
    }
}
