package de.ddb.labs.ddbid.service;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import de.ddb.labs.ddbid.model.DdbId;
import de.ddb.labs.ddbid.model.DdbIdComparators;
import de.ddb.labs.ddbid.model.paging.Column;
import de.ddb.labs.ddbid.model.paging.Order;
import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

@Slf4j
@Service
public class DdbIdService {

    private static final Comparator<DdbId> EMPTY_COMPARATOR = (e1, e2) -> 0;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Page<DdbId> getDdbIds(PagingRequest pagingRequest) {

        final int totalCount = jdbcTemplate.queryForObject("SELECT count(*) FROM main.\"data\"", Integer.class);

        final StringBuilder order = new StringBuilder("ORDER BY ");
        for (Order o : pagingRequest.getOrder()) {
            List<Column> columns = pagingRequest.getColumns();
            final String columnName = columns.get(o.getColumn()).getData();
            order.append(columnName);
            order.append(" ");
            order.append(o.getDir().toString());
            order.append(", ");
        }
        order.setLength(order.length() - 2);
        order.append(' ');

        final StringBuilder query = new StringBuilder("SELECT * FROM main.\"data\" ");
        if (!pagingRequest.getOrder().isEmpty()) {
            query.append(order.toString());
        }

        List<DdbId> ddbIds;
        if (pagingRequest.getLength() > 0) {
            query.append("LIMIT ? OFFSET ?");
            ddbIds = jdbcTemplate.query(query.toString(),
                    new BeanPropertyRowMapper(DdbId.class),
                    new Object[]{
                        pagingRequest.getLength(), // LIMIT
                        pagingRequest.getStart() // OFFSET
                    });
        } else {
            ddbIds = jdbcTemplate.query(query.toString(), new BeanPropertyRowMapper(DdbId.class));
        }

        final Page<DdbId> page = new Page<>(ddbIds);
        page.setRecordsFiltered(totalCount);
        page.setRecordsTotal(totalCount);
        page.setDraw(pagingRequest.getDraw());

        return page;
    }

    private Page<DdbId> getPage(List<DdbId> ddbIds, PagingRequest pagingRequest) {
        List<DdbId> filtered = ddbIds.stream()
                .sorted(sortDdbIds(pagingRequest))
                .filter(filterDdbIds(pagingRequest))
                .skip(pagingRequest.getStart())
                .limit(pagingRequest.getLength())
                .collect(Collectors.toList());

        long count = ddbIds.stream()
                .filter(filterDdbIds(pagingRequest))
                .count();

        Page<DdbId> page = new Page<>(filtered);
        page.setRecordsFiltered((int) count);
        page.setRecordsTotal((int) count);
        page.setDraw(pagingRequest.getDraw());

        return page;
    }

    private Predicate<DdbId> filterDdbIds(PagingRequest pagingRequest) {
        if (pagingRequest.getSearch() == null || !StringUtils.hasText(pagingRequest.getSearch().getValue())) {
            return ddbId -> true;
        }

        String value = pagingRequest.getSearch()
                .getValue();

        return ddbId -> ddbId.getLabel()
                .toLowerCase()
                .contains(value)
                || ddbId.getId()
                        .toLowerCase()
                        .contains(value)
                || ddbId.getProvider_id()
                        .toLowerCase()
                        .contains(value);
    }

    private Comparator<DdbId> sortDdbIds(PagingRequest pagingRequest) {
        if (pagingRequest.getOrder() == null) {
            return EMPTY_COMPARATOR;
        }

        try {
            Order order = pagingRequest.getOrder()
                    .get(0);

            int columnIndex = order.getColumn();
            Column column = pagingRequest.getColumns()
                    .get(columnIndex);

            Comparator<DdbId> comparator = DdbIdComparators.getComparator(column.getData(), order.getDir());
            if (comparator == null) {
                return EMPTY_COMPARATOR;
            }

            return comparator;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return EMPTY_COMPARATOR;
    }
}
