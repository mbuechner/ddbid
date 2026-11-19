package de.ddb.labs.ddbid.service;

import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.item.Item;
import de.ddb.labs.ddbid.model.paging.Direction;
import de.ddb.labs.ddbid.model.paging.Order;
import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import de.ddb.labs.ddbid.model.paging.Search;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataTableQueryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataTableQueryTest.class);
    private static final List<String> FIELDS = List.of("timestamp", "id", "status", "label");
    private static final Timestamp SNAPSHOT = Timestamp.valueOf("2026-11-19 11:12:13");

    private final Database database = new Database("h2", "jdbc:h2:mem:tables", null, null, null, 30);

    @BeforeEach
    void createTable() {
        database.getJdbcTemplate().execute("""
                CREATE TABLE "item" (
                    "timestamp" TIMESTAMP NOT NULL,
                    "id" VARCHAR(32) NOT NULL,
                    "status" VARCHAR(16) NOT NULL,
                    "label" VARCHAR(128)
                )
                """);
    }

    @AfterEach
    void closeDatabase() {
        database.close();
    }

    @Test
    void emptyTableReturnsAnEmptyDefaultPage() {
        assertNull(latestTimestamp());
        assertEquals(0, page(new PagingRequest()).getRecordsTotal());
    }

    @Test
    void globalSearchTreatsLikeCharactersAsTextAndToleratesMissingSortEntries() {
        insert("first", "Part_1");
        insert("second", "PartX1");

        PagingRequest request = new PagingRequest();
        request.setColumns(Collections.singletonList(null));
        request.setOrder(Arrays.asList(null, new Order(0, Direction.asc)));
        Search search = new Search();
        search.setValue("Part_1");
        request.setSearch(search);

        Page<Item> result = page(request);
        assertEquals(2, result.getRecordsTotal());
        assertEquals(1, result.getRecordsFiltered());
        assertEquals("first", result.getData().getFirst().getId());
    }

    @Test
    void malformedCalendarDateReturnsAnEmptyPage() {
        insert("first", "Part");
        PagingRequest request = new PagingRequest();
        request.setTimestamp("2026-02-31 (CW9)");
        assertTrue(page(request).getData().isEmpty());
    }

    @Test
    void offsetTimestampAndEpochReferToTheSameSnapshot() {
        Timestamp expected = Timestamp.from(Instant.parse("2026-11-19T09:12:13Z"));
        assertEquals(expected, DataTableTimestampHelper.filter("2026-11-19T11:12:13+02:00", null).startInclusive());
        assertEquals(expected, DataTableTimestampHelper.filter("2026-11-19 11:12:13+0200", null).startInclusive());
        assertEquals(expected, DataTableTimestampHelper.filter(Long.toString(expected.getTime()), null).startInclusive());
        assertEquals(SNAPSHOT, DataTableTimestampHelper.filter("2026-11-19 11:12:13", null).startInclusive());
    }

    @Test
    void unboundedPageLengthReturnsAtMostTheSupportedMaximum() {
        database.getJdbcTemplate().execute("""
                INSERT INTO "item"
                SELECT TIMESTAMP '2026-11-19 11:12:13', CAST(X AS VARCHAR), 'MISSING', 'Part'
                FROM SYSTEM_RANGE(1, 5001)
                """);

        PagingRequest request = new PagingRequest();
        request.setLength(-1);
        request.setStart(-1);
        Page<Item> result = page(request);

        assertEquals(5001, result.getRecordsTotal());
        assertEquals(5000, result.getData().size());
    }

    @Test
    void dropIndexesRemovesBothCurrentAndLegacyIndexes() {
        database.getJdbcTemplate().execute("CREATE INDEX \"item_status\" ON \"item\"(\"status\")");
        database.getJdbcTemplate().execute("CREATE INDEX \"item_timestamp\" ON \"item\"(\"timestamp\")");

        StatisticsService statistics = new StatisticsService(database, null, null, null, null, null);
        statistics.dropDatabaseIndexes();

        List<String> indexes = database.getJdbcTemplate().queryForList(
                "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = ?",
                String.class, "item");
        assertTrue(indexes.isEmpty(), () -> "Indexes still present: " + indexes);
    }

    @Test
    void databaseTransactionRollsBackWhenWorkFails() {
        assertThrows(IllegalStateException.class, () -> database.executeInTransaction(template -> {
            insert("first", "Part");
            throw new IllegalStateException("Abort test transaction");
        }));
        assertEquals(0, page(new PagingRequest()).getRecordsTotal());
    }

    private Timestamp latestTimestamp() {
        return DataTableTimestampSqlHelper.latestTimestamp(LOGGER, database, "item", "item");
    }

    private Page<Item> page(PagingRequest request) {
        return DataTablePageQueryHelper.page(LOGGER, database, "item", "item", FIELDS, Item.class,
                request, this::latestTimestamp);
    }

    private void insert(String id, String label) {
        database.getJdbcTemplate().update("INSERT INTO \"item\" VALUES (?, ?, ?, ?)", SNAPSHOT, id, "MISSING", label);
    }
}
