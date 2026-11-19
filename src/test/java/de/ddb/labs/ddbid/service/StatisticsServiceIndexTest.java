package de.ddb.labs.ddbid.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import de.ddb.labs.ddbid.database.Database;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

class StatisticsServiceIndexTest {

    private static final String REBUILD_SQL_PREFIX = "CREATE EXTENSION IF NOT EXISTS pg_trgm;";
    private final Database database = mock(Database.class);
    private final JdbcTemplate template = mock(JdbcTemplate.class);
    private final StatisticsService statistics = new StatisticsService(database, null, null, null, null, null);

    @BeforeEach
    void usePostgresIndexMaintenance() {
        when(database.isPostgres()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<Consumer<JdbcTemplate>>getArgument(0).accept(template);
            return null;
        }).when(database).executeInTransaction(any());
        ReflectionTestUtils.setField(statistics, "itemTableName", "item");
        ReflectionTestUtils.setField(statistics, "personTableName", "person");
        ReflectionTestUtils.setField(statistics, "organizationTableName", "organization");
    }

    @Test
    void canceledAdvisoryLockDoesNotRunDdlOrRetry() {
        doThrow(new QueryTimeoutException("Advisory lock wait canceled"))
                .when(template).execute(Database.POSTGRES_MAINTENANCE_LOCK_SQL);

        statistics.ensureDatabaseIndexes();

        verify(database).executeInTransaction(any());
        verify(template).execute(Database.POSTGRES_MAINTENANCE_LOCK_SQL);
        verifyNoMoreInteractions(template);
    }

    @Test
    void failedPostgresDdlStillEnsuresBtreeIndexesInANewTransaction() {
        doThrow(new PermissionDeniedDataAccessException("Cannot create pg_trgm extension", null))
                .when(template).execute(startsWith(REBUILD_SQL_PREFIX));

        statistics.ensureDatabaseIndexes();

        verify(database, times(2)).executeInTransaction(any());
        var statements = inOrder(template);
        statements.verify(template).execute(Database.POSTGRES_MAINTENANCE_LOCK_SQL);
        statements.verify(template).execute(contains(REBUILD_SQL_PREFIX));
        statements.verify(template).execute(Database.POSTGRES_MAINTENANCE_LOCK_SQL);
        statements.verify(template).execute(startsWith("CREATE INDEX IF NOT EXISTS \"item_timestamp\""));
        verifyNoMoreInteractions(template);
    }
}
