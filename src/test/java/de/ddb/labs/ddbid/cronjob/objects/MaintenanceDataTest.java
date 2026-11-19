package de.ddb.labs.ddbid.cronjob.objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import de.ddb.labs.ddbid.cronjob.helper.Helper;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import de.ddb.labs.ddbid.service.ItemService;
import de.ddb.labs.ddbid.service.OrganizationService;
import de.ddb.labs.ddbid.service.PersonService;
import de.ddb.labs.ddbid.service.StatisticsService;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.json.JsonMapper;

class MaintenanceDataTest {

    private static final String STAMP = "2026-07-02T12:00:00+0000";
    private static final String ITEM_HEADER = String.join(",", ItemDoc.getStaticHeader()) + "\n";

    @TempDir
    Path directory;

    @Test
    void retriesAnIncompleteDumpWithoutPublishingIt() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(chain -> {
            String json = switch (calls.incrementAndGet()) {
                case 1 -> "{\"response\":{\"numFound\":1,\"docs\":[]}}";
                case 2 -> "{\"response\":{\"numFound\":1,\"docs\":[{\"id\":\"one\"}]},\"nextCursorMark\":\"page-two\"}";
                default -> "{\"response\":{\"numFound\":1,\"docs\":[]},\"nextCursorMark\":\"page-two\"}";
            };
            return new Response.Builder().request(chain.request()).protocol(Protocol.HTTP_1_1)
                    .code(200).message("OK").body(ResponseBody.create(json, MediaType.get("application/json"))).build();
        }).build();
        Dump dump = new Dump(client, JsonMapper.builder().build());
        ReflectionTestUtils.setField(dump, "entityCount", 50);
        ReflectionTestUtils.setField(dump, "monthsToKeepDumps", 1);

        assertThatThrownBy(() -> dump.createNewDump("/items?wt=json", directory.toString(), ItemDoc.class))
                .isInstanceOf(IOException.class);
        try (var files = Files.list(directory)) {
            assertThat(files).isEmpty();
        }

        var file = dump.createNewDump("/items?wt=json", directory.toString(), ItemDoc.class);
        assertThat(Files.isRegularFile(Helper.successMarker(file.toPath()))).isTrue();
        assertThat(readGzip(file.toPath())).contains(",one,");
        assertThat(dump.createNewDump("/items?wt=json", directory.toString(), ItemDoc.class)).isEqualTo(file);
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void attemptsEveryDumpAndReusesSuccessesOnRetry() throws Exception {
        Map<String, Integer> calls = new HashMap<>();
        OkHttpClient client = new OkHttpClient.Builder().addInterceptor(chain -> {
            String endpoint = chain.request().url().pathSegments().get(3);
            int attempt = calls.merge(endpoint, 1, Integer::sum);
            int status = attempt == 1 && !endpoint.equals("person") ? 503 : 200;
            String page = "{\"response\":{\"numFound\":0,\"docs\":[]},\"nextCursorMark\":\"*\"}";
            return new Response.Builder().request(chain.request()).protocol(Protocol.HTTP_1_1)
                    .code(status).message("test").body(ResponseBody.create(page, MediaType.get("application/json"))).build();
        }).build();
        Dump dump = new Dump(client, JsonMapper.builder().build());
        configurePaths(dump, directory.resolve("item"));
        ReflectionTestUtils.setField(dump, "entityCount", 50);
        ReflectionTestUtils.setField(dump, "monthsToKeepDumps", 1);
        Path marker = directory.resolve("dump.lock");
        ReflectionTestUtils.setField(dump, "lockfile", marker.toString());

        Files.writeString(marker, STAMP);
        assertThatThrownBy(dump::run).isInstanceOf(IllegalStateException.class);
        assertThat(Files.readString(marker)).isEqualTo(STAMP);
        assertThat(calls).isEmpty();
        Files.delete(marker);

        assertThatThrownBy(dump::run).isInstanceOf(IllegalStateException.class)
                .satisfies(error -> assertThat(error.getSuppressed()).extracting(Throwable::getMessage)
                        .containsExactly("Could not dump ITEM", "Could not dump ORGANIZATION"));
        assertThat(marker).doesNotExist();
        String date = LocalDate.now().toString();
        assertThat(directory.resolve("person").resolve(date + ".txt")).exists();
        assertThat(directory.resolve("item").resolve(date + ".txt")).doesNotExist();

        dump.run();
        assertThat(calls).containsExactlyInAnyOrderEntriesOf(Map.of("search", 2, "person", 1, "organization", 2));
        assertThat(directory.resolve("item").resolve(date + ".txt")).exists();
        assertThat(directory.resolve("organization").resolve(date + ".txt")).exists();
    }

    @Test
    void recoversOnlyStagingFromAnInterruptedDump() throws Exception {
        Dump dump = new Dump(new OkHttpClient(), JsonMapper.builder().build());
        Path items = Files.createDirectory(directory.resolve("item"));
        configurePaths(dump, items);
        Path marker = directory.resolve("dump.lock");
        ReflectionTestUtils.setField(dump, "lockfile", marker.toString());
        Path guard = directory.resolve("dump.lock.guard");
        String stagingPrefix = ".ddbid-dump-22e1503c-c354-4d6f-a0a4-c9b3750dbdb2-";
        Path stagingFile = Files.createFile(items.resolve(stagingPrefix + "123.part"));
        Path stagingMarker = Files.createFile(items.resolve(stagingPrefix + "456.part"));
        Path otherRun = Files.createFile(items.resolve(".ddbid-dump-a59920c9-0981-4899-b456-e3439a317ef2-123.part"));
        Path unrelated = Files.createFile(items.resolve(".ddbid-unrelated.part"));
        Files.write(marker, List.of(STAMP, stagingPrefix), StandardCharsets.UTF_8);

        try (FileChannel channel = FileChannel.open(guard, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                var active = channel.lock()) {
            assertThat(dump.recoverInterruptedRun()).isFalse();
            assertThat(marker).exists();
            assertThat(stagingFile).exists();
            assertThat(stagingMarker).exists();
        }

        assertThat(dump.recoverInterruptedRun()).isTrue();
        assertThat(marker).doesNotExist();
        assertThat(stagingFile).doesNotExist();
        assertThat(stagingMarker).doesNotExist();
        assertThat(otherRun).exists();
        assertThat(unrelated).exists();
        assertThat(guard).exists();

        Files.writeString(marker, STAMP);
        assertThat(dump.recoverInterruptedRun()).isTrue();
        assertThat(otherRun).exists();
        assertThat(unrelated).exists();
    }

    @Test
    void comparesSnapshotsByHeaderAndPublishesBothDirections() throws Exception {
        Path itemDirectory = Files.createDirectory(directory.resolve("item"));
        String reorderedHeader = "id,timestamp,status,provider_item_id,dataset_id,label,provider_id,sector_fct,supplier_id\n";
        writeCompleteFile(itemDirectory.resolve("2026-07-01.csv.gz"), reorderedHeader + "gone,,,,,old label,,,\n");
        writeCompleteFile(itemDirectory.resolve("2026-07-02.csv.gz"), reorderedHeader + "arrived,,,,,new label,,,\n");
        Compare compare = new Compare();
        configurePaths(compare, itemDirectory);

        compare.run();

        var compared = Helper.getOkCmpFiles(itemDirectory.toString(), Comparator.naturalOrder());
        assertThat(compared).hasSize(2);
        assertThat(readGzip(itemDirectory.resolve("CMP_2026-07-01_2026-07-02_MISSING.csv.gz")))
                .startsWith(ITEM_HEADER.stripTrailing()).contains(",gone,MISSING,,,old label,,,");
        assertThat(readGzip(itemDirectory.resolve("CMP_2026-07-01_2026-07-02_NEW.csv.gz")))
                .contains(",arrived,NEW,,,new label,,,");
    }

    @Test
    void attemptsLaterComparisonsAndReportsAllFailedTypes() throws Exception {
        Path items = Files.createDirectory(directory.resolve("item"));
        Path people = Files.createDirectory(directory.resolve("person"));
        Path organizations = Files.createDirectory(directory.resolve("organization"));
        String entityHeader = "timestamp,id,status,variant_id,preferredName,type\n";
        writeCompleteFile(items.resolve("2026-07-01.csv.gz"), ITEM_HEADER);
        Path broken = items.resolve("2026-07-02.csv.gz");
        Files.writeString(broken, "not gzip");
        Files.writeString(Helper.successMarker(broken), STAMP);
        writeCompleteFile(people.resolve("2026-07-01.csv.gz"), entityHeader);
        writeCompleteFile(people.resolve("2026-07-02.csv.gz"), entityHeader + ",one,,,,\n");
        writeCompleteFile(organizations.resolve("2026-07-01.csv.gz"), entityHeader);
        writeCompleteFile(organizations.resolve("2026-07-02.csv.gz"), entityHeader);
        Files.writeString(organizations.resolve("2026-07-02.txt"), "");
        Compare compare = new Compare();
        configurePaths(compare, items);

        assertThatThrownBy(compare::run).isInstanceOf(IllegalStateException.class)
                .satisfies(error -> assertThat(error.getSuppressed()).extracting(Throwable::getMessage)
                        .containsExactly("Could not compare ITEM", "Could not compare ORGANIZATION"));
        assertThat(Helper.getOkCmpFiles(people.toString(), Comparator.naturalOrder())).hasSize(2);
    }

    @Test
    void rollsBackAllRowsWhenAComparisonCannotBeImported() throws Exception {
        Database database = new Database("h2", "jdbc:h2:mem:maintenance", "", "", "", 10);
        try {
            var tables = new MaintenanceTables("item_test", "person_test", "organization_test");
            Import importer = new Import(database, mock(StatisticsService.class), mock(ItemService.class),
                    mock(PersonService.class), mock(OrganizationService.class), tables);
            Path items = Files.createDirectory(directory.resolve("items"));
            Path people = Files.createDirectory(directory.resolve("person"));
            configurePaths(importer, items);
            importer.importToDb();
            database.getJdbcTemplate().update("INSERT INTO \"item_test\" (\"timestamp\", \"id\", \"status\") VALUES (CURRENT_TIMESTAMP, 'old', 'MISSING')");

            writeCompleteFile(items.resolve("CMP_2026-07-01_2026-07-02_MISSING.csv.gz"), ITEM_HEADER + STAMP + ",new,MISSING,,,,,,\n");
            writeCompleteFile(items.resolve("CMP_2026-07-01_2026-07-02_NEW.csv.gz"), ITEM_HEADER);
            Path personMissing = people.resolve("CMP_2026-07-01_2026-07-02_MISSING.csv.gz");
            Path personNew = people.resolve("CMP_2026-07-01_2026-07-02_NEW.csv.gz");
            writeCompleteFile(personMissing, "bad,header\n");
            String personHeader = "timestamp,id,status,variant_id,preferredName,type\n";
            writeCompleteFile(personNew, personHeader);

            assertThatThrownBy(importer::importToDb).isInstanceOf(IllegalArgumentException.class);
            assertThat(database.getJdbcTemplate().queryForList("SELECT \"id\" FROM \"item_test\"", String.class)).containsExactly("old");

            String personId = "person-1234567890123456789012345678901234567890";
            writeCompleteFile(personMissing, personHeader + STAMP + "," + personId + ",MISSING,,,\n");
            importer.importToDb();
            assertThat(database.getJdbcTemplate().queryForList("SELECT \"id\" FROM \"item_test\"", String.class)).containsExactly("new");
            assertThat(database.getJdbcTemplate().queryForList("SELECT \"id\" FROM \"person_test\"", String.class)).containsExactly(personId);
            importer.importToDb();
            assertThat(database.getJdbcTemplate().queryForList("SELECT \"pkey\" FROM \"item_test\"", Integer.class)).containsExactly(1);
            database.getJdbcTemplate().update("INSERT INTO \"item_test\" (\"timestamp\", \"id\", \"status\") VALUES (CURRENT_TIMESTAMP, 'external', 'MISSING')");
            assertThat(database.getJdbcTemplate().queryForList("SELECT \"pkey\" FROM \"item_test\" ORDER BY \"pkey\"", Integer.class)).containsExactly(1, 2);
        } finally {
            database.close();
        }
    }

    @Test
    void verifiesTheReturnedIdRatherThanTheNumberOfResponseLines() throws Exception {
        assertThat(Correct.hasId(new StringReader("id\nwrong\n"), "expected")).isFalse();
        assertThat(Correct.hasId(new StringReader("id\n\"expected\"\n"), "expected")).isTrue();
        assertThat(Correct.hasId(new StringReader("error\nexpected\n"), "expected")).isFalse();
    }

    private void configurePaths(Object job, Path itemDirectory) {
        ReflectionTestUtils.setField(job, "dataPathItem", itemDirectory.toString());
        ReflectionTestUtils.setField(job, "dataPathPerson", directory.resolve("person").toString());
        ReflectionTestUtils.setField(job, "dataPathOrganization", directory.resolve("organization").toString());
    }

    private static void writeCompleteFile(Path file, String csv) throws IOException {
        try (var writer = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(file)), StandardCharsets.UTF_8)) {
            writer.write(csv);
        }
        Files.writeString(Helper.successMarker(file), STAMP);
    }

    private static String readGzip(Path file) throws IOException {
        try (var stream = new GZIPInputStream(Files.newInputStream(file))) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
