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
package de.ddb.labs.ddbid.cronjob.objects;

import de.ddb.labs.ddbid.Application;
import de.ddb.labs.ddbid.cronjob.helper.Helper;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.Type;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import de.ddb.labs.ddbid.model.organization.OrganizationDoc;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class Dump implements Runnable {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
    private static final Pattern DUMP_STAGING_PREFIX = Pattern.compile("\\.ddbid-dump-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}-");
    private static final String QUERY_ITEM = "/search/index/search/select?q=*:*&wt=json&fl=id,provider_item_id,label,provider_id,supplier_id,dataset_id,sector_fct&sort=id ASC";
    private static final String QUERY_PERSON = "/search/index/person/select?q=*:*&wt=json&fl=id,variant_id,preferredName,type&sort=id ASC";
    private static final String QUERY_ORGANIZATION = "/search/index/organization/select?q=*:*&wt=json&fl=id,variant_id,preferredName,type&sort=id ASC";

    @Value(value = "${ddbid.datapath.item}")
    private String dataPathItem;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value(value = "${ddbid.datapath.organization}")
    private String dataPathOrganization;

    @Value("${ddbid.dump.lockfile}")
    private String lockfile;

    @Value("${ddbid.dump.entity-count:50000}")
    private int entityCount;

    @Value("${ddbid.dump.months-to-keep:1}")
    private int monthsToKeepDumps;

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    @Override
    public void run() {
        Path marker = Path.of(lockfile).toAbsolutePath();
        try (FileChannel guard = openGuard(marker); FileLock ownership = tryLock(guard)) {
            if (ownership == null) {
                throw new IllegalStateException("A dump is already running for " + marker);
            }
            Files.createFile(marker);
            try {
                String stagingPrefix = ".ddbid-dump-" + UUID.randomUUID() + "-";
                Files.write(marker, List.of(dtf.format(Instant.now()), stagingPrefix), StandardCharsets.UTF_8);
                dumpAll(LocalDate.now(), stagingPrefix);
            } finally {
                Files.deleteIfExists(marker);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Could not complete DDB ID dumps", ex);
        }
    }

    private void dumpAll(LocalDate date, String stagingPrefix) {
        IllegalStateException failure = new IllegalStateException("Could not complete DDB ID dumps");
        for (Type type : Type.values()) {
            try {
                switch (type) {
                    case ITEM -> createNewDump(QUERY_ITEM, dataPathItem, ItemDoc.class, date, stagingPrefix);
                    case PERSON -> createNewDump(QUERY_PERSON, dataPathPerson, PersonDoc.class, date, stagingPrefix);
                    case ORGANIZATION -> createNewDump(QUERY_ORGANIZATION, dataPathOrganization, OrganizationDoc.class, date, stagingPrefix);
                }
            } catch (IOException | ReflectiveOperationException | RuntimeException e) {
                failure.addSuppressed(new IllegalStateException("Could not dump " + type, e));
            }
        }
        if (failure.getSuppressed().length > 0) {
            throw failure;
        }
    }

    public boolean recoverInterruptedRun() throws IOException {
        Path marker = Path.of(lockfile).toAbsolutePath();
        if (!Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        try (FileChannel guard = openGuard(marker); FileLock ownership = tryLock(guard)) {
            if (ownership == null || !Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)) {
                return false;
            }
            String stagingPrefix;
            try (var reader = Files.newBufferedReader(marker, StandardCharsets.UTF_8)) {
                reader.readLine();
                stagingPrefix = reader.readLine();
            }
            if (stagingPrefix != null && DUMP_STAGING_PREFIX.matcher(stagingPrefix).matches()) {
                for (String dataPath : List.of(dataPathItem, dataPathPerson, dataPathOrganization)) {
                    Path directory = Path.of(dataPath);
                    if (Files.isDirectory(directory)) {
                        try (var staged = Files.newDirectoryStream(directory, stagingPrefix + "*.part")) {
                            for (Path file : staged) {
                                if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                                    Files.deleteIfExists(file);
                                }
                            }
                        }
                    }
                }
            }
            Files.delete(marker);
            return true;
        }
    }

    private static FileChannel openGuard(Path marker) throws IOException {
        Path directory = Objects.requireNonNull(marker.getParent(), "Missing dump lockfile directory");
        Files.createDirectories(directory);
        // Keep the guard inode stable so a new process always locks the same file.
        Path guard = marker.resolveSibling(Objects.requireNonNull(marker.getFileName()) + ".guard");
        return FileChannel.open(guard, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    private static FileLock tryLock(FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }

    public <T extends Doc> File createNewDump(String query, String dataPath, Class<T> docType) throws ReflectiveOperationException, IOException {
        return createNewDump(query, dataPath, docType, LocalDate.now(), ".ddbid-");
    }

    private <T extends Doc> File createNewDump(String query, String dataPath, Class<T> docType, LocalDate date, String stagingPrefix) throws ReflectiveOperationException, IOException {
        Path directory = Files.createDirectories(Path.of(dataPath));
        Path output = directory.resolve(date + Compare.OUTPUT_FILENAME_EXT);
        if (Files.isRegularFile(output) && Files.isRegularFile(Helper.successMarker(output))) {
            log.info("Dump {} already completed", output);
            return output.toFile();
        }
        Path temporary = Helper.createTemporaryFile(directory, stagingPrefix);
        try {
            try (OutputStream stream = Files.newOutputStream(temporary);
                    var writer = new OutputStreamWriter(new GZIPOutputStream(stream), StandardCharsets.UTF_8);
                    CSVPrinter outputWriter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
                outputWriter.printRecord(docType.getDeclaredConstructor().newInstance().getHeader());
                writeDump(query, docType, outputWriter);
            }
            Helper.publish(temporary, output, dtf.format(Instant.now()), stagingPrefix);
            log.info("Wrote complete dump {}", output);
        } finally {
            Files.deleteIfExists(temporary);
        }
        Helper.deleteOlderDumps(dataPath, date.minusMonths(monthsToKeepDumps));
        return output.toFile();
    }

    private <T extends Doc> void writeDump(String query, Class<T> docType, CSVPrinter outputWriter) throws IOException {
        long totalCount = -1;
        long processedCount = 0;
        String cursor = "*";
        while (true) {
            Request request = new Request.Builder()
                    .url(Application.API + query + "&rows=" + entityCount + "&cursorMark=" + URLEncoder.encode(cursor, StandardCharsets.UTF_8))
                    .addHeader("Accept", "application/json")
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful() || response.body() == null) {
                    throw new IOException("DDB API returned HTTP " + response.code() + " for " + request.url());
                }
                JsonNode document = objectMapper.readTree(response.body().byteStream());
                if (document == null) {
                    throw new IOException("DDB API returned an empty response");
                }
                JsonNode result = document.path("response");
                JsonNode count = result.path("numFound");
                JsonNode records = result.path("docs");
                String nextCursor = document.path("nextCursorMark").asString("");
                if (!count.isIntegralNumber() || count.asLong() < 0 || !records.isArray() || nextCursor.isBlank()) {
                    throw new IOException("DDB API returned an invalid dump page");
                }
                if (totalCount < 0) {
                    totalCount = count.asLong();
                }
                if (totalCount != count.asLong() || processedCount + records.size() > totalCount
                        || (records.isEmpty() && !cursor.equals(nextCursor))) {
                    throw new IOException("DDB API changed or returned inconsistent dump results during pagination");
                }
                List<T> entries = objectMapper.treeToValue(records, objectMapper.getTypeFactory().constructCollectionType(List.class, docType));
                for (Doc entry : entries) {
                    outputWriter.printRecord(entry.getData());
                }
                processedCount += entries.size();
                log.info("{} of {} processed", processedCount, totalCount);
                if (cursor.equals(nextCursor)) {
                    break;
                }
                cursor = nextCursor;
            }
        }
        if (processedCount != totalCount) {
            throw new IOException("Expected " + totalCount + " DDB IDs but received " + processedCount);
        }
    }

}
