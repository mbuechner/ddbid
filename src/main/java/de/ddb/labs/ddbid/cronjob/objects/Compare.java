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

import de.ddb.labs.ddbid.cronjob.helper.Helper;
import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Util;
import com.github.davidmoten.bigsorter.Writer;
import de.ddb.labs.ddbid.model.Status;
import de.ddb.labs.ddbid.model.Type;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import de.ddb.labs.ddbid.model.organization.OrganizationDoc;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Compare implements Runnable {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
    public static final String COMPARE_OUTPUT_FILENAME_PREFIX = "CMP_";
    public static final String OUTPUT_FILENAME_EXT = ".csv.gz";
    public static final String OK_FILENAME_EXT = ".txt";
    public static final String DUMP_FILES_PATTERN = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\.csv\\.gz";
    public static final String DUMPOK_FILES_PATTERN = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\.txt";
    public static final String CMP_FILES_PATTERN = "CMP\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}_(MISSING|NEW)\\.csv\\.gz";
    public static final String CMPOK_FILES_PATTERN = "CMP\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}_(MISSING|NEW)\\.txt";
    private static final CSVFormat CSV_WITH_FIRST_RECORD_AS_HEADER = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .get();

    @Value(value = "${ddbid.datapath.item}")
    private String dataPathItem;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value(value = "${ddbid.datapath.organization}")
    private String dataPathOrganization;

    @Override
    public void run() {
        IllegalStateException failure = new IllegalStateException("Could not complete dump comparisons");
        for (Type type : Type.values()) {
            try {
                switch (type) {
                    case ITEM -> compareDumps(dataPathItem, ItemDoc.getStaticHeader());
                    case PERSON -> compareDumps(dataPathPerson, PersonDoc.getStaticHeader());
                    case ORGANIZATION -> compareDumps(dataPathOrganization, OrganizationDoc.getStaticHeader());
                }
            } catch (IOException | RuntimeException e) {
                failure.addSuppressed(new IllegalStateException("Could not compare " + type, e));
            }
        }
        if (failure.getSuppressed().length > 0) {
            throw failure;
        }
    }

    private void compareDumps(String dataPath, List<String> header) throws IOException {
        final File[] dumpFiles = Helper.getOkDumpFiles(dataPath, Comparator.naturalOrder()).toArray(File[]::new);
        final Set<File> cmpFiles = Helper.getOkCmpFiles(dataPath, Comparator.naturalOrder());
        Path directory = Path.of(dataPath);

        for (int i = 0; i < dumpFiles.length - 1; ++i) {
            final File fileA = dumpFiles[i];
            final File fileB = dumpFiles[i + 1];

            final String fileABaseName = fileA.getName().substring(0, fileA.getName().indexOf('.'));
            final String fileBBaseName = fileB.getName().substring(0, fileB.getName().indexOf('.'));
            String prefix = COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_";
            compareIfNeeded(cmpFiles, fileA.toPath(), fileB.toPath(), directory.resolve(prefix + Status.MISSING + OUTPUT_FILENAME_EXT), header, Status.MISSING);
            compareIfNeeded(cmpFiles, fileB.toPath(), fileA.toPath(), directory.resolve(prefix + Status.NEW + OUTPUT_FILENAME_EXT), header, Status.NEW);
        }
    }

    private void compareIfNeeded(Set<File> completed, Path source, Path other, Path output, List<String> header, Status status) throws IOException {
        if (completed.contains(output.toFile())) {
            return;
        }
        Instant sourceDate = completedAt(source);
        Instant otherDate = completedAt(other);
        String timestamp = dtf.format(sourceDate.isAfter(otherDate) ? sourceDate : otherDate);
        Path directory = Objects.requireNonNull(output.toAbsolutePath().getParent(), "Missing comparison directory");
        Path temporary = Helper.createTemporaryFile(directory, ".ddbid-");
        try {
            writeDifferences(source, other, temporary, header, status, timestamp);
            Helper.publish(temporary, output, dtf.format(Instant.now()), ".ddbid-");
            log.info("Wrote {} differences between {} and {} to {}", status, source.getFileName(), other.getFileName(), output);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static Instant completedAt(Path file) throws IOException {
        try (var reader = Files.newBufferedReader(Helper.successMarker(file), StandardCharsets.UTF_8)) {
            String timestamp = reader.readLine();
            if (timestamp == null) {
                throw new IOException("Empty dump completion marker for " + file);
            }
            return dtf.parse(timestamp, Instant::from);
        }
    }

    private static void writeDifferences(Path source, Path other, Path output, List<String> header, Status status, String timestamp) throws IOException {
        Serializer<CSVRecord> serializer = Serializer.csv(CSV_WITH_FIRST_RECORD_AS_HEADER, StandardCharsets.UTF_8);
        Comparator<CSVRecord> byId = Comparator.comparing(record -> record.get("id"));
        try (InputStream sourceStream = Files.newInputStream(source);
                var sourceGzip = new GZIPInputStream(sourceStream);
                Reader<CSVRecord> sourceRecords = serializer.createReader(sourceGzip);
                InputStream otherStream = Files.newInputStream(other);
                var otherGzip = new GZIPInputStream(otherStream);
                Reader<CSVRecord> otherRecords = serializer.createReader(otherGzip);
                OutputStream outputStream = Files.newOutputStream(output);
                var gzip = new GZIPOutputStream(outputStream);
                var writer = new OutputStreamWriter(gzip, StandardCharsets.UTF_8);
                CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            printer.printRecord(header);
            Util.findComplement(sourceRecords, otherRecords, byId, new Writer<CSVRecord>() {
                @Override
                public void write(CSVRecord record) throws IOException {
                    List<String> values = new ArrayList<>(header.size());
                    for (String column : header) {
                        values.add(switch (column) {
                            case "timestamp" -> timestamp;
                            case "status" -> status.toString();
                            default -> record.get(column);
                        });
                    }
                    printer.printRecord(values);
                }

                @Override
                public void flush() throws IOException {
                    printer.flush();
                }

                @Override
                public void close() throws IOException {
                    printer.close();
                }
            });
        }
    }

}
