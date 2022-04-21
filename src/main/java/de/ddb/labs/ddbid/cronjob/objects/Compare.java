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
package de.ddb.labs.ddbid.cronjob.objects;

import de.ddb.labs.ddbid.cronjob.helper.Helper;
import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Util;
import com.github.davidmoten.bigsorter.Writer;
import de.ddb.labs.ddbid.model.Status;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import de.ddb.labs.ddbid.model.organization.OrganizationDoc;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

    @Value(value = "${ddbid.datapath.item}")
    private String dataPathItem;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value(value = "${ddbid.datapath.organization}")
    private String dataPathOrganization;

    @Override
    public void run() {
        compareDumps(dataPathItem, ItemDoc.getStaticHeader());
        compareDumps(dataPathPerson, PersonDoc.getStaticHeader());
        compareDumps(dataPathOrganization, OrganizationDoc.getStaticHeader());
    }

    private void compareDumps(String dataPath, List<String> header) {
        // search for uncompared and creates them
        final File[] dumpFiles = Helper.getOkDumpFiles(dataPath, Comparator.naturalOrder()).toArray(File[]::new);
        final Set<File> cmpFiles = Helper.getOkCmpFiles(dataPath, Comparator.naturalOrder());

        for (int i = 0; i < dumpFiles.length - 1; ++i) {
            final File fileA = dumpFiles[i];
            final File fileB = dumpFiles[i + 1];

            final String fileABaseName = fileA.getName().substring(0, fileA.getName().indexOf('.'));
            final String fileBBaseName = fileB.getName().substring(0, fileB.getName().indexOf('.'));
            final String outputBaseFileNameABMissing = dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.MISSING;
            final File outputFileNameABMissing = new File(outputBaseFileNameABMissing + OUTPUT_FILENAME_EXT);
            final String outputBaseFileNameBANew = dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.NEW;
            final File outputFileNameBANew = new File(outputBaseFileNameBANew + OUTPUT_FILENAME_EXT);

            // compare for MISSING
            if (cmpFiles.contains(outputFileNameABMissing)) {
                log.info("{} already exists. Skipping this one.", outputFileNameABMissing);
            } else {
                log.info("{} does not exist. Create compare file...", outputFileNameABMissing);
                try {
                    findDifferences(fileA, fileB, outputFileNameABMissing, header, Status.MISSING);
                } catch (Exception e) {
                    log.error("Error while comparing {} with {} to {}. {}", fileA, fileB, outputFileNameABMissing, e.getMessage());
                    if (!outputFileNameABMissing.delete()) {
                        outputFileNameABMissing.deleteOnExit();
                    }
                    continue;
                }
                // write OK file
                try {
                    Files.write(Path.of(outputBaseFileNameABMissing + OK_FILENAME_EXT), List.of(dtf.format(Instant.now())), StandardCharsets.UTF_8);
                } catch (IOException ex) {
                    log.error("Could not write OK file. {}", ex.getMessage());
                }
            }

            // compare for NEW
            if (cmpFiles.contains(outputFileNameBANew)) {
                log.info("{} already exists. Skipping this one.", outputFileNameBANew);
            } else {

                log.info("{} does not exist. Create compare file...", outputFileNameBANew);
                try {
                    findDifferences(fileB,fileA,  outputFileNameBANew, header, Status.NEW);
                } catch (Exception e) {
                    log.error("Error while comparing {} with {} to {}. {}", fileB,fileA, outputFileNameBANew, e.getMessage());
                    if (!outputFileNameBANew.delete()) {
                        outputFileNameBANew.deleteOnExit();
                    }
                    continue;
                }
                // write OK file
                try {
                    Files.write(Path.of(outputBaseFileNameBANew + OK_FILENAME_EXT), List.of(dtf.format(Instant.now())), StandardCharsets.UTF_8);
                } catch (IOException ex) {
                    log.error("Could not write OK file. {}", ex.getMessage());
                }
            }
        }
    }

    /**
     *
     * @param fileA Older file
     * @param fileB Newer File
     * @param output
     * @param status
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static int findDifferences(File fileA, File fileB, File output, List<String> header, Status status) throws FileNotFoundException, IOException {

        final File okFileA = new File(fileA.getAbsolutePath().replace(OUTPUT_FILENAME_EXT, OK_FILENAME_EXT));
        final File okFileB = new File(fileB.getAbsolutePath().replace(OUTPUT_FILENAME_EXT, OK_FILENAME_EXT));
        
        final List<String> okFileALines = Files.readAllLines(okFileA.toPath(), StandardCharsets.UTF_8);
        final List<String> okFileBLines = Files.readAllLines(okFileB.toPath(), StandardCharsets.UTF_8);
        
        final Instant okFileADate = dtf.parse(okFileALines.get(0), Instant::from);
        final Instant okFileBDate = dtf.parse(okFileBLines.get(0), Instant::from);
        Instant dateForCsv = okFileBDate;

        if (okFileADate.compareTo(okFileBDate) > 0) {
            dateForCsv = okFileADate;
        }

        final File tmpFile = File.createTempFile("ddbid-", "csv.gz");
        final Serializer<CSVRecord> csVSerializer = Serializer.csv(CSVFormat.DEFAULT.withFirstRecordAsHeader(), StandardCharsets.UTF_8);
        final Comparator<CSVRecord> comparator = (x, y) -> {
            final String a = x.get("id");
            final String b = y.get("id");
            return a.compareTo(b);
        };
        try (final InputStream fileStreamA = new FileInputStream(fileA); 
                final GZIPInputStream gzipA = new GZIPInputStream(fileStreamA); 
                final Reader readerA = csVSerializer.createReader(gzipA);
                final InputStream fileStreamB = new FileInputStream(fileB); 
                final GZIPInputStream gzipB = new GZIPInputStream(fileStreamB); 
                final Reader<CSVRecord> readerB = csVSerializer.createReader(gzipB); 
                final OutputStream fileOutputStream = new FileOutputStream(tmpFile, false); 
                final GZIPOutputStream gzOutStream = new GZIPOutputStream(fileOutputStream); 
                final Writer<CSVRecord> writerAb = csVSerializer.createWriter(gzOutStream)) {
            Util.findComplement(readerA, readerB, comparator, writerAb);
        }
        int lineCount = 0;
        try (final InputStream fileStream = new FileInputStream(tmpFile); 
                final InputStream gzipStream = new GZIPInputStream(fileStream); 
                final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
                final OutputStream os = Files.newOutputStream(Path.of(output.getAbsolutePath()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); 
                final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); 
                final BufferedWriter bw = new BufferedWriter(ow);
                final CSVPrinter csvPrinter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            if (header != null) {
                csvPrinter.printRecord(header);
            }
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(decoder);
            for (CSVRecord record : records) {
                final Map<String, String> map = record.toMap();
                map.put("timestamp", dtf.format(dateForCsv));
                if (status != null) {
                    map.put("status", status.toString());
                }
                csvPrinter.printRecord(map.values());
                if (lineCount++ % 1000 == 0) {
                    csvPrinter.flush();
                }
            }
            records = null; // free memory
            System.gc();
        }
        if (tmpFile.delete()) {
            tmpFile.deleteOnExit();
        }
        log.info("{} compared with {} has {} differences with status {}", fileA.getName(), fileB.getName(), lineCount, status);
        return lineCount;
    }

}
