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
package de.ddb.labs.ddbid.cronjob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Util;
import com.github.davidmoten.bigsorter.Writer;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.Status;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
public class CronJob<ItemDoc, PersonDoc, OrganizationDoc> {

    protected static final String API = "https://api.deutsche-digitale-bibliothek.de";
    protected static final String COMPARE_OUTPUT_FILENAME_PREFIX = "CMP_";
    protected static final String OUTPUT_FILENAME_EXT = ".csv.gz";
    protected static final String OK_FILENAME_EXT = ".txt";
    protected static final int ENTITYCOUNT = 500000; // count of entities per query
    private final Class<Doc> docType;
    private final Doc doc;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Setter
    private String query; // set in child class!
    @Setter
    private String dataPath;  // set in child class!
    @Setter
    private String tableName;  // set in child class!

    @Autowired
    private Database database;
    @Value(value = "${ddbid.apikey}")
    private String apiKey;
    @Autowired
    private OkHttpClient httpClient;
    @Autowired
    private ObjectMapper objectMapper;
    private Timestamp currentTime;
    
    /**
     *
     * @param docType
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    public CronJob(Class<Doc> docType) throws NoSuchMethodException, InstantiationException, InvocationTargetException, IllegalArgumentException, IllegalAccessException {
        this.docType = docType;
        this.doc = docType.getDeclaredConstructor().newInstance(); 
    }

    /**
     * Search last dump in directory
     *
     * @return
     */
    private static File lastDumpInDataPath(String dataPath) {
        final String patternString = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}" + OUTPUT_FILENAME_EXT.replaceAll("\\.", "\\\\.");
        final Pattern pattern = Pattern.compile(patternString);
        final FileFilter fileFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                final String fileName = pathname.getName();
                if (pattern.matcher(fileName).matches()) {
                    return true;
                }
                return false;
            }
        };
        final File[] files = new File(dataPath).listFiles(fileFilter);
        Arrays.sort(files, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                final File f1 = (File) o1;
                final File f2 = (File) o2;
                final String s1 = f1.getName().replace(OUTPUT_FILENAME_EXT, "");
                final String s2 = f2.getName().replace(OUTPUT_FILENAME_EXT, "");
                return s1.compareTo(s2);
            }
        });
        if (files.length < 1) {
            return null;
        }
        log.info("Last dump is {} in {}", files[files.length - 1], dataPath);
        return files[files.length - 1];
    }

    /**
     * Serach and delete invalid dumps. Invalid dumps don't have an OK file or
     * are not valid Gzip files
     *
     * @return
     */
    private static void cleanInvalidDumps(String dataPath) {
        // dump files
        final String dumpPatternString = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}" + OUTPUT_FILENAME_EXT.replaceAll("\\.", "\\\\.");
        final Pattern dumpPattern = Pattern.compile(dumpPatternString);
        final FileFilter dumpFileFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                final String fileName = pathname.getName();
                if (dumpPattern.matcher(fileName).matches()) {
                    return true;
                }
                return false;
            }
        };
        final File[] dumpfiles = new File(dataPath).listFiles(dumpFileFilter);

        //ok files
        final String okfilePatternString = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}" + OK_FILENAME_EXT.replaceAll("\\.", "\\\\.");
        final Pattern okfilePattern = Pattern.compile(okfilePatternString);
        final FileFilter okfileFileFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                final String fileName = pathname.getName();
                if (okfilePattern.matcher(fileName).matches()) {
                    return true;
                }
                return false;
            }
        };
        final File[] okfilefiles = new File(dataPath).listFiles(okfileFileFilter);

        // look for ok files ov every dump
        for (File dump : dumpfiles) {
            final String dumpFileName = dump.getName().replace(OUTPUT_FILENAME_EXT, OK_FILENAME_EXT);
            if (!Arrays.stream(okfilefiles).map(p -> p.getName()).anyMatch(dumpFileName::equals)) {
                log.warn("{} is a corrupt data dump and was deleted", dump.getAbsoluteFile());
                if (!dump.delete()) {
                    dump.deleteOnExit();
                }
            }
        }

    }

    /**
     * @throws java.io.IOException
     */
    public void schedule() throws IOException, IllegalArgumentException, RuntimeException {
        
        this.currentTime = Timestamp.from(LocalDateTime.now().toInstant(ZoneOffset.UTC));

        if (this.query == null || this.query.isBlank()) {
            throw new IllegalArgumentException("Query parameter not set");
        }
        if (dataPath == null || dataPath.isBlank()) {
            throw new IllegalArgumentException("DataPath parameter not set");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("TableName parameter not set");
        }

        // we clean up first
        cleanInvalidDumps(dataPath);

        // make new dump
        final File newDumpinDataPath = dumpIds();

        final File lastDumpInDataPath = lastDumpInDataPath(dataPath); // get filename of last dump
        if (lastDumpInDataPath == null) {
            log.warn("There's no last dump in path {}. Nothing to compare.", dataPath);
            return;
        }
        final String fileABaseName = lastDumpInDataPath.getName().substring(0, lastDumpInDataPath.getName().indexOf('.'));
        final String fileBBaseName = newDumpinDataPath.getName().substring(0, newDumpinDataPath.getName().indexOf('.'));

        final File outputFileNameAB = new File(dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.MISSING + OUTPUT_FILENAME_EXT);
        final int diffCountAB = findDifferences(lastDumpInDataPath, newDumpinDataPath, outputFileNameAB, currentTime, Status.MISSING);
        if (diffCountAB > 0) {
            database.getJdbcTemplate().execute("COPY main." + tableName + " FROM '" + outputFileNameAB + "' (AUTO_DETECT TRUE);");
        }
        if(!outputFileNameAB.delete()) {
            outputFileNameAB.deleteOnExit();
        }

        final File outputFileNameBA = new File(dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.NEW + OUTPUT_FILENAME_EXT);
        final int diffCountBA = findDifferences(newDumpinDataPath, lastDumpInDataPath, outputFileNameBA, currentTime, Status.NEW);
        if (diffCountBA > 0) {
            database.getJdbcTemplate().execute("COPY main." + tableName + " FROM '" + outputFileNameBA + "' (AUTO_DETECT TRUE);");
        }
        if(!outputFileNameBA.delete()) {
            outputFileNameBA.deleteOnExit();
        }
    }

    /**
     * @return @throws InterruptedException
     * @throws IOException
     */
    private File dumpIds() throws IOException {
        log.info("Start to dump DDB-Ids...");
        final String outputFileNameWithoutExt = dataPath + new SimpleDateFormat("yyyy-MM-dd").format(currentTime);
        final String outputFileName = outputFileNameWithoutExt + OUTPUT_FILENAME_EXT;
        final File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            throw new IllegalStateException("File " + outputFileName + " already exists.");
        }
        int totalCount = -1;
        int processedCount = 0;
        boolean errorOccurred = false;
        try (final OutputStream os = Files.newOutputStream(Path.of(outputFileName), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
             final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow);
            final CSVPrinter outputWriter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            outputWriter.printRecord(doc.getHeader());
            log.info("Writing data to dump file {}", outputFileName);
            String lastCursorMark = "";
            String nextCursorMark = "*";
            while (!lastCursorMark.equals(nextCursorMark) && !nextCursorMark.isBlank() && !errorOccurred) {
                // initial request
                final Request request = new Request.Builder().url(API + query + "&cursorMark=" + URLEncoder.encode(nextCursorMark, StandardCharsets.UTF_8)).addHeader("Accept", "application/json").addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"").build();
                log.info("Execute request \"{}\"", request.url());
                JsonNode doc;
                List<Doc> ec;
                try (final Response response = httpClient.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        errorOccurred = true;
                        log.warn("API respose code {} for {}", response.code(), response);
                        break;
                    }
                    doc = objectMapper.readTree(response.body().byteStream());
                    if (totalCount == -1) {
                        totalCount = doc.get("response").get("numFound").asInt(0);
                    }
                    // set cursorMarks
                    lastCursorMark = nextCursorMark;
                    nextCursorMark = doc.get("nextCursorMark").asText("");

                    ec = objectMapper.treeToValue(doc.get("response").get("docs"), objectMapper.getTypeFactory().constructCollectionType(List.class, docType));
                    for (Doc e : ec) {
                        outputWriter.printRecord(e.getData());
                    }
                    processedCount += ec.size();
                    log.info("{} of {} processed...", processedCount, totalCount);
                } finally {
                    // free memory
                    doc = null;
                    ec = null;
                    System.gc();
                }
                // for testing
                // break;
            }
        } catch (Exception e) {
            errorOccurred = true;
            log.error("{}", e.getMessage());
        }

        if (totalCount > processedCount) {
            log.warn("Total object count is {}, but processed object count is only {}", totalCount, processedCount);
            errorOccurred = true;
        }

        if (errorOccurred) {
            Files.delete(Path.of(outputFileName));
            log.warn("An error occured and the process was stopped. Corrupt dump {} was deleted, too.", outputFileName);
            throw new RuntimeException("An error occured while processing the dump");
        } else {
            // write OK file
            Files.write(Path.of(outputFileNameWithoutExt + OK_FILENAME_EXT), List.of("OK"), StandardCharsets.UTF_8);
            log.info("Wrote successfull data to dump file {}", outputFileName);
        }
        return outputFile;
    }

    private int findDifferences(File fileA, File fileB, File output, Timestamp timestamp, Status status) throws FileNotFoundException, IOException {
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
        try (final InputStream fileStream = new FileInputStream(tmpFile); final InputStream gzipStream = new GZIPInputStream(fileStream);
             final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
             final OutputStream os = Files.newOutputStream(Path.of(output.getAbsolutePath()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
             final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow);
             final CSVPrinter csvPrinter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            csvPrinter.printRecord(doc.getHeader());
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(decoder);
            for (CSVRecord record : records) {
                final Map<String, String> map = record.toMap();
                map.put("timestamp", sdf.format(timestamp));
                map.put("status", status.toString());
                csvPrinter.printRecord(map.values());
                lineCount++;
            }
            records= null; // free memory
            System.gc();
        }
        if(tmpFile.delete()) {
            tmpFile.deleteOnExit();
        }
        if (lineCount < 1) {
            try {
                Files.delete(Path.of(output.getAbsolutePath()));
            } catch (IOException e) {
                //nothing
            }
        }
        log.info("{} compared with {} has {} differences with status {}", fileA.getName(), fileB.getName(), lineCount, status);
        return lineCount;
    }
}
