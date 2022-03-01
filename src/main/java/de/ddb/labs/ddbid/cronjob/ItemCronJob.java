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
import de.ddb.labs.ddbid.model.item.ItemDoc;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVPrinter;
import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Util;
import com.github.davidmoten.bigsorter.Writer;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.Status;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

@Slf4j
@Service
public class ItemCronJob implements Runnable {

    private final static int ENTITYCOUNT = 500000; // count of entities per query
    private final static int MAX_NO_OF_THREADS = 1; // max no. of writing theads

    private final static String COMPARE_OUTPUT_FILENAME_PREFIX = "CMP_";
    private final static String OUTPUT_FILENAME_EXT = ".csv.gz";

    private final static String API = "https://api.deutsche-digitale-bibliothek.de";
    private final static String API_QUERY = "/search/index/search/select?q=*:*&wt=json&fl=id,label,provider_id,supplier_id,dataset_id&sort=id ASC&rows=" + ENTITYCOUNT;

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Value("${ddbid.apikey}")
    private String apiKey;
    @Autowired
    private OkHttpClient httpClient;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${ddbid.datapath.item}")
    private String dataPath;

    @Value("${ddbid.database.table.item}")
    private String tableName;
    
        @Autowired
    private Database database;

    // private int reRunCount = 0;
    private final List<Thread> threads = new ArrayList<>();

    private int totalCount = -1;
    private int processedCount = 0;
    private boolean errorOccured = false;
    private CSVPrinter outputWriter;
    private Timestamp currentTime;

    public ItemCronJob() {
        currentTime = Timestamp.from(LocalDateTime.now().toInstant(ZoneOffset.UTC));
    }

    @Override
    @Scheduled(cron = "${ddbid.cron.item}")
    public void run() {

        currentTime = Timestamp.from(LocalDateTime.now().toInstant(ZoneOffset.UTC));
        // get filename of last dump
        try {
            final File lastDumpInDataPath = lastDumpInDataPath();
            final File newDumpinDataPath = dumpIds();

            if (lastDumpInDataPath == null) {
                log.warn("There's no last dump in path {}. Nothing to compare.", dataPath);
                return;
            }
            final String fileABaseName = lastDumpInDataPath.getName().substring(0, lastDumpInDataPath.getName().indexOf('.'));
            final String fileBBaseName = newDumpinDataPath.getName().substring(0, newDumpinDataPath.getName().indexOf('.'));

            final File outputFileNameAB = new File(dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.MISSING + OUTPUT_FILENAME_EXT);
            final int diffCountAB = findDifferences(lastDumpInDataPath, newDumpinDataPath, outputFileNameAB, currentTime, Status.MISSING);
            if (diffCountAB > 0) {
                database.execute("COPY main." + tableName + " FROM '" + outputFileNameAB + "' (AUTO_DETECT TRUE);");
            }

            final File outputFileNameBA = new File(dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.NEW + OUTPUT_FILENAME_EXT);
            final int diffCountBA = findDifferences(newDumpinDataPath, lastDumpInDataPath, outputFileNameBA, currentTime, Status.NEW);
            if (diffCountBA > 0) {
                database.execute("COPY main." + tableName + " FROM '" + outputFileNameBA + "' (AUTO_DETECT TRUE);");
            }
        } catch (Exception e) {
            log.error("Error while processing ID dump. {}", e.getMessage());
            //run(); // re-run
        }
    }

    /**
     *
     * @return
     */
    private File lastDumpInDataPath() {
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
     *
     * @return @throws InterruptedException
     * @throws IOException
     */
    public File dumpIds() throws InterruptedException, IOException {
        log.info("Start to dump DDB-Ids...");
        log.info("API key is {}", apiKey);

        final String outputFileName = dataPath + new SimpleDateFormat("yyyy-MM-dd").format(currentTime) + OUTPUT_FILENAME_EXT;
        final File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            throw new IllegalStateException("File " + outputFileName + " already exists.");
        }
        totalCount = -1;
        processedCount = 0;
        errorOccured = false;

        try (
                final OutputStream os = Files.newOutputStream(Path.of(outputFileName), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow);) {

            outputWriter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            outputWriter.printRecord(ItemDoc.getHeader());

            log.info("Writing data to {}", outputFileName);

            String lastCursorMark = "";
            String nextCursorMark = "*";

            while (!lastCursorMark.equals(nextCursorMark) && !nextCursorMark.isBlank() && !errorOccured) {
                // initial request
                final Request request = new Request.Builder()
                        .url(API + API_QUERY + "&cursorMark=" + URLEncoder.encode(nextCursorMark, Charset.forName("UTF-8")))
                        .addHeader("Accept", "application/json")
                        .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"")
                        .build();

                log.info("Execute request \"{}\"", request.url().toString());

                try ( Response response = httpClient.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        errorOccured = true;
                        log.warn("API respose code {} for {}", response.code(), response.toString());
                        break;
                    }

                    final JsonNode doc = objectMapper.readTree(response.body().byteStream());
                    if (totalCount == -1) {
                        totalCount = doc.get("response").get("numFound").asInt(0);
                    }

                    // set cursorMarks
                    lastCursorMark = nextCursorMark;
                    nextCursorMark = doc.get("nextCursorMark").asText("");

                    // put it in a thread
                    addInThread(doc.get("response").get("docs"));

                }
                // for testing
                // break;
            }
            // wait until finished
            while (cleanUpThreads() > 0);
            outputWriter.close();
        }

        if (errorOccured) {
            log.error("An error occured and the process was stopped. File deleted, too.");
            Files.delete(Path.of(outputFileName));
            throw new IllegalStateException("An error occured while processing the Id dump");
        }

        return new File(outputFileName);
    }

    private int cleanUpThreads() {
        final Iterator it = threads.iterator();
        while (it.hasNext()) {
            final Thread th = (Thread) it.next();
            if (!th.isAlive()) {
                it.remove();
                log.info("Thread {} finished and removed.", th.getName());
            }
        }

        return threads.size();
    }

    private void addInThread(JsonNode docsArray) {

        // wait to add new thread
        while (cleanUpThreads() >= MAX_NO_OF_THREADS);

        final Thread t = new Thread(() -> {
            try {

                final ItemDoc[] ec = objectMapper.treeToValue(docsArray, ItemDoc[].class);

                for (ItemDoc e : ec) {
                    outputWriter.printRecord(e.getData());
                }
                processedCount += ec.length;
                log.info("{} of {} processed...", processedCount, totalCount);
            } catch (Exception e) {
                log.error("{}", e.getMessage());
                errorOccured = true;
            }
        });

        threads.add(t);
        t.start();
        log.info("Thread {} added and started...", t.getName());
    }

    public int findDifferences(File fileA, File fileB, File output, Timestamp timestamp, Status status) throws FileNotFoundException, IOException {

        final File tmpFile = File.createTempFile("ddbid-", "csv.gz");

        final Serializer<CSVRecord> csVSerializer = Serializer.csv(CSVFormat.DEFAULT.withFirstRecordAsHeader(), StandardCharsets.UTF_8);
        final Comparator<CSVRecord> comparator = (x, y) -> {
            final String a = x.get("id");
            final String b = y.get("id");
            return a.compareTo(b);
        };

        try (
                final InputStream fileStreamA = new FileInputStream(fileA); final GZIPInputStream gzipA = new GZIPInputStream(fileStreamA); final Reader readerA = csVSerializer.createReader(gzipA); final InputStream fileStreamB = new FileInputStream(fileB); final GZIPInputStream gzipB = new GZIPInputStream(fileStreamB); final Reader readerB = csVSerializer.createReader(gzipB); final OutputStream fileOutputStream = new FileOutputStream(tmpFile, false); final GZIPOutputStream gzOutStream = new GZIPOutputStream(fileOutputStream); final Writer writerAb = csVSerializer.createWriter(gzOutStream);) {

            // Sorter
            //        .serializer(csVSerializer)
            //        .comparator(comparator)
            //        .input(gzipA)
            //        .output(new File("out.txt"))
            //        .sort();
            Util.findComplement(readerA, readerB, comparator, writerAb);

        }

        int lineCount = 0;
        try (
                final InputStream fileStream = new FileInputStream(tmpFile); final InputStream gzipStream = new GZIPInputStream(fileStream); final InputStreamReader decoder = new InputStreamReader(gzipStream, Charset.forName("UTF-8")); //
                 final OutputStream os = Files.newOutputStream(Path.of(output.getAbsolutePath()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow); final CSVPrinter csvPrinter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader());) {

            csvPrinter.printRecord(ItemDoc.getHeader());

            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(decoder);

            for (CSVRecord record : records) {
                final Map<String, String> map = record.toMap();
                map.put("timestamp", sdf.format(timestamp));
                map.put("status", status.toString());
                csvPrinter.printRecord(map.values());
                lineCount++;
            }
        }

        tmpFile.delete();

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
