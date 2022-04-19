/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static de.ddb.labs.ddbid.cronjob.CronJob.API;
import static de.ddb.labs.ddbid.cronjob.CronJob.OK_FILENAME_EXT;
import static de.ddb.labs.ddbid.cronjob.CronJob.OUTPUT_FILENAME_EXT;
import de.ddb.labs.ddbid.model.Doc;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

@Slf4j
public class DataDumper {

    @Autowired
    private OkHttpClient httpClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Value(value = "${ddbid.apikey}")
    private String apiKey;

    public File createNewDump(String query, String dataPath, Class docType) throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        log.info("Start to dump DDB-Ids...");
        final Timestamp currentTime = Timestamp.valueOf(ZonedDateTime.now().toLocalDateTime());
        final Doc docInstance = (Doc) docType.getDeclaredConstructor().newInstance();

        
        final String outputFileNameWithoutExt = dataPath + new SimpleDateFormat("yyyy-MM-dd").format(currentTime);
        final String outputFileName = outputFileNameWithoutExt + OUTPUT_FILENAME_EXT;
        final File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            throw new IllegalStateException("File " + outputFileName + " already exists.");
        }
        int totalCount = -1;
        int processedCount = 0;
        boolean errorOccurred = false;
        try (final OutputStream os = Files.newOutputStream(Path.of(outputFileName), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow); final CSVPrinter outputWriter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            outputWriter.printRecord(docInstance.getHeader());
            log.info("Writing data to dump file {}", outputFileName);
            String lastCursorMark = "";
            String nextCursorMark = "*";
            while (!lastCursorMark.equals(nextCursorMark) && !nextCursorMark.isBlank() && !errorOccurred) {
                // initial request
                final Request request = new Request.Builder()
                        .url(API + query + "&cursorMark=" + URLEncoder.encode(nextCursorMark, StandardCharsets.UTF_8))
                        .addHeader("Accept", "application/json")
                        .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"").build();
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

                    ec = objectMapper.treeToValue(
                            doc.get("response").get("docs"),
                            objectMapper.getTypeFactory().constructCollectionType(List.class, docType)
                    );
                    for (Doc e : ec) {
                        outputWriter.printRecord(e.getData());
                    }
                    processedCount += ec.size();
                    log.info("{} of {} processed...", processedCount, totalCount);
                } finally {
                    // free memory
                    outputWriter.flush();
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
}
