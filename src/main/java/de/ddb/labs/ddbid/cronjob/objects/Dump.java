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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ddb.labs.ddbid.Application;
import de.ddb.labs.ddbid.cronjob.helper.Helper;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import de.ddb.labs.ddbid.model.organization.OrganizationDoc;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
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

@Slf4j
@Service
@RequiredArgsConstructor
public class Dump implements Runnable {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
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

        try {
            //create lockfile
            Files.write(Path.of(lockfile), List.of(dtf.format(Instant.now())), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            log.warn("Could not write lockfile. {}", ex.getMessage());
        }
        try {
            dumpItem();
        } catch (Exception e) {
            log.error("Dump of items failed", e);
        }

        try {
            dumpPerson();
        } catch (Exception e) {
            log.error("Dump of persons failed", e);
        }

        try {
            dumpOrganization();
        } catch (Exception e) {
            log.error("Dump of organizations failed", e);
        }

        try {
            Files.delete(Path.of(lockfile));
        } catch (IOException ex) {
            log.warn("Could not delete lockfile. {}", ex.getMessage());
        }
    }

    public void dumpItem() {
        try {
            Helper.deleteOlderDumps(dataPathItem, LocalDate.now().minusMonths(monthsToKeepDumps));
            createNewDump(QUERY_ITEM, dataPathItem, ItemDoc.class);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Error while dumping ITEMS. {}", ex.getMessage());
        } catch (IOException ex) {
            log.error("Error while dumping ITEMS.", ex);
        }
    }

    public void dumpPerson() {

        try {
            Helper.deleteOlderDumps(dataPathPerson, LocalDate.now().minusMonths(monthsToKeepDumps));
            createNewDump(QUERY_PERSON, dataPathPerson, PersonDoc.class);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Error while dumping PERSON. {}", ex.getMessage());
        } catch (IOException ex) {
            log.error("Error while dumping PERSON.", ex);
        }
    }

    public void dumpOrganization() {
        try {
            Helper.deleteOlderDumps(dataPathOrganization, LocalDate.now().minusMonths(monthsToKeepDumps));
            createNewDump(QUERY_ORGANIZATION, dataPathOrganization, OrganizationDoc.class);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Error while dumping ORGANIZATION. {}", ex.getMessage());
        } catch (IOException ex) {
            log.error("Error while dumping ORGANIZATION.", ex);
        }
    }

    public <T extends Doc> File createNewDump(String query, String dataPath, Class<T> docType) throws NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException  {

        log.info("Start to dump DDB-Ids...");
        final Doc docInstance = (Doc) docType.getDeclaredConstructor().newInstance();

        final String outputFileNameWithoutExt = dataPath + LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        final String outputFileName = outputFileNameWithoutExt + Compare.OUTPUT_FILENAME_EXT;
        final File outputFile = new File(outputFileName);
        if (outputFile.exists()) {
            throw new IllegalStateException("File " + outputFileName + " already exists.");
        }
        int totalCount = -1;
        int processedCount = 0;
        boolean errorOccurred = false;
        try (final OutputStream os = Files.newOutputStream(Path.of(outputFileName), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow); final CSVPrinter outputWriter = new CSVPrinter(bw, CSVFormat.DEFAULT)) {
            outputWriter.printRecord(docInstance.getHeader());
            log.info("Writing data to dump file {}", outputFileName);
            String lastCursorMark = "";
            String nextCursorMark = "*";
            while (!lastCursorMark.equals(nextCursorMark) && !nextCursorMark.isBlank() && !errorOccurred) {
                // initial request
                final Request request = new Request.Builder()
                        .url(Application.API + query + "&rows=" + entityCount + "&cursorMark=" + URLEncoder.encode(nextCursorMark, StandardCharsets.UTF_8))
                        .addHeader("Accept", "application/json")
                        .build();
                log.info("Execute request \"{}\"", request.url());
                JsonNode doc;
                List<T> ec;
                try (final Response response = httpClient.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        errorOccurred = true;
                        log.warn("API response code {} for {}", response.code(), response);
                        break;
                    }
                    final var body = response.body();
                    if (body == null) {
                        errorOccurred = true;
                        log.warn("Empty response body for {}", request.url());
                        break;
                    }
                    doc = objectMapper.readTree(body.byteStream());
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
                    outputWriter.flush();
                }
                // for testing
                // break;
            }
        } catch (IOException e) {
            errorOccurred = true;
            log.error("{}", e.getMessage());
        }

        if (totalCount > processedCount) {
            log.warn("Total object count is {}, but processed object count is only {}", totalCount, processedCount);
            errorOccurred = true;
        }

        if (errorOccurred) {
            Files.delete(Path.of(outputFileName));
            log.warn("An error occurred and the process was stopped. Corrupt dump {} was deleted, too.", outputFileName);
            throw new RuntimeException("An error occurred while processing the dump");
        } else {
            // write OK file
            Files.write(Path.of(outputFileNameWithoutExt + Compare.OK_FILENAME_EXT), List.of(dtf.format(Instant.now())), StandardCharsets.UTF_8);
            log.info("Wrote successful data to dump file {}", outputFileName);
        }
        return outputFile;
    }

}
