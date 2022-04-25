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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static de.ddb.labs.ddbid.Application.API;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.OK_FILENAME_EXT;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.OUTPUT_FILENAME_EXT;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import de.ddb.labs.ddbid.model.organization.OrganizationDoc;
import de.ddb.labs.ddbid.model.person.PersonDoc;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Dump implements Runnable {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
    public static final int ENTITYCOUNT = 100000; // count of entities per query
    private static final String QUERY_ITEM = "/search/index/search/select?q=*:*&wt=json&fl=id,provider_item_id,label,provider_id,supplier_id,dataset_id,sector_fct&sort=id ASC&rows=" + ENTITYCOUNT;
    private static final String QUERY_PERSON = "/search/index/person/select?q=*:*&wt=json&fl=id,variant_id,preferredName,type&sort=id ASC&rows=" + ENTITYCOUNT;
    private static final String QUERY_ORGANIZATION = "/search/index/organization/select?q=*:*&wt=json&fl=id,variant_id,preferredName,type&sort=id ASC&rows=" + ENTITYCOUNT;

    @Value(value = "${ddbid.datapath.item}")
    private String dataPathItem;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value(value = "${ddbid.datapath.organization}")
    private String dataPathOrganization;

    @Value("${ddbid.dump.lockfile}")
    private String lockfile;

    @Autowired
    private OkHttpClient httpClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Value(value = "${ddbid.apikey}")
    private String apiKey;

    @Override
    public void run() {

        try {
            //create lockfile
            Files.write(Path.of(lockfile), List.of(dtf.format(Instant.now())), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            log.warn("Could not wrte lockfile. {}", ex.getMessage());
        }
        try {
            dumpItem();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }

        try {
            dumpPerson();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }

        try {
            dumpOrganization();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }

        try {
            Files.delete(Path.of(lockfile));
        } catch (IOException ex) {
            log.warn("Could not delete lockfile. {}", ex.getMessage());
        }
    }

    public void dumpItem() {
        try {
            createNewDump(QUERY_ITEM, dataPathItem, ItemDoc.class);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Error while dumping ITEMS. {}", ex.getMessage());
        } catch (IOException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void dumpPerson() {

        try {
            createNewDump(QUERY_PERSON, dataPathPerson, PersonDoc.class);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Error while dumping PERSON. {}", ex.getMessage());
        } catch (IOException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void dumpOrganization() {
        try {
            createNewDump(QUERY_ORGANIZATION, dataPathOrganization, OrganizationDoc.class);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            log.error("Error while dumping ORGANIZATION. {}", ex.getMessage());
        } catch (IOException ex) {
            Logger.getLogger(Dump.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "I don't understand the problem, SpotBug!")
    public File createNewDump(String query, String dataPath, Class docType) throws NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException  {

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
                    // System.gc();
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
            Files.write(Path.of(outputFileNameWithoutExt + OK_FILENAME_EXT), List.of(dtf.format(Instant.now())), StandardCharsets.UTF_8);
            log.info("Wrote successfull data to dump file {}", outputFileName);
        }
        return outputFile;
    }

}
