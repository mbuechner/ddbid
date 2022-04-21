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
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.Type;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Import implements Runnable {

    private final static String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS main;";
    private final static String DROP_TABLE = "DROP TABLE IF EXISTS main.item";
    private final static String CREATE_ITEM_TABLE = """
                                                    CREATE TABLE IF NOT EXISTS main.item(
                                                    "timestamp" TIMESTAMP NOT NULL,
                                                    id VARCHAR(32) NOT NULL,
                                                    status VARCHAR(16) NOT NULL,
                                                    dataset_id VARCHAR(128),
                                                    label VARCHAR(1024),
                                                    provider_id VARCHAR(256),
                                                    supplier_id VARCHAR(128),
                                                    provider_item_id VARCHAR(512),
                                                    sector_fct VARCHAR(16),
                                                    PRIMARY KEY ("timestamp", id)
                                                    );""";

    private final static String CREATE_PERSON_TABLE = """
                                                      CREATE TABLE IF NOT EXISTS main.person(
                                                      "timestamp" TIMESTAMP NOT NULL,
                                                      id VARCHAR(64) NOT NULL,
                                                      status VARCHAR(16) NOT NULL,
                                                      variant_id VARCHAR(256),
                                                      preferredName VARCHAR(1024),
                                                      type VARCHAR(32),
                                                      PRIMARY KEY ("timestamp", id)
                                                      );""";

    private final static String CREATE_ORGANIZATION_TABLE = """
                                                            CREATE TABLE IF NOT EXISTS main.organization(
                                                            "timestamp" TIMESTAMP NOT NULL,
                                                            id VARCHAR(64) NOT NULL,
                                                            status VARCHAR(16) NOT NULL,
                                                            variant_id VARCHAR(256),
                                                            preferredName VARCHAR(1024),
                                                            type VARCHAR(32),
                                                            PRIMARY KEY ("timestamp", id)
                                                            );""";

    @Autowired
    private Database database;

    @Value("${ddbid.datapath.item}")
    private String dataPathItem;

    @Value("${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value("${ddbid.datapath.organization}")
    private String dataPathOrganization;

    @Override
    public void run() {
        try {
            importToDb();
        } catch (IOException ex) {
            Logger.getLogger(Import.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void importToDb() throws IOException {

        database.delete(); // delete DB file
        database.getJdbcTemplate().execute(CREATE_SCHEMA); // create database (only inital)
        database.getJdbcTemplate().execute(DROP_TABLE); // drop table
        database.getJdbcTemplate().execute(CREATE_ITEM_TABLE); // create table
        database.getJdbcTemplate().execute(CREATE_PERSON_TABLE);
        database.getJdbcTemplate().execute(CREATE_ORGANIZATION_TABLE);
        database.commit();

        for (Type t : Type.values()) {
            switch (t) {
                case ITEM -> importTypes(t, dataPathItem);
                case PERSON -> importTypes(t, dataPathPerson);
                case ORGANIZATION -> importTypes(t, dataPathOrganization);
                default -> {
                }
            }

        }

    }

    private void importTypes(Type type, String dataPath) throws FileNotFoundException, IOException {
        final Set<File> filesItem = Helper.getOkCmpFiles(dataPath, Comparator.naturalOrder());

        for (File f : filesItem) {
            int lineCount = -1; // start at -1 -> frist record is 0
            String header = "";
            try (final InputStream fileStream = new FileInputStream(f); 
                    final InputStream gzipStream = new GZIPInputStream(fileStream); 
                    final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8); 
                    final BufferedReader br = new BufferedReader(decoder)) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (lineCount == -1) {
                        header = line;
                    }
                    lineCount++;
                }
            }

            if (lineCount > 0) {
                final String queryTmp = "COPY main." + type.toString().toLowerCase() + "(" + header + ") FROM '" + f.getAbsolutePath() + "' (FORMAT 'CSV', HEADER 1, DELIMITER ',', QUOTE '\"')";
                database.getJdbcTemplate().execute(queryTmp);
                log.info("Copied {} MISSING {} from {} to database with \"{}\"", lineCount, type, f.getName(), queryTmp);
            } else {
                log.info("Copied {} MISSING {} from {} to database", lineCount, type, f.getName());
            }

        }

    }
}
