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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Import implements Runnable {

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
    // private final static String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS \"main\";";
    
    private final static String DROP_ITEM_TABLE = """
                                                  DROP TABLE IF EXISTS "item";
                                                  DROP INDEX IF EXISTS "item_timestamp";
                                                  DROP INDEX IF EXISTS "item_status";
                                                  """;
    private final static String DROP_PERSON_TABLE = """
                                                    DROP TABLE IF EXISTS "person";
                                                    DROP INDEX IF EXISTS "person_timestamp";
                                                    DROP INDEX IF EXISTS "person_status";
                                                    """;
    private final static String DROP_ORGANIZATION_TABLE = """
                                                          DROP TABLE IF EXISTS "organization";
                                                          DROP INDEX IF EXISTS "organization_timestamp";
                                                          DROP INDEX IF EXISTS "organization_status";
                                                          """;
    private final static String CREATE_ITEM_TABLE = """
                                                    CREATE TABLE IF NOT EXISTS "item"(
                                                      "pkey" int NOT NULL AUTO_INCREMENT,
                                                      "timestamp" TIMESTAMP NOT NULL,
                                                      "id" VARCHAR(32) NOT NULL,
                                                      "status" VARCHAR(16) NOT NULL,
                                                      "dataset_id" VARCHAR(128),
                                                      "label" VARCHAR(1024),
                                                      "provider_id" VARCHAR(256),
                                                      "supplier_id" VARCHAR(128),
                                                      "provider_item_id" VARCHAR(512),
                                                      "sector_fct" VARCHAR(16),
                                                      PRIMARY KEY ("pkey")
                                                    );
                                                    CREATE INDEX IF NOT EXISTS "item_timestamp" ON "item"("timestamp");
                                                    CREATE INDEX IF NOT EXISTS "item_status" ON "item"("status");
                                                    """;

    private final static String CREATE_PERSON_TABLE = """
                                                      CREATE TABLE IF NOT EXISTS "person"(
                                                        "pkey" int NOT NULL AUTO_INCREMENT,
                                                        "timestamp" TIMESTAMP NOT NULL,
                                                        "id" VARCHAR(64) NOT NULL,
                                                        "status" VARCHAR(16) NOT NULL,
                                                        "variant_id" VARCHAR(256),
                                                        "preferredName" VARCHAR(1024),
                                                        "type" VARCHAR(32),
                                                        PRIMARY KEY ("pkey")
                                                      );
                                                      CREATE INDEX IF NOT EXISTS "person_timestamp" ON "person"("timestamp");
                                                      CREATE INDEX IF NOT EXISTS "person_status" ON "person"("status");
                                                      """;

    private final static String CREATE_ORGANIZATION_TABLE = """
                                                            CREATE TABLE IF NOT EXISTS "organization"(
                                                              "pkey" int NOT NULL AUTO_INCREMENT,
                                                              "timestamp" TIMESTAMP NOT NULL,
                                                              "id" VARCHAR(64) NOT NULL,
                                                              "status" VARCHAR(16) NOT NULL,
                                                              "variant_id" VARCHAR(256),
                                                              "preferredName" VARCHAR(1024),
                                                              "type" VARCHAR(32),
                                                              PRIMARY KEY ("pkey")
                                                            );
                                                            CREATE INDEX IF NOT EXISTS "organization_timestamp" ON "organization"("timestamp");
                                                            CREATE INDEX IF NOT EXISTS "organization_status" ON "organization"("status");
                                                            """;

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
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
    }

    public void importToDb() throws IOException, FileNotFoundException, SQLException {

        // database.getJdbcTemplate().execute(CREATE_SCHEMA); // create database (only inital)
        database.getJdbcTemplate().execute(DROP_ITEM_TABLE); // drop existing tables
        database.getJdbcTemplate().execute(DROP_PERSON_TABLE);
        database.getJdbcTemplate().execute(DROP_ORGANIZATION_TABLE);
        database.getJdbcTemplate().execute(CREATE_ITEM_TABLE); // create table
        database.getJdbcTemplate().execute(CREATE_PERSON_TABLE);
        database.getJdbcTemplate().execute(CREATE_ORGANIZATION_TABLE);
        database.getConnection().commit();

        for (Type t : Type.values()) {
            switch (t) {
                case ITEM ->
                    importTypes(t, dataPathItem);
                case PERSON ->
                    importTypes(t, dataPathPerson);
                case ORGANIZATION ->
                    importTypes(t, dataPathOrganization);
                default -> {
                }
            }

        }

    }

    private void importTypes(Type type, String dataPath) throws FileNotFoundException, IOException, SQLException {
        final Set<File> filesItem = Helper.getOkCmpFiles(dataPath, Comparator.naturalOrder());

        for (File f : filesItem) {
            try (final InputStream fileStream = new FileInputStream(f); final InputStream gzipStream = new GZIPInputStream(fileStream); final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8); final BufferedReader br = new BufferedReader(decoder)) {

                final CSVParser records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(br);
                final Iterator<CSVRecord> recIt = records.iterator();
                final StringBuilder recQueryBuilder = new StringBuilder("INSERT INTO \"" + type.toString().toLowerCase() + "\"(");
                for (String head : records.getHeaderNames()) {
                    recQueryBuilder.append('"');
                    recQueryBuilder.append(head);
                    recQueryBuilder.append('"');
                    recQueryBuilder.append(',');
                }
                recQueryBuilder.setLength(recQueryBuilder.length() - 1);
                recQueryBuilder.append(") VALUES (");
                for (int i = 0; i < records.getHeaderNames().size(); ++i) {
                    recQueryBuilder.append("?,");
                }
                recQueryBuilder.setLength(recQueryBuilder.length() - 1);
                recQueryBuilder.append(");");

                final String recQuery = recQueryBuilder.toString();

                PreparedStatement statement = database.getConnection().prepareStatement(recQuery);
//                PreparedStatementCreator psc = new PreparedStatementCreator() {
//                    @Override
//                    public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
//                        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
//                    }
//                }
//                        
                int count = 0;
                while (recIt.hasNext()) {
                    final CSVRecord rec = recIt.next();
                    final LocalDateTime localDateTime = LocalDateTime.from(dtf.parse(rec.get(0)));

                    statement.setTimestamp(1, Timestamp.valueOf(localDateTime));
                    statement.setString(2, StringUtils.abbreviate(rec.get(1), 32)); // id VARCHAR(32) NOT NULL,
                    statement.setString(3, StringUtils.abbreviate(rec.get(2), 16)); // status VARCHAR(16) NOT NULL,
                    switch (type) {
                        case ITEM -> {
                            statement.setString(4, StringUtils.abbreviate(rec.get(3), 128)); // dataset_id VARCHAR(128)
                            statement.setString(5, StringUtils.abbreviate(rec.get(4), 1024)); // label VARCHAR(1024),
                            statement.setString(6, StringUtils.abbreviate(rec.get(5), 256)); // provider_id VARCHAR(256),
                            statement.setString(7, StringUtils.abbreviate(rec.get(6), 128)); // supplier_id VARCHAR(128),
                            statement.setString(8, StringUtils.abbreviate(rec.get(7), 512)); // provider_item_id VARCHAR(512),
                            statement.setString(9, StringUtils.abbreviate(rec.get(8), 16)); //  sector_fct VARCHAR(16),
                        }
                        case PERSON -> {
                            statement.setString(4, StringUtils.abbreviate(rec.get(3), 256)); // variant_id VARCHAR(256),
                            statement.setString(5, StringUtils.abbreviate(rec.get(4), 1024)); // preferredName VARCHAR(1024),
                            statement.setString(6, StringUtils.abbreviate(rec.get(5), 32)); // type VARCHAR(32),
                        }
                        case ORGANIZATION -> {
                            statement.setString(4, StringUtils.abbreviate(rec.get(3), 256)); // variant_id VARCHAR(256),
                            statement.setString(5, StringUtils.abbreviate(rec.get(4), 1024)); // preferredName VARCHAR(1024),
                            statement.setString(6, StringUtils.abbreviate(rec.get(5), 32)); // type VARCHAR(32),
                        }
                        default -> {
                        }
                    }
                    statement.addBatch();
                    
                    if (++count % 1000 == 0 || !recIt.hasNext()) {
                        statement.executeBatch();
                    }

                }
                statement.close();
                database.getConnection().commit();
                log.info("Copied {} MISSING {} from {} to database with \"{}\"", count, type, f.getName(), recQuery.toString());
            }
        }
    }
}
