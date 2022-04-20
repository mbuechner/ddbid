/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjobs;

import de.ddb.labs.ddbid.database.Database;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
public class Importer implements Runnable {

    private final static String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS main;";
    private final static String DROP_TABLE = "DROP TABLE IF EXISTS main.item";
    private final static String CREATE_ITEM_TABLE = "CREATE TABLE IF NOT EXISTS main.item (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(32) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "dataset_id VARCHAR(128),\n"
            + "label VARCHAR(1024),\n"
            + "provider_id VARCHAR(256),\n"
            + "supplier_id VARCHAR(128),\n"
            + "provider_item_id VARCHAR(512),\n"
            + "sector_fct VARCHAR(16),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";

    private final static String CREATE_PERSON_TABLE = "CREATE TABLE IF NOT EXISTS main.person (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(64) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "variant_id VARCHAR(256),\n"
            + "preferredName VARCHAR(1024),\n"
            + "type VARCHAR(32),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";

    private final static String CREATE_ORGANIZATION_TABLE = "CREATE TABLE IF NOT EXISTS main.organization (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(64) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "variant_id VARCHAR(256),\n"
            + "preferredName VARCHAR(1024),\n"
            + "type VARCHAR(32),\n"
            + "PRIMARY KEY (\"timestamp\", id)\n"
            + ");";

    @Autowired
    private Database database;

    @Autowired
    private Compare compare;

    @Value("${ddbid.datapath.item}")
    private String dataPathItem;

    @Value("${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value("${ddbid.datapath.organization}")
    private String dataPathOrganization;

    // delete database
    // re-import all cmp_files
    // commit!
    @Override
    public void run() {
        try {
            importToDb();
        } catch (IOException ex) {
            Logger.getLogger(Importer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void importToDb() throws IOException {

        // create database (only inital)
        database.getJdbcTemplate().execute(CREATE_SCHEMA);
        // drop table
        database.getJdbcTemplate().execute(DROP_TABLE);
        // create table
        database.getJdbcTemplate().execute(CREATE_ITEM_TABLE);
        database.getJdbcTemplate().execute(CREATE_PERSON_TABLE);
        database.getJdbcTemplate().execute(CREATE_ORGANIZATION_TABLE);
        database.commit();

        final Set<File> filesItem = compare.getOkCmpFiles(dataPathItem, Comparator.naturalOrder());

        for (File f : filesItem) {
            int lineCount = -1; // start at -1 -> frist record is 0
            String header = "";
            try (final InputStream fileStream = new FileInputStream(f); final InputStream gzipStream = new GZIPInputStream(fileStream); final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8); final BufferedReader br = new BufferedReader(decoder);) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (lineCount == -1) {
                        header = line;
                    }
                    lineCount++;
                }
            }

            if (lineCount > 0) {
                final String queryTmp = "COPY main.item(" + header + ") FROM '" + f.getAbsolutePath() + "' (FORMAT 'CSV', HEADER 1, DELIMITER ',', QUOTE '\"')";
                database.getJdbcTemplate().execute(queryTmp);
                log.info("Copied {} MISSING item from {} to database with \"{}\"", lineCount, f.getName(), queryTmp);
            } else {
                log.info("Copied {} MISSING item from {} to database", lineCount, f.getName());
            }
            
        }

    }
}
