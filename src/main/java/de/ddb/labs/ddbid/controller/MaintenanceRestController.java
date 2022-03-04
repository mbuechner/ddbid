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
package de.ddb.labs.ddbid.controller;

import de.ddb.labs.ddbid.database.Database;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("maintenance")
@Slf4j
public class MaintenanceRestController {

    private final static String SET_TIMEZONE = "Set TimeZone='UTC';";
    private final static String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS main;";
    private final static String CREATE_ITEM_TABLE = "CREATE TABLE IF NOT EXISTS main.item (\n"
            + "\"timestamp\" TIMESTAMP NOT NULL,\n"
            + "id VARCHAR(32) NOT NULL,\n"
            + "status VARCHAR(16) NOT NULL,\n"
            + "dataset_id VARCHAR(128),\n"
            + "label VARCHAR(1024),\n"
            + "provider_id VARCHAR(256),\n"
            + "supplier_id VARCHAR(128),\n"
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
    private final static String CREATE_SEARCH_INDEX_1 = "CREATE INDEX IF NOT EXISTS data_timestamp_IDX ON main.{} (\"timestamp\");";
    private final static String CREATE_SEARCH_INDEX_2 = "CREATE INDEX IF NOT EXISTS data_status_IDX ON main.{} (\"status\");";
    private final static String CREATE_SEARCH_INDEX_3 = "CREATE INDEX IF NOT EXISTS data_id_IDX ON main.{} (\"id\");";
    @Autowired
    private Database database;
    @Value("${ddbid.datapath.item}")
    private String dataPathItem;

    @Value("${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value("${ddbid.datapath.organization}")
    private String dataPathOrganization;

    @Value("${ddbid.database.table.item}")
    private String itemTableName;

    @Value("${ddbid.database.table.person}")
    private String personTableName;

    @Value("${ddbid.database.table.organization}")
    private String organizationTableName;

//    @Autowired
//    private TaskScheduler taskScheduler;
//
//    // Tasks
//    @Autowired
//    private ItemCronJob itemCronJob;
//
//    @Autowired
//    private PersonCronJob personCronJob;
//
//    @Autowired
//    private OrganizationCronJob organizationCronJob;

    @GetMapping
    @RequestMapping("initdb")
    public Map<String, String> initDb() {

        try {

            final List<String> queries = new ArrayList<>();

            queries.add(SET_TIMEZONE);
            queries.add(CREATE_SCHEMA);
            // item
            queries.add(CREATE_ITEM_TABLE);
            queries.add(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", itemTableName));
            queries.add(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", itemTableName));
            queries.add(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", itemTableName));
            //person
            queries.add(CREATE_PERSON_TABLE);
            queries.add(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", personTableName));
            queries.add(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", personTableName));
            queries.add(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", personTableName));
            //person
            queries.add(CREATE_ORGANIZATION_TABLE);
            queries.add(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", organizationTableName));
            queries.add(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", organizationTableName));
            queries.add(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", organizationTableName));

            for (String query : queries) {
                database.getJdbcTemplate().execute(query);
            }

            // create dirs
            if (!Files.exists(Path.of(dataPathItem))) {
                Files.createDirectories(Path.of(dataPathItem));
            }
            if (!Files.exists(Path.of(dataPathPerson))) {
                Files.createDirectories(Path.of(dataPathPerson));
            }
            if (!Files.exists(Path.of(dataPathOrganization))) {
                Files.createDirectories(Path.of(dataPathOrganization));
            }
        } catch (Exception e) {
            return new HashMap<>() {
                {
                    put("status", "error");
                    put("message", e.getMessage());
                }
            };
        }
        return new HashMap<>() {
            {
                put("status", "ok");
            }
        };
    }

    @GetMapping
    @RequestMapping("runcrons")
    public Map<String, String> runCrons() {

//        try {
//            final Runnable itemCronJobRunnable = new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        itemCronJob.sched();
//                    } catch (Exception ex) {
//                        log.error("{}", ex.getMessage());
//                    }
//                }
//            };
//
//            final Runnable personCronJobRunnable = new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        personCronJob.run();
//                    } catch (Exception ex) {
//                        log.error("{}", ex.getMessage());
//                    }
//                }
//            };
//
//            final Runnable organizationCronJobRunnable = new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        organizationCronJob.run();
//                    } catch (Exception ex) {
//                        log.error("{}", ex.getMessage());
//                    }
//                }
//            };
//
//            taskScheduler.schedule(itemCronJobRunnable, new Date());
//            taskScheduler.schedule(personCronJobRunnable, new Date());
//            taskScheduler.schedule(organizationCronJobRunnable, new Date());
//        } catch (Exception e) {
//            return new HashMap<>() {
//                {
//                    put("status", "error");
//                    put("message", e.getMessage());
//                }
//            };
//        }
//
        return new HashMap<>() {
            {
                put("status", "ok");
            }
        };
    }
}
