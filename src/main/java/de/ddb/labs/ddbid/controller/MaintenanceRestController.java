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

import de.ddb.labs.ddbid.cronjob.ItemCronJob;
import de.ddb.labs.ddbid.cronjob.OrganizationCronJob;
import de.ddb.labs.ddbid.cronjob.PersonCronJob;
import de.ddb.labs.ddbid.database.Database;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("maintenance")
public class MaintenanceRestController {
    
    @Autowired
    private Database database;

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

    @Autowired
    private TaskScheduler taskScheduler;

    // Tasks
    @Autowired
    private ItemCronJob itemCronJob;

    @Autowired
    private PersonCronJob personCronJob;

    @Autowired
    private OrganizationCronJob organizationCronJob;

    @GetMapping
    @RequestMapping("initdb")
    public Map<String, String> initDb() {

        try {
            database.executeWithWriteAccess(SET_TIMEZONE);
            database.executeWithWriteAccess(CREATE_SCHEMA);
            // item
            database.executeWithWriteAccess(CREATE_ITEM_TABLE);
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", itemTableName));
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", itemTableName));
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", itemTableName));
            //person
            database.executeWithWriteAccess(CREATE_PERSON_TABLE);
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", personTableName));
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", personTableName));
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", personTableName));
            //person
            database.executeWithWriteAccess(CREATE_ORGANIZATION_TABLE);
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_1.replaceAll("\\{\\}", organizationTableName));
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_2.replaceAll("\\{\\}", organizationTableName));
            database.executeWithWriteAccess(CREATE_SEARCH_INDEX_3.replaceAll("\\{\\}", organizationTableName));

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

        try {
            taskScheduler.schedule(itemCronJob, new Date());
            taskScheduler.schedule(personCronJob, new Date());
            taskScheduler.schedule(organizationCronJob, new Date());
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
}
