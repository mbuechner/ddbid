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

import de.ddb.labs.ddbid.cronjob.CorrectorCronJob;
import de.ddb.labs.ddbid.cronjob.DirectMigrationCronJob;
import de.ddb.labs.ddbid.cronjob.ItemCronJob;
import de.ddb.labs.ddbid.cronjob.OrganizationCronJob;
import de.ddb.labs.ddbid.cronjob.PersonCronJob;
import de.ddb.labs.ddbid.database.Database;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.scheduling.TaskScheduler;

@RestController
@RequestMapping("maintenance")
@Slf4j
public class MaintenanceRestController {
    
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
    private final static String CREATE_ITEM_TABLE_ALTER01 = "ALTER TABLE item ADD COLUMN provider_item_id VARCHAR(512);";
    private final static String CREATE_ITEM_TABLE_ALTER02 = "ALTER TABLE item ADD COLUMN sector_fct VARCHAR(16);";
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
    /*
    private final static String ADD_MISSING_SECTOR_01 = "UPDATE item SET sector_fct = 'sec_02' WHERE provider_id = '[265BI7NE7QBS4NQMZCCGIVLFR73OCOSL, 00014072, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_02 = "UPDATE item SET sector_fct = 'sec_02' WHERE provider_id = '[2Q37XY5KXJNJE5MV6SWP3UKKZ6RSBLK5, 00012008]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_03 = "UPDATE item SET sector_fct = 'sec_04' WHERE provider_id = '[2RBJRLJLDGCQHRDTFSUQ6UHVRRVTJRCH, oid1488275186543, LL7CDI64JH5L5GXNLU53D25TIM6PL2SI, KXX5FA2GGOI6O3XS3JO6G2WDG5LIPNWB, XDNCPF2LBDI5VCSGTHZKY7RINDXKCMJX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_04 = "UPDATE item SET sector_fct = 'sec_01' WHERE provider_id = '[3NW7MGKSV3WGREW62YG54LJYKSNPMPMV, 00000371]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_05 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[AHIKGV2VQJCTI63BAIGBDSB3J3MLASOS, oid1523956740946, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_06 = "UPDATE item SET sector_fct = 'sec_02' WHERE provider_id = '[BZVTR553HLJBDMQD5NCJ6YKP3HMBQRF4, 00050009, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_07 = "UPDATE item SET sector_fct = 'sec_05' WHERE provider_id = '[CJY7MSLPOPB7FTPC7JM5K2GGM5PBGLYI, 99900890, 265BI7NE7QBS4NQMZCCGIVLFR73OCOSL]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_08 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[DZSR6QYESO5Z25FNKUDODHOPRPBNWNMA, oid1526289503911, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_09 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[EGCA66RQCQZMVMLIFI74MXN4PU2Q6AF3, oid1558946268517]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_10 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[HUO4N7TCVYWGADSRSVDFUJAQEF4OJHXF, oid1526298063398, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_11 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[I6OHYTLAP2UVCMPZS6IUAN6MMBKFDPSZ, oid1523953248808, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_12 = "UPDATE item SET sector_fct = 'sec_02' WHERE provider_id = '[INLVDM4I3AMZLTG6AE6C5GZRJKGOF75K, 00005846, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_13 = "UPDATE item SET sector_fct = 'sec_05' WHERE provider_id = '[JJUO42747XGNHECFH7ZFVLVQYPXS4ZBK, oid1470131600302, 265BI7NE7QBS4NQMZCCGIVLFR73OCOSL]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_14 = "UPDATE item SET sector_fct = 'sec_02' WHERE provider_id = '[JKXOBCEL27PNRZPTRDYWYXVI3E5KHMX2, 00017154, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_15 = "UPDATE item SET sector_fct = 'sec_01' WHERE provider_id = '[MGWFPAWRGVKJ3YVWDOYVLRJQG2CKCLIC, oid1471510992314]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_16 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[N2JJLO6NUJRVPUKRJAZVQRAC6JUP2ATP, oid1523956195060, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_17 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[R2354GQ5RX6BQQB7RVF6CKRKZT24JEPP, 00050315, U3OFZLW5PNNYI54ZVBLJSCMWIBJ2T5ZU, XDNCPF2LBDI5VCSGTHZKY7RINDXKCMJX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_18 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[R2354GQ5RX6BQQB7RVF6CKRKZT24JEPP, 00050315, U3OFZLW5PNNYI54ZVBLJSCMWIBJ2T5ZU]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_19 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[SAZUM7BIJH2V7RD4VSFSCZO67R4E3SNA, oid1523957047471, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_20 = "UPDATE item SET sector_fct = 'sec_02' WHERE provider_id = '[TJPSHRLGAEG4CVXBL3ZVMWCDEOSEV2OV, 99900812, KXX5FA2GGOI6O3XS3JO6G2WDG5LIPNWB, X6VKVOM5HGHDIQX36BI3ZKWROZTN74UX]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_21 = "UPDATE item SET sector_fct = 'sec_06' WHERE provider_id = '[VS424HF5PDIIP6JGX77KXU27RFTTF4GS, oid1526292408684, XJRNQZS3AW2U2BZTHYJQ2MAGF45YT6X2]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_22 = "UPDATE item SET sector_fct = 'sec_01' WHERE provider_id = '[XYMQPA4OHAYDDFYWHV6Q4RFUIISTLQJV, 00000896]' AND sector_fct IS NULL;";
    private final static String ADD_MISSING_SECTOR_23 = "UPDATE item SET sector_fct = 'sec_03' WHERE provider_id = '[ZUSXA5RDTYUYRQ5DWSIOL2TXHV62R47F, 00008976]' AND sector_fct IS NULL;";
     */

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

    @Autowired
    private TaskScheduler taskScheduler;

    // Tasks
    @Autowired
    private ItemCronJob itemCronJob;

    @Autowired
    private PersonCronJob personCronJob;

    @Autowired
    private OrganizationCronJob organizationCronJob;

    @Autowired
    private CorrectorCronJob correctorCronJob;

    @Autowired
    private DirectMigrationCronJob directMigrationCronJob;

    @GetMapping
    @RequestMapping("initdb")
    public Map<String, Object> initDb() {

        final List<String> queries = new ArrayList<>();

        queries.add(CREATE_SCHEMA);
        // item
        queries.add(CREATE_ITEM_TABLE);
        queries.add(CREATE_ITEM_TABLE_ALTER01);
        queries.add(CREATE_ITEM_TABLE_ALTER02);
        /*
        queries.add(ADD_MISSING_SECTOR_01);
        queries.add(ADD_MISSING_SECTOR_02);
        queries.add(ADD_MISSING_SECTOR_03);
        queries.add(ADD_MISSING_SECTOR_04);
        queries.add(ADD_MISSING_SECTOR_05);
        queries.add(ADD_MISSING_SECTOR_06);
        queries.add(ADD_MISSING_SECTOR_07);
        queries.add(ADD_MISSING_SECTOR_08);
        queries.add(ADD_MISSING_SECTOR_09);
        queries.add(ADD_MISSING_SECTOR_10);
        queries.add(ADD_MISSING_SECTOR_11);
        queries.add(ADD_MISSING_SECTOR_12);
        queries.add(ADD_MISSING_SECTOR_13);
        queries.add(ADD_MISSING_SECTOR_14);
        queries.add(ADD_MISSING_SECTOR_15);
        queries.add(ADD_MISSING_SECTOR_16);
        queries.add(ADD_MISSING_SECTOR_17);
        queries.add(ADD_MISSING_SECTOR_18);
        queries.add(ADD_MISSING_SECTOR_19);
        queries.add(ADD_MISSING_SECTOR_20);
        queries.add(ADD_MISSING_SECTOR_21);
        queries.add(ADD_MISSING_SECTOR_22);
        queries.add(ADD_MISSING_SECTOR_23);
         */
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

        final List<String> errors = new ArrayList<>();
        try {
            for (String query : queries) {
                try {
                    database.getJdbcTemplate().execute(query);
                    database.commit(); // make sure change are written
                } catch (Exception ex) {
                    errors.add(ex.getMessage());
                }
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

            if (!errors.isEmpty()) {
                throw new RuntimeException("SQL-Errors occured");
            }
        } catch (RuntimeException e) {
            return new HashMap<String, Object>() {
                {
                    put("status", "warn");
                    put("message", errors);
                }
            };
        } catch (IOException e) {
            return new HashMap<String, Object>() {
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
    @RequestMapping("runcron")
    public Map<String, String> runCrons() {

        try {

//            taskScheduler.schedule(itemCronJob, new Date());
//            taskScheduler.schedule(personCronJob, new Date());
//            taskScheduler.schedule(organizationCronJob, new Date());
//            taskScheduler.schedule(correctorCronJob, new Date());
            taskScheduler.schedule(directMigrationCronJob, new Date());

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
