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

import de.ddb.labs.ddbid.cronjob.objects.Correct;
import de.ddb.labs.ddbid.cronjob.DirectMigrationCronJob;
import de.ddb.labs.ddbid.cronjob.ObjectsCronJob;
import de.ddb.labs.ddbid.cronjob.objects.Dump;
import de.ddb.labs.ddbid.cronjob.objects.Compare;
import de.ddb.labs.ddbid.cronjob.objects.Import;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.springframework.scheduling.TaskScheduler;

@RestController
@RequestMapping("maintenance")
@Slf4j
public class MaintenanceRestController {

     @Autowired
    private TaskScheduler taskScheduler;

    @Autowired
    private ObjectsCronJob objectsCronJob;

    @Autowired
    private DirectMigrationCronJob directMigrationCronJob;

    @Autowired
    private Dump dump;

    @Autowired
    private Compare compare;

    @Autowired
    private Import importer;

    @Autowired
    private Correct correct;

    /**
     * Dump data from API
     *
     * @return
     */
    @GetMapping
    @RequestMapping("dump")
    public Map<String, String> dump() {
        try {
            taskScheduler.schedule(dump, new Date());

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
    @RequestMapping("compare")
    public Map<String, String> compare() {
        try {
            taskScheduler.schedule(compare, new Date());
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
    @RequestMapping("import")
    public Map<String, String> importer() {
        try {
            taskScheduler.schedule(importer, new Date());
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
    @RequestMapping("correct")
    public Map<String, String> correct() {
        try {
            taskScheduler.schedule(correct, new Date());
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
    @RequestMapping("runcron")
    public Map<String, String> runCrons() {

        try {

            taskScheduler.schedule(objectsCronJob, new Date());
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
