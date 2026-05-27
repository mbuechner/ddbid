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
package de.ddb.labs.ddbid.controller;

import de.ddb.labs.ddbid.cronjob.objects.Correct;
import de.ddb.labs.ddbid.cronjob.ObjectsCronJob;
import de.ddb.labs.ddbid.cronjob.objects.Dump;
import de.ddb.labs.ddbid.cronjob.objects.Compare;
import de.ddb.labs.ddbid.cronjob.objects.Import;
import de.ddb.labs.ddbid.service.StatisticsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.time.Instant;
import java.util.Map;
import org.springframework.scheduling.TaskScheduler;

@RestController
@RequestMapping("maintenance")
@Slf4j
@RequiredArgsConstructor
public class MaintenanceRestController {

    private final TaskScheduler taskScheduler;
    private final ObjectsCronJob objectsCronJob;
    private final Dump dump;
    private final Compare compare;
    private final Import importer;
    private final Correct correct;
    private final StatisticsService statisticsService;
    
    /**
     * Dump data from API
     *
     * @return
     */
    @GetMapping("dump")
    public Map<String, String> dump() {
        try {
            taskScheduler.schedule(dump, Instant.now());
            log.info("Dump job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok");
    }

    @GetMapping("compare")
    public Map<String, String> compare() {
        try {
            taskScheduler.schedule(compare, Instant.now());
            log.info("Compare job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok");
    }

    @GetMapping("import")
    public Map<String, String> importer() {
        try {
            taskScheduler.schedule(importer, Instant.now());
            log.info("Import job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok");
    }

    @GetMapping("correct")
    public Map<String, String> correct() {
        try {
            taskScheduler.schedule(correct, Instant.now());
            log.info("Correct job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok");
    }

    @GetMapping("indexes")
    public Map<String, String> indexes() {
        try {
            taskScheduler.schedule(statisticsService::ensureDatabaseIndexes, Instant.now());
            log.info("Index creation job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok", "message", "Index creation scheduled. H2 may lock tables while indexes are created.");
    }

    @DeleteMapping("indexes")
    public Map<String, String> dropIndexes() {
        try {
            taskScheduler.schedule(statisticsService::dropDatabaseIndexes, Instant.now());
            log.info("Index drop job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok", "message", "Index drop scheduled.");
    }
    
    @GetMapping("runcron")
    public Map<String, String> runCrons() {

        try {

            taskScheduler.schedule(objectsCronJob, Instant.now());
            log.info("Objects Cron Job successfully scheduled.");
        } catch (Exception e) {
            return Map.of("status", "error", "message", String.valueOf(e.getMessage()));
        }

        return Map.of("status", "ok");
    }
}
