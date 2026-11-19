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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.time.Instant;
import java.util.Map;
import org.springframework.scheduling.TaskScheduler;

@RestController
@RequestMapping(value = "maintenance", headers = "X-DDBID-Maintenance=true")
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
    
    @PostMapping("dump")
    public ResponseEntity<Map<String, String>> dump() {
        return enqueue(() -> objectsCronJob.runMaintenance(dump), "Dump scheduled.");
    }

    @PostMapping("compare")
    public ResponseEntity<Map<String, String>> compare() {
        return enqueue(() -> objectsCronJob.runMaintenance(compare), "Comparison scheduled.");
    }

    @PostMapping("import")
    public ResponseEntity<Map<String, String>> importer() {
        return enqueue(() -> objectsCronJob.runMaintenance(importer), "Import scheduled.");
    }

    @PostMapping("correct")
    public ResponseEntity<Map<String, String>> correct() {
        return enqueue(() -> objectsCronJob.runMaintenance(correct), "Correction scheduled.");
    }

    @PostMapping("indexes")
    public ResponseEntity<Map<String, String>> indexes() {
        return enqueue(() -> objectsCronJob.runMaintenance(statisticsService::ensureDatabaseIndexes),
                "Index creation scheduled. H2 may lock tables while indexes are created.");
    }

    @DeleteMapping("indexes")
    public ResponseEntity<Map<String, String>> dropIndexes() {
        return enqueue(() -> objectsCronJob.runMaintenance(statisticsService::dropDatabaseIndexes), "Index drop scheduled.");
    }

    @PostMapping("runcron")
    public ResponseEntity<Map<String, String>> runCrons() {
        return enqueue(objectsCronJob::schedule, "Objects cron job scheduled.");
    }

    private ResponseEntity<Map<String, String>> enqueue(Runnable operation, String message) {
        try {
            taskScheduler.schedule(operation, Instant.now());
            log.info(message);
            return ResponseEntity.accepted().body(Map.of("status", "ok", "message", message));
        } catch (RuntimeException e) {
            log.error("Could not schedule maintenance", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("status", "error", "message", "Could not schedule maintenance."));
        }
    }
}
