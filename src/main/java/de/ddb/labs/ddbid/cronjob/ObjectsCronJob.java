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
package de.ddb.labs.ddbid.cronjob;

import de.ddb.labs.ddbid.cronjob.objects.Compare;
import de.ddb.labs.ddbid.cronjob.objects.Correct;
import de.ddb.labs.ddbid.cronjob.objects.Dump;
import de.ddb.labs.ddbid.cronjob.objects.Import;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class ObjectsCronJob implements CronJobInterface {

    private final Dump dump;
    private final Compare compare;
    private final Import importer;
    private final Correct correct;
    private final ReentrantLock maintenanceLock = new ReentrantLock(true);

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Spring maintenance services are intentionally shared")
    public ObjectsCronJob(
            @Value(value = "${ddbid.cron.objects}") String scheduledPattern,
            Dump dump,
            Compare compare,
            Import importer,
            Correct correct) {
        this.dump = dump;
        this.compare = compare;
        this.importer = importer;
        this.correct = correct;
        log.info("{} is scheduled at {}", getClass().getName(), scheduledPattern);
    }
    
    @Override
    @Scheduled(cron = "${ddbid.cron.objects}")
    @Retryable(retryFor = {Exception.class}, maxAttemptsExpression = "${ddbid.cron.retry.max-attempts}", backoff = @Backoff(delayExpression = "${ddbid.cron.retry.delay}"))
    public void schedule() {
        runMaintenance(() -> {
            log.info("Create new dumps");
            dump.run();
            log.info("Compare dump files");
            compare.run();
            log.info("Import comparisons");
            importer.run();
            log.info("Check for reappearing IDs");
            correct.run();
        });
    }

    public void runMaintenance(Runnable operation) {
        maintenanceLock.lock();
        try {
            operation.run();
        } finally {
            maintenanceLock.unlock();
        }
    }
    
    @Override
    public void run() {
        schedule();
    } 
}
