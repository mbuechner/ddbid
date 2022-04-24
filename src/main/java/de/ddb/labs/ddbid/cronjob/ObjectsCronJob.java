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
package de.ddb.labs.ddbid.cronjob;

import de.ddb.labs.ddbid.cronjob.objects.Compare;
import de.ddb.labs.ddbid.cronjob.objects.Correct;
import de.ddb.labs.ddbid.cronjob.objects.Dump;
import de.ddb.labs.ddbid.cronjob.objects.Import;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class ObjectsCronJob implements CronJobInterface {
    
    @Autowired
    private Dump dump;
    
    @Autowired
    private Compare compare;
    
    @Autowired
    private Import importer;
    
    @Autowired
    private Correct correct;
    
    public ObjectsCronJob(@Value(value = "${ddbid.cron.objects}") String scheduledPattern) {
        log.info("{} is scheduled at {}", getClass().getName(), scheduledPattern);
    }
    
    @Override
    @Scheduled(cron = "${ddbid.cron.objects}")
    @Retryable(value = {Exception.class}, maxAttemptsExpression = "${ddbid.cron.retry.maxAttempts}", backoff = @Backoff(delayExpression = "${ddbid.cron.retry.delay}"))
    public void schedule() throws Exception {
        log.info("Start to create dumps, comare them, import data and correct database items...");
        log.info("Create new dump...");
        dump.run();
        log.info("Compare dump files...");
        compare.run();
        log.info("Import data to new database...");
        importer.run();
        log.info("Correct impored data...");
        correct.run();
        log.info("Done");
    }
    
    @Override
    public void run() {
        try {
            schedule();
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }
    } 
}
