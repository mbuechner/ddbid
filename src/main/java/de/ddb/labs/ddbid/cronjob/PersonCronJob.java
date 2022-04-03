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

import de.ddb.labs.ddbid.model.person.PersonDoc;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@Slf4j
@Service
public class PersonCronJob extends CronJob implements CronJobInterface {

    private static final String QUERY = "/search/index/person/select?q=*:*&wt=json&fl=id,variant_id,preferredName,type&sort=id ASC&rows=" + ENTITYCOUNT;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPath;

    @Value(value = "${ddbid.database.table.person}")
    private String tableName;

    public PersonCronJob(@Value(value = "${ddbid.cron.person}") String scheduledPattern) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        super(PersonDoc.class);
        log.info("{} is scheduled at {}", getClass().getName(), scheduledPattern);
    }

    @Override
    @Scheduled(cron = "${ddbid.cron.person}")
    @Retryable(value = {Exception.class}, maxAttemptsExpression = "${ddbid.cron.retry.maxAttempts}", backoff = @Backoff(delayExpression = "${ddbid.cron.retry.delay}"))
    public void schedule() throws IOException {
        log.info("{} started...", this.getClass().getName());
        super.setQuery(QUERY);
        super.setDataPath(dataPath);
        super.setTableName(tableName);
        super.schedule();
        log.info("{} finished.", this.getClass().getName());
    }

    @Override
    public void run() {
        try {
            schedule();
        } catch (IOException e) {
            log.error("{}", e.getMessage());
        }
    }
}
