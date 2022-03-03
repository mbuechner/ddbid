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

import de.ddb.labs.ddbid.model.item.ItemDoc;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ItemCronJob extends CronJob {

    private static final String QUERY = "/search/index/search/select?q=*:*&wt=json&fl=id,label,provider_id,supplier_id,dataset_id&sort=id ASC&rows=" + ENTITYCOUNT;

    @Value(value = "${ddbid.datapath.item}")
    private String dataPath;

    @Value(value = "${ddbid.database.table.item}")
    private String tableName;

    public ItemCronJob() throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        super(ItemDoc.class);
    }

    @Scheduled(cron = "${ddbid.cron.item}")
    @Retryable(value = RuntimeException.class, maxAttempts = 3, backoff = @Backoff(delay = 600000))
    @Override
    public void run() throws IOException {
        log.info("{} started...", this.getClass().getName());
        super.setQuery(QUERY);
        super.setDataPath(dataPath);
        super.setTableName(tableName);
        super.run();
        log.info("{} finished.", this.getClass().getName());
    }
}
