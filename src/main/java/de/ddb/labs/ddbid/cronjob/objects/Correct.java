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
package de.ddb.labs.ddbid.cronjob.objects;

import de.ddb.labs.ddbid.Application;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.Status;
import de.ddb.labs.ddbid.model.Type;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Correct implements Runnable {
    
    private static final int BATCH_SIZE = 500;

    private final Database database;
    private final OkHttpClient httpClient;

    private void check(Type type) {
        final String tableName = type.getType().toLowerCase();

        String apiBase = Application.API;
        switch (type) {
            case ITEM -> apiBase += "/search/index/search/select?wt=csv&fl=id&q=id:";
            case PERSON -> apiBase += "/search/index/person/select?wt=csv&fl=id&q=id:";
            case ORGANIZATION -> apiBase += "/search/index/organization/select?wt=csv&fl=id&q=id:";
            default -> { }
        }
        final String api = apiBase;

        log.info("Start checking MISSING {} entries if they're back again ...", tableName);
        final AtomicInteger totalChecked = new AtomicInteger(0);
        final AtomicInteger countFound = new AtomicInteger(0);
        int lastPkey = 0;

        while (true) {
            final int fromPkey = lastPkey;
            final List<Map.Entry<Integer, String>> batch = database.getJdbcTemplate().query(
                    "SELECT \"pkey\", \"id\" FROM \"" + tableName
                    + "\" WHERE \"status\" = 'MISSING' AND \"pkey\" > ? ORDER BY \"pkey\" LIMIT " + BATCH_SIZE,
                    ps -> ps.setInt(1, fromPkey),
                    (rs, rowNum) -> Map.entry(rs.getInt("pkey"), rs.getString("id")));

            if (batch.isEmpty()) {
                break;
            }

            final CountDownLatch latch = new CountDownLatch(batch.size());

            for (final Map.Entry<Integer, String> entry : batch) {
                final Request request = new Request.Builder()
                        .url(api + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                        .get()
                        .build();

                httpClient.newCall(request).enqueue(new Callback() {
                    @Override
                    public void onFailure(Call call, IOException e) {
                        log.error("Could not check if {} {} still exists. {}", tableName, entry.getValue(), e.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onResponse(Call call, Response response) throws IOException {
                        try (response) {
                            if (response.isSuccessful() && response.body() != null) {
                                final String body = response.body().string();
                                if (countLines(body) > 1) {
                                    log.info("Re-ingested: {} – updating status to FOUND.", entry.getValue());
                                    countFound.incrementAndGet();
                                    database.getJdbcTemplate().update(
                                            "UPDATE \"" + tableName + "\" SET \"status\" = ? WHERE \"pkey\" = ?",
                                            Status.FOUND.toString(), entry.getKey());
                                }
                            }
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while processing MISSING {}.", tableName);
                return;
            }

            totalChecked.addAndGet(batch.size());
            lastPkey = batch.get(batch.size() - 1).getKey();
        }

        log.info("Done checking {} MISSING {}. {} are back again.", totalChecked.get(), tableName, countFound.get());
    }

    private static int countLines(String str) {
        final String[] lines = str.split("\r\n|\r|\n");
        return lines.length;
    }

    @Override
    public void run() {
        for (Type type : Type.values()) {
            check(type);
        }
    }
}
