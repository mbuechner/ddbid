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
import de.ddb.labs.ddbid.service.StatisticsService;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Correct implements Runnable {
    
    private static final int BATCH_SIZE = 500;
    private static final CSVFormat RESPONSE_CSV = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();

    private final Database database;
    private final OkHttpClient httpClient;
    private final MaintenanceTables tables;
    private final StatisticsService statisticsService;

    private void check(Type type) throws InterruptedException {
        final String tableName = tables.identifier(type);

        String apiBase = Application.API;
        switch (type) {
            case ITEM -> apiBase += "/search/index/search/select?wt=csv&fl=id&q=id:";
            case PERSON -> apiBase += "/search/index/person/select?wt=csv&fl=id&q=id:";
            case ORGANIZATION -> apiBase += "/search/index/organization/select?wt=csv&fl=id&q=id:";
            default -> { }
        }
        final String api = apiBase;

        log.info("Start checking MISSING {} entries if they're back again ...", tableName);
        long totalChecked = 0;
        long countFound = 0;
        int lastPkey = 0;

        while (true) {
            final int fromPkey = lastPkey;
            final List<Map.Entry<Integer, String>> batch = database.getJdbcTemplate().query(
                    "SELECT \"pkey\", \"id\" FROM " + tableName
                    + " WHERE \"status\" = 'MISSING' AND \"pkey\" > ? ORDER BY \"pkey\" LIMIT " + BATCH_SIZE,
                    ps -> ps.setInt(1, fromPkey),
                    (rs, rowNum) -> Map.entry(rs.getInt("pkey"), rs.getString("id")));

            if (batch.isEmpty()) {
                break;
            }

            final CountDownLatch latch = new CountDownLatch(batch.size());
            final Queue<Map.Entry<Integer, String>> found = new ConcurrentLinkedQueue<>();
            final List<Call> calls = new ArrayList<>(batch.size());

            for (final Map.Entry<Integer, String> entry : batch) {
                final Request request = new Request.Builder()
                        .url(api + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                        .get()
                        .build();

                Call call = httpClient.newCall(request);
                calls.add(call);
                call.enqueue(new Callback() {
                    @Override
                    public void onFailure(Call call, IOException e) {
                        log.error("Could not check if {} {} still exists. {}", tableName, entry.getValue(), e.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onResponse(Call call, Response response) {
                        try (response) {
                            if (!response.isSuccessful() || response.body() == null) {
                                log.warn("Could not check {} {}: DDB API returned HTTP {}", tableName, entry.getValue(), response.code());
                            } else if (hasId(response.body().charStream(), entry.getValue())) {
                                found.add(entry);
                            }
                        } catch (IOException | UncheckedIOException e) {
                            log.warn("Could not read DDB API response for {} {}", tableName, entry.getValue(), e);
                        } finally {
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                calls.forEach(Call::cancel);
                throw e;
            }

            totalChecked += batch.size();
            lastPkey = batch.get(batch.size() - 1).getKey();

            if (!found.isEmpty()) {
                final List<Object[]> updateArgs = new ArrayList<>(found.size());
                for (var entry : found) {
                    updateArgs.add(new Object[]{Status.FOUND.toString(), entry.getKey(), entry.getValue()});
                }
                int[] changes = database.getJdbcTemplate().batchUpdate(
                        "UPDATE " + tableName + " SET \"status\" = ? WHERE \"pkey\" = ? AND \"id\" = ? AND \"status\" = 'MISSING'", updateArgs);
                for (int change : changes) {
                    if (change > 0 || change == java.sql.Statement.SUCCESS_NO_INFO) {
                        countFound++;
                    }
                }
            }
        }

        log.info("Done checking {} MISSING {}. {} are back again.", totalChecked, tableName, countFound);
    }

    static boolean hasId(java.io.Reader response, String id) throws IOException {
        try (var csv = RESPONSE_CSV.parse(response)) {
            for (var record : csv) {
                if (record.isSet("id") && id.equals(record.get("id"))) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public void run() {
        try {
            for (Type type : Type.values()) {
                check(type);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("DDB ID correction interrupted");
        } finally {
            statisticsService.clearCache();
        }
    }
}
