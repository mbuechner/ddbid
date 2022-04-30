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
package de.ddb.labs.ddbid.cronjob.objects;

import de.ddb.labs.ddbid.Application;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.Status;
import de.ddb.labs.ddbid.model.Type;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Correct implements Runnable {
    
    private final static String QUERY = """
                                        SELECT "timestamp", "id" FROM "{{tbl}}" WHERE "status" = 'MISSING';
                                        """;

    @Autowired
    private Database database;
    
    @Value(value = "${ddbid.apikey}")
    private String apiKey;
    
    @Autowired
    private OkHttpClient httpClient;

    private void check(Type type) {

        final MultiValuedMap<Timestamp, String> mi = database.getJdbcTemplate().query(QUERY.replace("{{tbl}}", type.getType().toLowerCase()), new ResultSetExtractor<MultiValuedMap>() {
            @Override
            public MultiValuedMap extractData(ResultSet rs) throws SQLException, DataAccessException {
                final MultiValuedMap<Timestamp, String> mapRet = new ArrayListValuedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getTimestamp("timestamp"), rs.getString("id"));
                }
                return mapRet;
            }
        });

        if (mi == null) {
            log.error("Database error. Get null as result set.");
            return;
        }

        log.info("Start checking {} MISSING {} if they're back again...", mi.entries().size(), type.getType().toLowerCase());

        String api = Application.API;
        switch (type) {
            case ITEM ->
                api += "/search/index/search/select?wt=csv&fl=id&q=id:";
            case PERSON ->
                api += "/search/index/person/select?wt=csv&fl=id&q=id:";
            case ORGANIZATION ->
                api += "/search/index/organization/select?wt=csv&fl=id&q=id:";
            default -> {
            }
        }

        final AtomicInteger countItem = new AtomicInteger(0);
        for (Map.Entry<Timestamp, String> i : mi.entries()) {

            final Request request = new Request.Builder()
                    .url(api + URLEncoder.encode(i.getValue(), StandardCharsets.UTF_8))
                    .get()
                    .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"")
                    .build();

            httpClient.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    log.error("Could not check if {} {} with {} still exists. {}", type.toString().toLowerCase(), i.getValue(), call.request().url().toString(), e.getMessage());
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    // log.info(response.request().url().toString());
                    if (response.isSuccessful()) {
                        final String body = response.body().string();
                        if (countLines(body) > 1) {
                            log.info("Found {} with {}, so it was re-ingested. Deleted it from DB.", i.getValue(), response.request().url().toString());
                            countItem.incrementAndGet();
                            final String query = "UPDATE main.{{tbl}} SET status = ? WHERE \"timestamp\" = ? AND id = ?".replace("{{tbl}}", type.toString().toLowerCase());
                            database.getJdbcTemplate().update(query, Status.FOUND.toString(), i.getKey(), i.getValue());
                        }
                    }
                }
            });
        }
        while (httpClient.dispatcher().queuedCallsCount() > 0);
        log.info("Done checking {} MISSING {}. {} are back again.", mi.entries().size(), type.toString().toLowerCase(), countItem.get());
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
