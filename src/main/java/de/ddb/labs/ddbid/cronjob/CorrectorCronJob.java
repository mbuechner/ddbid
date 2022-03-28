/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjob;

import static de.ddb.labs.ddbid.cronjob.CronJob.API;
import de.ddb.labs.ddbid.database.Database;
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
import okhttp3.ResponseBody;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 *
 * @author hanshandlampe
 */
@Slf4j
@Service
public class CorrectorCronJob implements CronJobInterface {

    @Autowired
    private Database database;
    @Value(value = "${ddbid.apikey}")
    private String apiKey;
    @Autowired
    private OkHttpClient httpClient;

    private final static String MISSING_ITEM = "SELECT \"timestamp\", id FROM item\n"
            + "WHERE status = 'MISSING';";

    @Override
    @Scheduled(cron = "${ddbid.cron.corrector}")
    @Retryable(value = {Exception.class}, maxAttemptsExpression = "${ddbid.cron.retry.maxAttempts}", backoff = @Backoff(delayExpression = "${ddbid.cron.retry.delay}"))
    public void schedule() throws IOException {
        final MultiValuedMap<Timestamp, String> mi = database.getJdbcTemplate().query(MISSING_ITEM, new ResultSetExtractor<MultiValuedMap>() {
            @Override
            public MultiValuedMap extractData(ResultSet rs) throws SQLException, DataAccessException {
                final MultiValuedMap<Timestamp, String> mapRet = new ArrayListValuedHashMap<>();
                while (rs.next()) {
                    mapRet.put(rs.getTimestamp("timestamp"), rs.getString("id"));
                }
                return mapRet;
            }
        });

        log.info("Start checking {} MISSING items if they're back again...", mi.entries().size());

        final AtomicInteger count = new AtomicInteger(0);
        for (Map.Entry<Timestamp, String> i : mi.entries()) {

            final Request request = new Request.Builder()
                    .url(CronJob.API + "/items/" + i.getValue())
                    .head()
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"")
                    .build();

            httpClient.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    log.error("Could not check if {} still exists. {}", i.getValue(), e.getMessage());
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    if (response.isSuccessful()) {
                        log.warn("Found {} with {}, so it was re-ingested. Deleted it from DB.", i.getValue(), response.request().url().toString());
                        count.incrementAndGet();
                        //database.getJdbcTemplate().execute("DELETE FROM item WHERE \"timestamp\" = % AND id = %");
                    }
                }
            });

        }

        while (httpClient.dispatcher().queuedCallsCount() > 0);
        log.info("Done checking {} MISSING items. {} are back again.", mi.entries().size(), count.get());
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
