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
package de.ddb.labs.ddbid;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.ddb.labs.ddbid.cronjob.ObjectsCronJob;
import de.ddb.labs.ddbid.cronjob.helper.Helper;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.service.GitHubService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableRetry
@EnableAsync
@Slf4j
public class Application {

    public static final String API = "https://api.deutsche-digitale-bibliothek.de/2";

    @Value("${ddbid.database}")
    private String databaseName;

    @Value("${ddbid.database.type:h2}")
    private String databaseType;

    @Value("${ddbid.database.url:}")
    private String databaseUrl;

    @Value("${ddbid.database.user:}")
    private String databaseUser;

    @Value("${ddbid.database.password:}")
    private String databasePassword;

    @Value("${ddbid.dump.lockfile}")
    private String lockfile;
    
    @Value("${ddbid.cron.objects}")
    private String cronPatternObjects;

    @Value("${scheduler.enabled:true}")
    private boolean schedulerEnabled;

    private Database database; // for write access

    private OkHttpClient httpClient; // http client

    private ObjectMapper objectMapper; // http client

    private ObjectsCronJob objectsCronJob;
    
    @Autowired
    private GitHubService gitHub;

    @Autowired
    private TaskScheduler taskScheduler;

    @Value(value = "${ddbid.datapath.item}")
    private String dataPathItem;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value(value = "${ddbid.datapath.organization}")
    private String dataPathOrganization;

    public static void main(String[] args) {

        SpringApplication.run(Application.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void afterStartup() throws IOException {
        log.info("Creating data directories");
        try {
            if (!Files.exists(Path.of(dataPathItem))) {
                Files.createDirectories(Path.of(dataPathItem));
            }
        } catch (IOException e) {
            log.error("Error creating directory for ITEM. {}", e.getMessage());
        }
        try {
            if (!Files.exists(Path.of(dataPathPerson))) {
                Files.createDirectories(Path.of(dataPathPerson));
            }
        } catch (IOException e) {
            log.error("Error creating directory for PERSON. {}", e.getMessage());
        }
        try {
            if (!Files.exists(Path.of(dataPathOrganization))) {
                Files.createDirectories(Path.of(dataPathOrganization));
            }
        } catch (IOException e) {
            log.error("Error creating directory for ORAGNIZATION. {}", e.getMessage());
        }
        log.info("Check for dump lockfile...");
        if (Files.exists(Path.of(lockfile), LinkOption.NOFOLLOW_LINKS)) {
            log.info("There was a cancled dump run. Delete it...");
            Files.deleteIfExists(Path.of(lockfile));
            Helper.deleteInvalidDumps(dataPathItem);
            Helper.deleteInvalidDumps(dataPathPerson);
            Helper.deleteInvalidDumps(dataPathOrganization);
            if (schedulerEnabled) {
                log.info("Re-run Objects Cron Job...");
                taskScheduler.schedule(objectsCronJob, Instant.now());
            } else {
                log.info("Scheduler is disabled. Skip automatic Objects Cron Job restart.");
            }
        }
    }

    @PreDestroy
    private void destroy() {
        log.info("Destroy callback triggered: Closing database...");
        try {
            database.close();
            gitHub.close();
            httpClient.dispatcher().cancelAll();
        } catch (Exception e) {
            log.error("Could not close connection to database. {}", e.getMessage());
        }
    }
    
    @Bean
    protected ObjectsCronJob objectsCronJob() {
        objectsCronJob = new ObjectsCronJob(cronPatternObjects);
        return objectsCronJob;
    }

    @Bean
    protected Database database() {
        if (database != null) {
            return database;
        }
        database = new Database(databaseType, databaseName, databaseUrl, databaseUser, databasePassword);
        return database;
    }

    @Bean
    protected OkHttpClient httpClient() {
        if (httpClient != null) {
            return httpClient;
        }
        final Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(64);
        dispatcher.setMaxRequestsPerHost(8);
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(0, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS)
                .dispatcher(dispatcher)
                .build();
        return httpClient;
    }

    @Bean
    protected ObjectMapper objectMapper() {
        if (objectMapper != null) {
            return objectMapper;
        }
        objectMapper = new ObjectMapper();
        return objectMapper;
    }
}
