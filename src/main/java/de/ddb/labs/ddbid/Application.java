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
package de.ddb.labs.ddbid;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.service.GitHubService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableScheduling
@ConditionalOnProperty(name = "scheduler.enabled", matchIfMissing = true)
@EnableRetry
@EnableAsync
@Slf4j
public class Application {

    public static final String API = "https://api.deutsche-digitale-bibliothek.de";

    @Value("${ddbid.database}")
    private String databaseName;

    private Database database; // for write access

    private OkHttpClient httpClient; // http client

    private ObjectMapper objectMapper; // http client

    @Autowired
    private GitHubService gitHub;

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
    public void afterStartup() {
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
    }

    @PreDestroy
    public void destroy() {
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
    public Database database() {
        if (database != null) {
            return database;
        }
        database = new Database(databaseName);
        return database;
    }

    @Bean
    public OkHttpClient httpClient() {
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
    public ObjectMapper objectMapper() {
        if (objectMapper != null) {
            return objectMapper;
        }
        objectMapper = new ObjectMapper();
        return objectMapper;
    }
}
