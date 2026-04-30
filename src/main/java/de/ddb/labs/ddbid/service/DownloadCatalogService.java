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
package de.ddb.labs.ddbid.service;

import de.ddb.labs.ddbid.cronjob.helper.Helper;
import java.io.File;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriUtils;

@Slf4j
@Service
public class DownloadCatalogService {

    private static final Duration CACHE_DURATION = Duration.ofMinutes(5);
    private static final ThreadLocal<DecimalFormat> FORMATTER = ThreadLocal.withInitial(() -> new DecimalFormat("#,##0.#"));

    private final GitHubService gitHub;
    private final Object cacheLock = new Object();

    @Value(value = "${ddbid.datapath.item}")
    private String dataPathItem;

    @Value(value = "${ddbid.datapath.person}")
    private String dataPathPerson;

    @Value(value = "${ddbid.datapath.organization}")
    private String dataPathOrganization;

    private volatile CacheEntry cacheEntry;
    private volatile MigrationCacheEntry migrationCacheEntry;

    public DownloadCatalogService(GitHubService gitHub) {
        this.gitHub = gitHub;
    }

    public DownloadCatalog getCatalog(boolean refresh) {
        final Instant now = Instant.now();
        CacheEntry current = cacheEntry;
        if (!refresh && current != null && current.isValid(now)) {
            return current.catalog();
        }

        synchronized (cacheLock) {
            current = cacheEntry;
            if (!refresh && current != null && current.isValid(now)) {
                return current.catalog();
            }

            final DownloadCatalog catalog = new DownloadCatalog(now, List.of(
                    group("item", "Item", dataPathItem),
                    group("person", "Person", dataPathPerson),
                    group("organization", "Organization", dataPathOrganization)
            ));
            cacheEntry = new CacheEntry(now, catalog);
            return catalog;
        }
    }

    public MigrationCatalog getMigrationCatalog(boolean refresh) {
        final Instant now = Instant.now();
        MigrationCacheEntry current = migrationCacheEntry;
        if (!refresh && current != null && current.isValid(now)) {
            return current.catalog();
        }

        synchronized (cacheLock) {
            current = migrationCacheEntry;
            if (!refresh && current != null && current.isValid(now)) {
                return current.catalog();
            }

            final MigrationCatalog catalog = loadMigrationCatalog(now);
            migrationCacheEntry = new MigrationCacheEntry(now, catalog);
            return catalog;
        }
    }

    private MigrationCatalog loadMigrationCatalog(Instant generatedAt) {
        try {
            return new MigrationCatalog(generatedAt, migrationEntries(gitHub.getCommits()), null);
        } catch (Exception e) {
            log.warn("Could not load migration downloads. {}", e.getMessage());
            return new MigrationCatalog(generatedAt, List.of(), "Migration downloads could not be loaded.");
        }
    }

    private DownloadGroup group(String type, String label, String path) {
        return new DownloadGroup(
                type,
                label,
                fileEntries(type, Helper.getOkDumpFiles(path, Comparator.reverseOrder())),
                fileEntries(type, Helper.getOkCmpFiles(path, Comparator.reverseOrder()))
        );
    }

    private static List<DownloadEntry> fileEntries(String type, Set<File> files) {
        final List<DownloadEntry> entries = new ArrayList<>(files.size());
        for (File file : files) {
            entries.add(new DownloadEntry(
                    file.getName(),
                    readableFileSize(file.length()),
                    "download/ddbid/" + type + "/" + UriUtils.encodePathSegment(file.getName(), java.nio.charset.StandardCharsets.UTF_8)
            ));
        }
        return entries;
    }

    private static List<DownloadEntry> migrationEntries(Map<String, String> commits) {
        final List<DownloadEntry> entries = new ArrayList<>(commits.size());
        for (Map.Entry<String, String> commit : commits.entrySet()) {
            entries.add(new DownloadEntry(
                    commit.getValue() + "-" + GitHubService.FILE_NAME,
                    null,
                    "download/migration/" + commit.getKey() + "/" + commit.getValue()
            ));
        }
        return entries;
    }

    public static String readableFileSize(long size) {
        if (size <= 0) {
            return "0 byte";
        }
        final String[] units = new String[]{"byte", "KiB", "MiB", "GiB", "TiB"};
        final int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return FORMATTER.get().format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    public record DownloadCatalog(Instant generatedAt, List<DownloadGroup> groups) {

        public DownloadCatalog {
            groups = List.copyOf(groups);
        }

        @Override
        public List<DownloadGroup> groups() {
            return List.copyOf(groups);
        }
    }

    public record MigrationCatalog(Instant generatedAt, List<DownloadEntry> entries, String error) {

        public MigrationCatalog {
            entries = List.copyOf(entries);
        }

        @Override
        public List<DownloadEntry> entries() {
            return List.copyOf(entries);
        }
    }

    public record DownloadGroup(String type, String label, List<DownloadEntry> dumps, List<DownloadEntry> compare) {

        public DownloadGroup {
            dumps = List.copyOf(dumps);
            compare = List.copyOf(compare);
        }

        @Override
        public List<DownloadEntry> dumps() {
            return List.copyOf(dumps);
        }

        @Override
        public List<DownloadEntry> compare() {
            return List.copyOf(compare);
        }
    }

    public record DownloadEntry(String name, String size, String href) {
    }

    private record CacheEntry(Instant createdAt, DownloadCatalog catalog) {

        private boolean isValid(Instant now) {
            return createdAt.plus(CACHE_DURATION).isAfter(now);
        }
    }

    private record MigrationCacheEntry(Instant createdAt, MigrationCatalog catalog) {

        private boolean isValid(Instant now) {
            return createdAt.plus(CACHE_DURATION).isAfter(now);
        }
    }
}
