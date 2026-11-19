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
package de.ddb.labs.ddbid.cronjob.helper;

import static de.ddb.labs.ddbid.cronjob.objects.Compare.CMP_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.DUMP_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.OK_FILENAME_EXT;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.OUTPUT_FILENAME_EXT;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author buechner
 */
@Slf4j
public class Helper {

    private static final Pattern COMPARE_PATTERN = Pattern.compile(CMP_FILES_PATTERN);
    private static final Pattern DUMP_PATTERN = Pattern.compile(DUMP_FILES_PATTERN);

    public static Set<File> getOkCmpFiles(String dataPath, Comparator<File> comparator) {
        return completedFiles(dataPath, COMPARE_PATTERN, comparator);
    }

    public static Set<File> getOkDumpFiles(String dataPath, Comparator<File> comparator) {
        return completedFiles(dataPath, DUMP_PATTERN, comparator);
    }

    private static Set<File> completedFiles(String dataPath, Pattern pattern, Comparator<File> comparator) {
        Set<File> files = new TreeSet<>(comparator);
        File[] candidates = new File(dataPath).listFiles(file -> pattern.matcher(file.getName()).matches() && file.isFile());
        if (candidates != null) {
            for (File candidate : candidates) {
                if (Files.isRegularFile(successMarker(candidate.toPath()))) {
                    files.add(candidate);
                }
            }
        }
        return files;
    }

    public static Path successMarker(Path dataFile) {
        String name = Objects.requireNonNull(dataFile.getFileName(), "Missing data filename").toString();
        return dataFile.resolveSibling(name.substring(0, name.length() - OUTPUT_FILENAME_EXT.length()) + OK_FILENAME_EXT);
    }

    public static Path createTemporaryFile(Path directory, String prefix) throws IOException {
        if (directory.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            // Match normal archive creation permissions, subject to the process umask.
            return Files.createTempFile(directory, prefix, ".part",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-rw-rw-")));
        }
        return Files.createTempFile(directory, prefix, ".part");
    }

    public static void publish(Path temporary, Path destination, String timestamp, String stagingPrefix) throws IOException {
        Path marker = successMarker(destination);
        // A marker left behind by a removed output must not expose its replacement early.
        Files.deleteIfExists(marker);
        Path directory = Objects.requireNonNull(destination.toAbsolutePath().getParent(), "Missing data directory");
        Path temporaryMarker = createTemporaryFile(directory, stagingPrefix);
        try {
            Files.write(temporaryMarker, List.of(timestamp), StandardCharsets.UTF_8);
            moveIntoPlace(temporary, destination);
            moveIntoPlace(temporaryMarker, marker);
        } finally {
            Files.deleteIfExists(temporaryMarker);
        }
    }

    private static void moveIntoPlace(Path temporary, Path destination) throws IOException {
        try {
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Deletes dumps older than a date, compared by filename date and NOT file
     * date.
     *
     * @param dataPath
     * @param date
     */
    public static void deleteOlderDumps(String dataPath, LocalDate date) {
        log.info("Start to delete old dumps...");
        deleteInvalidDumps(dataPath);

        final Set<File> okDumps = getOkDumpFiles(dataPath, Comparator.reverseOrder());
        int kept = 0;
        for (File f : okDumps) {
            // Keep the newest dump and its predecessor even across a long outage.
            if (kept++ < 2) {
                continue;
            }
            final String d = f.getName().substring(0, 10);
            final LocalDate ld = LocalDate.parse(d);
            if (ld.isBefore(date)) {
                log.info("Delete dump {}, because {} is before {}", f.getAbsoluteFile(), ld.toString(), date.toString());
                try {
                    Files.deleteIfExists(f.toPath());
                    Files.deleteIfExists(successMarker(f.toPath()));
                } catch (IOException e) {
                    log.warn("Could not delete old dump {}", f, e);
                }
            }
        }
    }

    /**
     * Deletes invalid dumps
     *
     * @param dataPath
     */
    public static void deleteInvalidDumps(String dataPath) {
        File[] dumpFiles = new File(dataPath).listFiles(file -> DUMP_PATTERN.matcher(file.getName()).matches() && file.isFile());
        if (dumpFiles != null) {
            for (File file : dumpFiles) {
                if (!Files.isRegularFile(successMarker(file.toPath()))) {
                    try {
                        Files.deleteIfExists(file.toPath());
                    } catch (IOException e) {
                        log.warn("Could not delete incomplete dump {}", file, e);
                    }
                }
            }
        }
    }

}
