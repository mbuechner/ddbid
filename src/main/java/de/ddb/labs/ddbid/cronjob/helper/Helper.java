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
package de.ddb.labs.ddbid.cronjob.helper;

import static de.ddb.labs.ddbid.cronjob.objects.Compare.CMPOK_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.CMP_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.DUMPOK_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.DUMP_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.OK_FILENAME_EXT;
import static de.ddb.labs.ddbid.cronjob.objects.Compare.OUTPUT_FILENAME_EXT;
import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 *
 * @author buechner
 */
public class Helper {

    public static Set<File> getOkCmpFiles(String dataPath, Comparator comparator) {

        // dump files
        final Pattern dumpPattern = Pattern.compile(CMP_FILES_PATTERN);
        final FileFilter dumpFileFilter = (File pathname) -> {
            final String fileName = pathname.getName();
            return dumpPattern.matcher(fileName).matches();
        };
        final File[] dumpfiles = new File(dataPath).listFiles(dumpFileFilter);
        final TreeSet<File> dumpfilesSorted = new TreeSet<>(comparator);
        dumpfilesSorted.addAll(Arrays.asList(dumpfiles));

        //ok files
        final Pattern okfilePattern = Pattern.compile(CMPOK_FILES_PATTERN);
        final FileFilter okfileFileFilter = (File pathname) -> {
            final String fileName = pathname.getName();
            return okfilePattern.matcher(fileName).matches();
        };
        final File[] okfilefiles = new File(dataPath).listFiles(okfileFileFilter);
        // final TreeSet<File> okfilefilesSorted = new TreeSet<>(Arrays.asList(okfilefiles));

        final Iterator<File> dumpfilesSortedIt = dumpfilesSorted.iterator();
        while (dumpfilesSortedIt.hasNext()) {
            final File dump = dumpfilesSortedIt.next();
            final String dumpFileName = dump.getName().replace(OUTPUT_FILENAME_EXT, OK_FILENAME_EXT);
            if (!Arrays.stream(okfilefiles).map(p -> p.getName()).anyMatch(dumpFileName::equals)) {
                dumpfilesSortedIt.remove();
            }
        }

        return dumpfilesSorted;
    }

    public static Set<File> getOkDumpFiles(String dataPath, Comparator comparator) {

        // dump files
        final Pattern dumpPattern = Pattern.compile(DUMP_FILES_PATTERN);
        final FileFilter dumpFileFilter = (File pathname) -> {
            final String fileName = pathname.getName();
            return dumpPattern.matcher(fileName).matches();
        };
        final File[] dumpfiles = new File(dataPath).listFiles(dumpFileFilter);
        final TreeSet<File> dumpfilesSorted = new TreeSet<>(comparator);
        dumpfilesSorted.addAll(Arrays.asList(dumpfiles));

        //ok files
        final Pattern okfilePattern = Pattern.compile(DUMPOK_FILES_PATTERN);
        final FileFilter okfileFileFilter = (File pathname) -> {
            final String fileName = pathname.getName();
            return okfilePattern.matcher(fileName).matches();
        };
        final File[] okfilefiles = new File(dataPath).listFiles(okfileFileFilter);
        // final TreeSet<File> okfilefilesSorted = new TreeSet<>(Arrays.asList(okfilefiles));

        final Iterator<File> dumpfilesSortedIt = dumpfilesSorted.iterator();
        while (dumpfilesSortedIt.hasNext()) {
            final File dump = dumpfilesSortedIt.next();
            final String dumpFileName = dump.getName().replace(OUTPUT_FILENAME_EXT, OK_FILENAME_EXT);
            if (!Arrays.stream(okfilefiles).map(p -> p.getName()).anyMatch(dumpFileName::equals)) {
                dumpfilesSortedIt.remove();
            }
        }

        return dumpfilesSorted;
    }

    public static void deleteInvalidDumps(String dataPath) {
        final Set<File> okDumps = getOkDumpFiles(dataPath, Comparator.naturalOrder());

        // dump files
        final Pattern dumpPattern = Pattern.compile(DUMP_FILES_PATTERN);
        final FileFilter dumpFileFilter = (File pathname) -> {
            final String fileName = pathname.getName();
            return dumpPattern.matcher(fileName).matches();
        };
        final File[] dumpfiles = new File(dataPath).listFiles(dumpFileFilter);
        if (dumpfiles != null) {
            for (File df : dumpfiles) {
                if (!okDumps.contains(df)) {
                    if (df.delete()) {
                        df.deleteOnExit();
                    }
                }
            }
        }
    }

}
