/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package de.ddb.labs.ddbid.cronjobs;

import com.github.davidmoten.bigsorter.Reader;
import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Util;
import com.github.davidmoten.bigsorter.Writer;
import static de.ddb.labs.ddbid.cronjob.CronJob.CMP_FILES_PATTERN;
import static de.ddb.labs.ddbid.cronjob.CronJob.COMPARE_OUTPUT_FILENAME_PREFIX;
import static de.ddb.labs.ddbid.cronjob.CronJob.OK_FILENAME_EXT;
import static de.ddb.labs.ddbid.cronjob.CronJob.OUTPUT_FILENAME_EXT;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.Status;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DumpComparer implements Runnable {

    private final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.systemDefault());
    public static final String COMPARE_OUTPUT_FILENAME_PREFIX = "CMP_";
    public static final String OUTPUT_FILENAME_EXT = ".csv.gz";
    public static final String OK_FILENAME_EXT = ".txt";
    public static final String DUMP_FILES_PATTERN = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\.csv\\.gz";
    public static final String DUMPOK_FILES_PATTERN = "[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\.txt";
    public static final String CMP_FILES_PATTERN = "CMP\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}_(MISSING|NEW)\\.csv\\.gz";
    public static final String CMPOK_FILES_PATTERN = "CMP\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}\\_[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}_(MISSING|NEW)\\.txt";

    @Value(value = "${ddbid.datapath.item}")
    private String dataPath;

    @Override
    public void run() {

        // search for uncompared and creates them
        final File[] dumpFiles = getOkDumpFiles(dataPath, Comparator.naturalOrder());
        final File[] cmpFiles = getOkCmpFiles(dataPath, Comparator.naturalOrder());

        for(int i =0; i<dumpFiles.length;++i) {
            final File fileA = dumpFiles[i];
            if (i >= dumpFiles.length) {
                log.info("{} can't be compared, because it is the last dump.", fileA);
                break; // create no compare for now
            }
            final File fileB = dumpFiles[i+1];

            final String fileABaseName = fileA.getName().substring(0, fileA.getName().indexOf('.'));
            final String fileBBaseName = fileB.getName().substring(0, fileB.getName().indexOf('.'));
            final File outputFileNameABMissing = new File(dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.MISSING + OUTPUT_FILENAME_EXT);
            final File outputFileNameBANew = new File(dataPath + COMPARE_OUTPUT_FILENAME_PREFIX + fileABaseName + "_" + fileBBaseName + "_" + Status.NEW + OUTPUT_FILENAME_EXT);

            if (Arrays.stream(dumpFiles).cmpFiles.contains(outputFileNameABMissing)) {
                log.info("{} already exists. Skipping this one.", outputFileNameABMissing);
            } else {
                log.info("{} does not exist. Create compare file...", outputFileNameABMissing);
            }

            if (cmpFiles.contains(outputFileNameBANew)) {
                log.info("{} already exists. Skipping this one.", outputFileNameBANew);
            } else {
                log.info("{} does not exist. Create compare file...", outputFileNameBANew);
            }

            dumpFilesIterator.
            // compare for NEW
            // write OK file
            // compare for MISSING
            // write OK file
        }

        // writes ok file in the end
    }

    public File[] getOkCmpFiles(String dataPath, Comparator comparator) {

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

        return (File[]) dumpfilesSorted.toArray();
    }

    public File[] getOkDumpFiles(String dataPath, Comparator comparator) {

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

        return (File[]) dumpfilesSorted.toArray();
    }

    /**
     *
     * @param fileA Older file
     * @param fileB Newer File
     * @param doc
     * @param output
     * @param status
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static int findDifferences(File fileA, File fileB, Doc doc, File output, Status status) throws FileNotFoundException, IOException {

        final FileTime creationTimeFileA = (FileTime) Files.getAttribute(fileA.toPath(), "creationTime");
        final FileTime creationTimeFileB = (FileTime) Files.getAttribute(fileB.toPath(), "creationTime");

        if (creationTimeFileA.compareTo(creationTimeFileB) > 0) {
            throw new IllegalArgumentException("First file was created before second file");
        }

        final File tmpFile = File.createTempFile("ddbid-", "csv.gz");
        final Serializer<CSVRecord> csVSerializer = Serializer.csv(CSVFormat.DEFAULT.withFirstRecordAsHeader(), StandardCharsets.UTF_8);
        final Comparator<CSVRecord> comparator = (x, y) -> {
            final String a = x.get("id");
            final String b = y.get("id");
            return a.compareTo(b);
        };
        try (final InputStream fileStreamA = new FileInputStream(fileA); final GZIPInputStream gzipA = new GZIPInputStream(fileStreamA); final Reader readerA = csVSerializer.createReader(gzipA); final InputStream fileStreamB = new FileInputStream(fileB); final GZIPInputStream gzipB = new GZIPInputStream(fileStreamB); final Reader<CSVRecord> readerB = csVSerializer.createReader(gzipB); final OutputStream fileOutputStream = new FileOutputStream(tmpFile, false); final GZIPOutputStream gzOutStream = new GZIPOutputStream(fileOutputStream); final Writer<CSVRecord> writerAb = csVSerializer.createWriter(gzOutStream)) {
            Util.findComplement(readerA, readerB, comparator, writerAb);
        }
        int lineCount = 0;
        try (final InputStream fileStream = new FileInputStream(tmpFile); final InputStream gzipStream = new GZIPInputStream(fileStream); final InputStreamReader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8); final OutputStream os = Files.newOutputStream(Path.of(output.getAbsolutePath()), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE); final OutputStreamWriter ow = new OutputStreamWriter(new GZIPOutputStream(os), StandardCharsets.UTF_8); final BufferedWriter bw = new BufferedWriter(ow); final CSVPrinter csvPrinter = new CSVPrinter(bw, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            csvPrinter.printRecord(doc.getHeader());
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(decoder);
            for (CSVRecord record : records) {
                final Map<String, String> map = record.toMap();
                map.put("timestamp", dtf.format(creationTimeFileB.toInstant()));
                map.put("status", status.toString());
                csvPrinter.printRecord(map.values());
                if (lineCount++ % 1000 == 0) {
                    csvPrinter.flush();
                }
            }
            records = null; // free memory
            System.gc();
        }
        if (tmpFile.delete()) {
            tmpFile.deleteOnExit();
        }
        log.info("{} compared with {} has {} differences with status {}", fileA.getName(), fileB.getName(), lineCount, status);
        return lineCount;
    }

}
