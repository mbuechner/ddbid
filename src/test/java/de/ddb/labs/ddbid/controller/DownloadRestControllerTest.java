package de.ddb.labs.ddbid.controller;

import static org.assertj.core.api.Assertions.assertThat;

import de.ddb.labs.ddbid.service.DownloadCatalogService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

class DownloadRestControllerTest {

    @TempDir
    Path directory;

    private final DownloadRestController controller = new DownloadRestController(new DownloadCatalogService());

    @BeforeEach
    void configureDownloadDirectory() {
        ReflectionTestUtils.setField(controller, "itemDataPath", directory.toString());
    }

    @Test
    void returnsNotFoundWhenRequestedDumpIsMissing() throws Exception {
        final MockHttpServletResponse response = new MockHttpServletResponse();

        controller.getDdbIdFile("item", "missing.csv.gz", response);

        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void returnsNotFoundWhenDumpDirectoryIsMissing() throws Exception {
        ReflectionTestUtils.setField(controller, "itemDataPath", directory.resolve("missing").toString());
        final MockHttpServletResponse response = new MockHttpServletResponse();

        controller.getDdbIdFile("item", "dump.csv.gz", response);

        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void streamsDumpWithAnAttachmentFilename() throws Exception {
        final String filename = "dump-ä.csv.gz";
        final Path dump = directory.resolve(filename);
        try (final GZIPOutputStream output = new GZIPOutputStream(Files.newOutputStream(dump))) {
            output.write(42);
        }
        final MockHttpServletResponse response = new MockHttpServletResponse();

        controller.getDdbIdFile("item", filename, response);

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getContentLengthLong()).isEqualTo(Files.size(dump));
        assertThat(response.getContentAsByteArray()).isEqualTo(Files.readAllBytes(dump));
        assertThat(ContentDisposition.parse(response.getHeader(HttpHeaders.CONTENT_DISPOSITION)).getFilename())
                .isEqualTo(filename);
    }
}
