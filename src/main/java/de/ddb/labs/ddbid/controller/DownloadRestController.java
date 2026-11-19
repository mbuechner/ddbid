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
package de.ddb.labs.ddbid.controller;

import de.ddb.labs.ddbid.service.DownloadCatalogService;
import de.ddb.labs.ddbid.service.DownloadCatalogService.DownloadCatalog;
import java.io.IOException;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("download")
@RequiredArgsConstructor
public class DownloadRestController {

    @Value(value = "${ddbid.datapath.item}")
    private String itemDataPath;

    @Value(value = "${ddbid.datapath.person}")
    private String personDataPath;

    @Value(value = "${ddbid.datapath.organization}")
    private String organizationDataPath;

    private final DownloadCatalogService downloadCatalogService;

    @GetMapping("catalog")
    public DownloadCatalog catalog(@RequestParam(value = "refresh", defaultValue = "false") boolean refresh) {
        return downloadCatalogService.getCatalog(refresh);
    }

    @GetMapping("ddbid/{type:.+}/{filename:.+}")
    public void getDdbIdFile(@PathVariable("type") String type, @PathVariable("filename") String filename, HttpServletResponse response) throws IOException {
        final String dataPath = switch (type) {
            case "item" -> itemDataPath;
            case "person" -> personDataPath;
            case "organization" -> organizationDataPath;
            default -> null;
        };
        if (dataPath == null || !filename.endsWith(".gz")) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        final Path directory;
        final Path file;
        try {
            directory = Path.of(dataPath).toRealPath();
            file = directory.resolve(filename);
        } catch (InvalidPathException | NoSuchFileException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        if (!directory.equals(file.getParent()) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        try {
            streamFile(file, filename, response);
        } catch (NoSuchFileException e) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private static void streamFile(Path file, String filename, HttpServletResponse response) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            response.setContentLengthLong(channel.size());
            response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    ContentDisposition.attachment().filename(filename, StandardCharsets.UTF_8).build().toString());
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            StreamUtils.copy(Channels.newInputStream(channel), response.getOutputStream());
        }
    }
}
