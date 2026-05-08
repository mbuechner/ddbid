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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.util.StreamUtils;

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

        if (type.equals("item")) {
            final Set<String> s = getDumpFileNames(itemDataPath);
            if (s.contains(filename)) {
                final File file = new File(itemDataPath, filename);
                try (final InputStream is = new FileInputStream(file)) {

                    response.setContentLengthLong(file.length());
                    response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");
                    response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

                    StreamUtils.copy(is, response.getOutputStream());
                }
            }

        } else if (type.equals("person")) {
            final Set<String> s = getDumpFileNames(personDataPath);
            if (s.contains(filename)) {
                final File file = new File(personDataPath, filename);
                try (final InputStream is = new FileInputStream(file)) {

                    response.setContentLengthLong(file.length());
                    response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");
                    response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

                    StreamUtils.copy(is, response.getOutputStream());
                }
            }
        } else if (type.equals("organization")) {
            final Set<String> s = getDumpFileNames(organizationDataPath);
            if (s.contains(filename)) {
                final File file = new File(organizationDataPath, filename);
                try (final InputStream is = new FileInputStream(file)) {

                    response.setContentLengthLong(file.length());
                    response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");
                    response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

                    StreamUtils.copy(is, response.getOutputStream());
                }
            }
        } else {

            response.sendError(404);
        }
    }

    private Set<String> getDumpFileNames(String dataPath) {
        final File[] files = new File(dataPath).listFiles();
        if (files == null) {
            return Collections.emptySet();
        }
        return Stream.of(files)
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(".gz"))
                .map(File::getName)
                .collect(Collectors.toSet());
    }
}
