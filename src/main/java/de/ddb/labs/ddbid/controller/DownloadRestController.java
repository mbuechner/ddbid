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
package de.ddb.labs.ddbid.controller;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class DownloadRestController<T> {

    @Value(value = "${ddbid.datapath.item}")
    private String itemDataPath;

    @Value(value = "${ddbid.datapath.person}")
    private String personDataPath;

    @Value(value = "download/${ddbid.datapath.organization}")
    private String organizationDataPath;

    @GetMapping
    @RequestMapping("download/{type:.+}/{filename:.+}")
    public ResponseEntity<Resource> listFiles(@PathVariable("type") String type, @PathVariable("filename") String filename) throws IOException {

        final HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Disposition", "attachment; filename=\"" + filename + "\"");

        if (type.equals("item")) {
            final Set s = Stream.of(new File(itemDataPath).listFiles())
                    .filter(file -> !file.isDirectory())
                    .filter(file -> file.getName().endsWith(".gz"))
                    .map(File::getName)
                    .collect(Collectors.toSet());
            if (s.contains(filename)) {
                final File file = new File(itemDataPath + filename);
                final Path path = Paths.get(file.getAbsolutePath());
                final ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(path));
                return ResponseEntity.ok()
                        .headers(responseHeaders)
                        .contentLength(file.length())
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .body(resource);
            }

        } else if (type.equals("person")) {
            final Set s = Stream.of(new File(personDataPath).listFiles())
                    .filter(file -> !file.isDirectory())
                    .filter(file -> file.getName().endsWith(".gz"))
                    .map(File::getName)
                    .collect(Collectors.toSet());
            if (s.contains(filename)) {
                final File file = new File(personDataPath + filename);
                final Path path = Paths.get(file.getAbsolutePath());
                final ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(path));
                return ResponseEntity.ok()
                        .headers(responseHeaders)
                        .contentLength(file.length())
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .body(resource);
            }
        } else if (type.equals("organization")) {
            final Set s = Stream.of(new File(organizationDataPath).listFiles())
                    .filter(file -> !file.isDirectory())
                    .filter(file -> file.getName().endsWith(".gz"))
                    .map(File::getName)
                    .collect(Collectors.toSet());
            if (s.contains(filename)) {
                final File file = new File(organizationDataPath + filename);
                final Path path = Paths.get(file.getAbsolutePath());
                final ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(path));
                return ResponseEntity.ok()
                        .headers(responseHeaders)
                        .contentLength(file.length())
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .body(resource);
            }
        }

        return ResponseEntity.notFound().build();
    }
}
