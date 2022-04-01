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

import de.ddb.labs.ddbid.service.GitHubService;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("download")
@Slf4j
public class DownloadRestController<T> {

    @Value(value = "${ddbid.datapath.item}")
    private String itemDataPath;

    @Value(value = "${ddbid.datapath.person}")
    private String personDataPath;

    @Value(value = "${ddbid.datapath.organization}")
    private String organizationDataPath;

    @Autowired
    private GitHubService gitHub;

    @GetMapping
    @RequestMapping("ddbid/{type:.+}/{filename:.+}")
    public void getDdbIdFile(@PathVariable("type") String type, @PathVariable("filename") String filename, HttpServletResponse response) throws IOException {

        if (type.equals("item")) {
            final Set s = Stream.of(new File(itemDataPath).listFiles())
                    .filter(file -> !file.isDirectory())
                    .filter(file -> file.getName().endsWith(".gz"))
                    .map(File::getName)
                    .collect(Collectors.toSet());
            if (s.contains(filename)) {
                final File file = new File(itemDataPath + filename);
                final InputStream is = new FileInputStream(file);

                response.setContentLengthLong(file.length());
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");
                response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

                IOUtils.copyLarge(is, response.getOutputStream());
            }

        } else if (type.equals("person")) {
            final Set s = Stream.of(new File(personDataPath).listFiles())
                    .filter(file -> !file.isDirectory())
                    .filter(file -> file.getName().endsWith(".gz"))
                    .map(File::getName)
                    .collect(Collectors.toSet());
            if (s.contains(filename)) {
                final File file = new File(personDataPath + filename);
                final InputStream is = new FileInputStream(file);

                response.setContentLengthLong(file.length());
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");
                response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

                IOUtils.copyLarge(is, response.getOutputStream());
            }
        } else if (type.equals("organization")) {
            final Set s = Stream.of(new File(organizationDataPath).listFiles())
                    .filter(file -> !file.isDirectory())
                    .filter(file -> file.getName().endsWith(".gz"))
                    .map(File::getName)
                    .collect(Collectors.toSet());
            if (s.contains(filename)) {
                final File file = new File(organizationDataPath + filename);
                final InputStream is = new FileInputStream(file);

                response.setContentLengthLong(file.length());
                response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"");
                response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);

                IOUtils.copyLarge(is, response.getOutputStream());
            }
        } else {

            response.sendError(404);
        }
    }

    @GetMapping
    @RequestMapping("migration/{commit}/{date}")
    public void getMigrationFile(@PathVariable("commit") String commit, @PathVariable("date") String date, HttpServletResponse response) throws IOException, IncorrectObjectTypeException, CorruptObjectException, GitAPIException {
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + date + "-" + GitHubService.FILE_NAME + "\"");
        response.setContentType(MediaType.TEXT_PLAIN_VALUE);
        IOUtils.copyLarge(gitHub.getFile(commit), response.getOutputStream());
    }
}
