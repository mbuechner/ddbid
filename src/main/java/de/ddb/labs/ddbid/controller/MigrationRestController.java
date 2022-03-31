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
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.CorruptObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping({"download"})
public class MigrationRestController {

    @Autowired
    private GitHubService gitHub;

    @GetMapping
    @RequestMapping("migration/{commit}/{date}")
    public void listFiles(@PathVariable("commit") String commit, @PathVariable("date") String date, HttpServletResponse response) throws IOException, IncorrectObjectTypeException, CorruptObjectException, GitAPIException {
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + date + "-" + GitHubService.FILE_NAME + "\"");
        response.setContentType(MediaType.TEXT_PLAIN_VALUE);
        IOUtils.copyLarge(gitHub.getFile(commit), response.getOutputStream());
    }
}
