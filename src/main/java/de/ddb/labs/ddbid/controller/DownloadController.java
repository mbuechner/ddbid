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

import de.ddb.labs.ddbid.cronjob.CronJob;
import de.ddb.labs.ddbid.service.GitHubService;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping({"/download.html"})
public class DownloadController {

    private final static ThreadLocal<DecimalFormat> FORMATTER = new ThreadLocal<DecimalFormat>() {
        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("#,##0.#");
        }
    };

    @Value(value = "${ddbid.datapath.item}")
    private String itemDataPath;

    @Value(value = "${ddbid.datapath.person}")
    private String personDataPath;

    @Value(value = "${ddbid.datapath.organization}")
    private String organizationDataPath;

    @Autowired
    private GitHubService gitHub;

    @GetMapping
    public ModelAndView main() throws IOException, GitAPIException {

        final ModelAndView mav = new ModelAndView();
        mav.addObject("itemList", Stream.of(new File(itemDataPath).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(CronJob.OUTPUT_FILENAME_EXT))
                .filter(file -> !file.getName().startsWith(CronJob.COMPARE_OUTPUT_FILENAME_PREFIX))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toMap(File::getName, file -> readableFileSize(file.length()), (o1, o2) -> o1, LinkedHashMap::new)));
        mav.addObject("itemListCmp", Stream.of(new File(itemDataPath).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(CronJob.OUTPUT_FILENAME_EXT))
                .filter(file -> file.getName().startsWith(CronJob.COMPARE_OUTPUT_FILENAME_PREFIX))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toMap(File::getName, file -> readableFileSize(file.length()), (o1, o2) -> o1, LinkedHashMap::new)));
        mav.addObject("personList", Stream.of(new File(personDataPath).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(CronJob.OUTPUT_FILENAME_EXT))
                .filter(file -> !file.getName().startsWith(CronJob.COMPARE_OUTPUT_FILENAME_PREFIX))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toMap(File::getName, file -> readableFileSize(file.length()), (o1, o2) -> o1, LinkedHashMap::new)));
        mav.addObject("personListCmp", Stream.of(new File(personDataPath).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(CronJob.OUTPUT_FILENAME_EXT))
                .filter(file -> file.getName().startsWith(CronJob.COMPARE_OUTPUT_FILENAME_PREFIX))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toMap(File::getName, file -> readableFileSize(file.length()), (o1, o2) -> o1, LinkedHashMap::new)));
        mav.addObject("organizationList", Stream.of(new File(organizationDataPath).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(CronJob.OUTPUT_FILENAME_EXT))
                .filter(file -> !file.getName().startsWith(CronJob.COMPARE_OUTPUT_FILENAME_PREFIX))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toMap(File::getName, file -> readableFileSize(file.length()), (o1, o2) -> o1, LinkedHashMap::new)));
        mav.addObject("organizationListCmp", Stream.of(new File(organizationDataPath).listFiles())
                .filter(file -> !file.isDirectory())
                .filter(file -> file.getName().endsWith(CronJob.OUTPUT_FILENAME_EXT))
                .filter(file -> file.getName().startsWith(CronJob.COMPARE_OUTPUT_FILENAME_PREFIX))
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toMap(File::getName, file -> readableFileSize(file.length()), (o1, o2) -> o1, LinkedHashMap::new)));

        mav.addObject("commits", gitHub.getCommits());
        mav.setViewName("download");

        return mav;
    }

    public static String readableFileSize(long size) {
        if (size <= 0) {
            return "0";
        }
        final String[] units = new String[]{"byte", "KiB", "MiB", "GiB", "TiB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return FORMATTER.get().format(size / Math.pow(1024, digitGroups)) + units[digitGroups];
    }
}
