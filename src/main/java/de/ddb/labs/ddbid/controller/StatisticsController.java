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

import de.ddb.labs.ddbid.service.StatisticsService;
import de.ddb.labs.ddbid.service.StatisticsService.StatisticsData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping({"/statistics.html", "/statistics"})
public class StatisticsController {

    @Autowired
    private StatisticsService statisticsService;

    @GetMapping
    public String statistics() {
        return "statistics";
    }

    @ResponseBody
    @GetMapping("data")
    public StatisticsData data(@RequestParam(value = "refresh", defaultValue = "false") boolean refresh) {
        return statisticsService.getStatisticsData(refresh);
    }
}
