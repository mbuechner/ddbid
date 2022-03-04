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
package de.ddb.labs.ddbid.controller.person;

import de.ddb.labs.ddbid.model.paging.Page;
import de.ddb.labs.ddbid.model.paging.PagingRequest;
import de.ddb.labs.ddbid.model.person.Person;
import de.ddb.labs.ddbid.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.Map;

@RestController
@RequestMapping("person")
public class PersonRestController {

    private final PersonService service;

    @Autowired
    public PersonRestController(PersonService ddbIdService) {
        this.service = ddbIdService;
    }

    @PostMapping
    public Page<Person> list(@RequestBody PagingRequest pagingRequest) {
        return service.getDdbIds(pagingRequest);
    }

    @PostMapping
    @RequestMapping("timestamp")
    public Map<String, Timestamp> timestamps() {
        return service.getTimestamps();
    }
}
