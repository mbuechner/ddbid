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
package de.ddb.labs.ddbid.model.person;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.Status;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.Getter;

/**
 *
 * @author michael
 */
@Data
public class PersonDoc extends Doc {

    @JsonIgnore
    private Timestamp timestamp;
    private String id;
    @JsonIgnore
    private Status status;
    @Getter(lazy = true)
    private final List<String> variant_id = new ArrayList<>();
    private String preferredName;
    private String type;

    public static List<String> getStaticHeader() {
        final List<String> l = new ArrayList<>();
        l.add("timestamp");
        l.add("id");
        l.add("status");
        l.add("variant_id");
        l.add("preferredName");
        l.add("type");
        return l;
    }

    @Override
    public List<String> getHeader() {
        return getStaticHeader();
    }

    @Override
    public List<Object> getData(Timestamp timestamp, Status status) {
        setTimestamp(timestamp);
        setStatus(status);
        return getData();
    }

    @Override
    public List<Object> getData() {
        final List<Object> l = new ArrayList<>();
        l.add(getTimestamp());
        l.add(getId());
        l.add(getStatus());
        l.add(getVariant_id().toString());
        l.add(getPreferredName());
        l.add(getType());
        return l;
    }
}
