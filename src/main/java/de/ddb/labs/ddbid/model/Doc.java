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
package de.ddb.labs.ddbid.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Data;
import lombok.Getter;

/**
 *
 * @author michael
 */
@Data
public class Doc {

    public enum Status {
        NEW("NEW"), MISSING("MISSING");

        @Getter
        private final String status;

        private Status(String status) {
            this.status = status;
        }
    }

    @JsonIgnore
    private Timestamp timestamp;
    private String id;
    @JsonIgnore
    private Status status;
    private String dataset_id;
    private String[] label;
    private String[] provider_id;
    private String supplier_id;

    public static List<String> getHeader() {
        final List<String> l = new ArrayList<>();
        l.add("timestamp");
        l.add("id");
        l.add("status");
        l.add("dataset_id");
        l.add("label");
        l.add("provider_id");
        l.add("supplier_id");
        return l;
    }

    public List<Object> getData(Timestamp timestamp, Status status) {
        setTimestamp(timestamp);
        setStatus(status);
        return getData();
    }

    public List<Object> getData() {
        final List<Object> l = new ArrayList<>();
        l.add(getTimestamp());
        l.add(getId());
        l.add(getStatus());
        l.add(getDataset_id());
        l.add(Arrays.toString(getLabel()));
        l.add(Arrays.toString(getProvider_id()));
        l.add(getSupplier_id());
        return l;
    }
}
