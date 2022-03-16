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
package de.ddb.labs.ddbid.model.item;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.ddb.labs.ddbid.model.Doc;
import de.ddb.labs.ddbid.model.Status;
import lombok.Data;
import lombok.Getter;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Data
public class ItemDoc extends Doc {

    @Getter(lazy = true)
    private final List<String> label = new ArrayList<>();
    @Getter(lazy = true)
    private final List<String> provider_id = new ArrayList<>();
    @JsonIgnore
    private Timestamp timestamp;
    private String id;
    @JsonIgnore
    private Status status;
    private String provider_item_id;
    private String dataset_id;
    private String sector_fct;
    private String supplier_id;

    public static List<String> getStaticHeader() {
        final List<String> l = new ArrayList<>();
        l.add("timestamp");
        l.add("id");
        l.add("status");
        l.add("provider_item_id");
        l.add("dataset_id");
        l.add("label");
        l.add("provider_id");
        l.add("sector_fct");
        l.add("supplier_id");
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
        l.add(getProvider_item_id());
        l.add(getDataset_id());
        l.add(getLabel().toString());
        l.add(getProvider_id().toString());
        l.add(getSector_fct());
        l.add(getSupplier_id());
        return l;
    }
}
