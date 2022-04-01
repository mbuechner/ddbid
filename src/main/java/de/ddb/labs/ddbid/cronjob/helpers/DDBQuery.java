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
package de.ddb.labs.ddbid.cronjob.helpers;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DDBQuery {

    @Autowired
    private OkHttpClient httpClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Value(value = "${ddbid.apikey}")
    private String apiKey;

    private List<String> searchValues = new ArrayList<>();
    private List<FACET> facetValues = new ArrayList<>();

    private JsonNode jn = null;
    private List<Facets> facets = null;

    public enum SECTOR {
        SEC_01("sec_01", "Archiv"),
        SEC_02("sec_02", "Bibliothek"),
        SEC_03("sec_03", "Denkmalpflege"),
        SEC_04("sec_04", "Forschung"),
        SEC_05("sec_05", "Mediathek"),
        SEC_06("sec_06", "Museum"),
        SEC_07("sec_07", "Sonstige");

        private final String naturalName;
        private final String shortName;

        SECTOR(String shortName, String sector) {
            this.shortName = shortName;
            this.naturalName = sector;
        }

        @Override
        public String toString() {
            return naturalName;
        }

        public String toShortString() {
            return shortName;
        }

        public String toNaturalNameString() {
            return toString();
        }

        public static SECTOR forShortName(String shortName) {
            for (SECTOR s : SECTOR.values()) {
                if (shortName.equals(s.toShortString())) {
                    return s;
                }
            }
            return null;
        }
    }

    public enum FACET {
        INGEST_ID("ingest_id"),
        REVISION_ID("revision_id"),
        LAST_UPDATE("last_update"),
        PROVIDER_ID("provider_id"),
        DATASET_ID("dataset_id"),
        DATASET_LABEL("dataset_label"),
        SUPPLIER_ID("supplier_id"),
        SOURCE_FORMAT("source_format"),
        AGGREGATOR_ID("aggregator_id"),
        AGGREGATOR_NAME("aggregator_name");

        private final String facet;

        FACET(String facet) {
            this.facet = facet;
        }

        @Override
        public String toString() {
            return facet;
        }

        public static FACET forName(String facet) {
            for (FACET f : FACET.values()) {
                if (facet.equalsIgnoreCase(f.toString())) {
                    return f;
                }
            }
            return null;
        }
    }

    public DDBQuery init() {
        searchValues = new ArrayList<>();
        facetValues = new ArrayList<>();

        jn = null;
        facets = null;
        return this;
    }

    public DDBQuery addSearchValue(String value) {
        searchValues.add(value);
        return this;
    }

    public DDBQuery addFacetValue(FACET value) {
        facetValues.add(value);
        return this;
    }

    public String getSearchText() {
        final StringBuffer sb = new StringBuffer();
        for (String searchValue : searchValues) {
            if (sb.length() > 0) {
                sb.append(" AND ");
            }
            sb.append(searchValue);
        }
        return sb.toString();
    }

    public String getSearchQuery() throws UnsupportedEncodingException {

        final StringBuffer sb = new StringBuffer()
                .append("https://api.deutsche-digitale-bibliothek.de/search")
                .append("?query=")
                .append(URLEncoder.encode(getSearchText(), "UTF-8"))
                .append("&rows=0")
                .append("&facet.limit=")
                .append(Integer.MAX_VALUE);

        for (FACET FACET : facetValues) {
            sb.append("&facet=");
            sb.append(FACET.toString());
        }

        return sb.toString();
    }

    public void run() throws UnsupportedEncodingException, IOException {
        if (this.jn == null) {
            log.debug("GET " + getSearchQuery());
            final Request request = new Request.Builder().url(getSearchQuery())
                    .addHeader("Accept", "application/json")
                    .addHeader("Authorization", "OAuth oauth_consumer_key=\"" + apiKey + "\"")
                    .build();
            try (final Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    this.jn = objectMapper.readTree(response.body().byteStream());
                }
            }
            parseFacets();
        }
    }

    public Map<String, List<String>> getFacetValues() throws IOException {

        final Map<String, List<String>> resultMap = new HashMap<>();

        for (Facets f : this.facets) {
            if (facetValues.contains(FACET.forName(f.getField()))) {
                resultMap.put(f.getField(), new ArrayList<>());
                for (Map.Entry<String, Integer> entry : f.getFacetValues().entrySet()) {
                    resultMap.get(f.getField()).add(entry.getKey());
                }
            }
        }

        return resultMap;
    }

    public int getNumberOfResults() {
        final JsonPointer jp = JsonPointer.compile("/numberOfResults");
        final JsonNode branch = this.jn.at(jp);
        return branch.asInt();
    }

    private void parseFacets() {

        final JsonPointer jp = JsonPointer.compile("/facets");
        final JsonNode branch = this.jn.at(jp);

        final CollectionType javaType = objectMapper.getTypeFactory().constructCollectionType(List.class, Facets.class);
        this.facets = objectMapper.convertValue(branch, javaType);
    }

    private void clearZeroValues(List<Facets> facets) {
        for (Iterator<Facets> it = facets.iterator(); it.hasNext();) {
            Facets entry = it.next();
            if (entry.getNumberOfFacets() == 0) {
                it.remove();
            }
        }
    }

    public List<Facets> getFacets() {
        return facets;
    }

}
