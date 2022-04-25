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
package de.ddb.labs.ddbid.cronjob.helper;

import java.util.LinkedHashMap;
import lombok.Getter;
import lombok.Setter;

public class Facets {

    private LinkedHashMap<String, Integer> facetValues;
    
    @Getter
    @Setter
    private int numberOfFacets;
    
    @Getter
    @Setter
    private String field;

    public Facets(LinkedHashMap<String, Integer> facetValues, int numberOfFacets, String field) {
        this.facetValues = new LinkedHashMap<>(facetValues);
        this.numberOfFacets = numberOfFacets;
        this.field = field;
    }

    public Facets() {
    }
                
    public LinkedHashMap<String, Integer> getFacetValues() {
        return new LinkedHashMap<>(facetValues);
    }
    
    public void setFacetValues(LinkedHashMap[] hms) {
        if (facetValues == null) {
            facetValues = new LinkedHashMap<>();
        }
        for (LinkedHashMap hm : hms) {
            final String newKey = (String) hm.get("value");
            final int newValue = (int) hm.get("count");
            facetValues.put(newKey, newValue);
        }
    }
}
