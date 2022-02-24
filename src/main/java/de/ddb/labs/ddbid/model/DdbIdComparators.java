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

import de.ddb.labs.ddbid.model.paging.Direction;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

public final class DdbIdComparators {

    @EqualsAndHashCode
    @AllArgsConstructor
    @Getter
    static class Key {

        String name;
        Direction dir;
    }

    static Map<Key, Comparator<DdbId>> map = new HashMap<>();

    static {

        map.put(new Key("timestamp", Direction.asc), Comparator.comparing(DdbId::getTimestamp));
        map.put(new Key("timestamp", Direction.desc), Comparator.comparing(DdbId::getTimestamp).reversed());

        map.put(new Key("id", Direction.asc), Comparator.comparing(DdbId::getId));
        map.put(new Key("id", Direction.desc), Comparator.comparing(DdbId::getId).reversed());

        map.put(new Key("status", Direction.asc), Comparator.comparing(DdbId::getStatus));
        map.put(new Key("status", Direction.desc), Comparator.comparing(DdbId::getStatus).reversed());

        map.put(new Key("dataset_id", Direction.asc), Comparator.comparing(DdbId::getDataset_id));
        map.put(new Key("dataset_id", Direction.desc), Comparator.comparing(DdbId::getDataset_id).reversed());

        map.put(new Key("label", Direction.asc), Comparator.comparing(DdbId::getLabel));
        map.put(new Key("label", Direction.desc), Comparator.comparing(DdbId::getLabel).reversed());

        map.put(new Key("provider_id", Direction.asc), Comparator.comparing(DdbId::getProvider_id));
        map.put(new Key("provider_id", Direction.desc), Comparator.comparing(DdbId::getProvider_id).reversed());

        map.put(new Key("supplier_id", Direction.asc), Comparator.comparing(DdbId::getSupplier_id));
        map.put(new Key("supplier_id", Direction.desc), Comparator.comparing(DdbId::getSupplier_id).reversed());
    }

    public static Comparator<DdbId> getComparator(String name, Direction dir) {
        return map.get(new Key(name, dir));
    }

    private DdbIdComparators() {
    }
}
