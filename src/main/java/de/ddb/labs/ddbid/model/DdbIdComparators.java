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
