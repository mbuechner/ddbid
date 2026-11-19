package de.ddb.labs.ddbid.cronjob.objects;

import de.ddb.labs.ddbid.model.Type;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class MaintenanceTables {

    private final String item;
    private final String person;
    private final String organization;

    MaintenanceTables(@Value("${ddbid.database.table.item}") String item,
            @Value("${ddbid.database.table.person}") String person,
            @Value("${ddbid.database.table.organization}") String organization) {
        this.item = item;
        this.person = person;
        this.organization = organization;
    }

    String identifier(Type type) {
        String name = switch (type) {
            case ITEM -> item;
            case PERSON -> person;
            case ORGANIZATION -> organization;
        };
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
