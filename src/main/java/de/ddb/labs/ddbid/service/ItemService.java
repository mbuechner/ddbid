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
package de.ddb.labs.ddbid.service;

import de.ddb.labs.ddbid.database.Database;
import de.ddb.labs.ddbid.model.item.Item;
import de.ddb.labs.ddbid.model.item.ItemDoc;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ItemService extends AbstractDataTableEntityService<Item> {

    private final Database database;

    @Value("${ddbid.database.table.item}")
    private String tableName;

    @Override
    protected Logger logger() {
        return log;
    }

    @Override
    protected Database database() {
        return database;
    }

    @Override
    protected String entityLabel() {
        return "item";
    }

    @Override
    protected String tableName() {
        return tableName;
    }

    @Override
    protected List<String> fields() {
        return ItemDoc.getStaticHeader();
    }

    @Override
    protected Class<Item> rowType() {
        return Item.class;
    }

    @Override
    protected Map<String, List<String>> loadFilterOptions() {
        return DataTableFilterOptionsHelper.itemOptions(log, database, tableName);
    }
}
