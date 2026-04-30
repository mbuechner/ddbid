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

import java.util.List;
import java.util.stream.Collectors;

final class DataTableSqlHelper {

    private DataTableSqlHelper() {
    }

    static String selectColumns(List<String> fields) {
        return fields.stream()
                .map(DataTableSqlHelper::quote)
                .collect(Collectors.joining(", "));
    }

    static String quote(String field) {
        return "\"" + field + "\"";
    }
}
