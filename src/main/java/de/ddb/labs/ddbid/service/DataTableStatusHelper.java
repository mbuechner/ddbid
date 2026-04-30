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

import de.ddb.labs.ddbid.model.Status;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

final class DataTableStatusHelper {

    static final String ALL = "ALL";
    private static final String DEFAULT_STATUS = Status.MISSING.name();
    private static final Set<String> VALID_STATUSES = Arrays.stream(Status.values())
            .map(Enum::name)
            .collect(Collectors.toUnmodifiableSet());

    private DataTableStatusHelper() {
    }

    static String status(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_STATUS;
        }

        String normalized = value.trim().toUpperCase(Locale.ROOT);
        if (ALL.equals(normalized) || VALID_STATUSES.contains(normalized)) {
            return normalized;
        }
        return DEFAULT_STATUS;
    }
}
