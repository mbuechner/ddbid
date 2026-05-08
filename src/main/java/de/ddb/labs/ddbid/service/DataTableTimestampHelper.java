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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.function.Supplier;

final class DataTableTimestampHelper {

    private DataTableTimestampHelper() {
    }

    static TimestampFilter filter(String value, Supplier<Timestamp> latestTimestamp) {
        String normalized = normalize(value);
        if (normalized == null) {
            return TimestampFilter.inactive();
        }
        if ("-1".equals(normalized)) {
            return TimestampFilter.inactive();
        }

        if (isDateOnly(normalized)) {
            LocalDate date = LocalDate.parse(normalized.substring(0, 10));
            return TimestampFilter.range(
                    Timestamp.valueOf(date.atStartOfDay()),
                    Timestamp.valueOf(date.plusDays(1).atStartOfDay()));
        }

        Timestamp timestamp;
        try {
            timestamp = parse(normalized);
        } catch (IllegalArgumentException e) {
            return TimestampFilter.invalidFilter();
        }
        return TimestampFilter.exact(timestamp);
    }

    static Timestamp parse(String value) {
        String normalized = normalize(value);
        if (normalized == null || "-1".equals(normalized)) {
            return null;
        }

        try {
            return new Timestamp(Long.parseLong(normalized));
        } catch (NumberFormatException e) {
            // Fall through to ISO parsing. DataTables timestamp menus can receive ISO strings from Jackson.
        }

        try {
            return Timestamp.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            // Fall through to ISO-like local date-time parsing.
        }

        try {
            String localTimestamp = normalized.replaceFirst("(Z|[+-]\\d{2}:?\\d{2})$", "");
            return Timestamp.valueOf(LocalDateTime.parse(localTimestamp));
        } catch (DateTimeParseException | IllegalArgumentException e) {
            // Fall through to instant parsing.
        }

        try {
            return Timestamp.from(Instant.parse(normalized));
        } catch (DateTimeParseException e) {
            // Fall through to offset / local timestamp parsing.
        }

        try {
            return Timestamp.from(OffsetDateTime.parse(normalized).toInstant());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Unsupported timestamp value: " + normalized, e);
        }
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    private static boolean isDateOnly(String value) {
        return value != null && value.matches("\\d{4}-\\d{2}-\\d{2}(\\s*\\(CW\\d+\\))?");
    }

    record TimestampFilter(Timestamp startInclusive, Timestamp endExclusive, boolean invalidValue) {

        static TimestampFilter inactive() {
            return new TimestampFilter(null, null, false);
        }

        static TimestampFilter invalidFilter() {
            return new TimestampFilter(null, null, true);
        }

        static TimestampFilter exact(Timestamp timestamp) {
            return new TimestampFilter(timestamp, null, false);
        }

        static TimestampFilter range(Timestamp startInclusive, Timestamp endExclusive) {
            return new TimestampFilter(startInclusive, endExclusive, false);
        }

        boolean active() {
            return invalidValue || startInclusive != null;
        }

        boolean invalid() {
            return invalidValue;
        }

        boolean range() {
            return endExclusive != null;
        }
    }
}
