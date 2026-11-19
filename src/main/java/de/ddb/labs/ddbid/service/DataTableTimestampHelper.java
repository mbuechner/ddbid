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
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.function.Supplier;

final class DataTableTimestampHelper {

    private static final DateTimeFormatter SQL_TIMESTAMP = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-M-d H:m:s")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT);

    private DataTableTimestampHelper() {
    }

    static TimestampFilter filter(String value, Supplier<Timestamp> latestTimestamp) {
        String normalized = normalize(value);
        if (normalized == null) {
            // No timestamp requested: default to the latest available timestamp so that
            // every query carries a WHERE clause and avoids a full-table scan.
            Timestamp latest = latestTimestamp != null ? latestTimestamp.get() : null;
            return latest != null ? TimestampFilter.exact(latest) : TimestampFilter.inactive();
        }
        if ("-1".equals(normalized)) {
            // Explicit sentinel value: caller wants all timestamps (no filter).
            return TimestampFilter.inactive();
        }

        try {
            if (isDateOnly(normalized)) {
                LocalDate date = LocalDate.parse(normalized.substring(0, 10));
                return TimestampFilter.range(
                        Timestamp.valueOf(date.atStartOfDay()),
                        Timestamp.valueOf(date.plusDays(1).atStartOfDay()));
            }
            return TimestampFilter.exact(parse(normalized));
        } catch (DateTimeException | IllegalArgumentException e) {
            return TimestampFilter.invalidFilter();
        }
    }

    private static Timestamp parse(String value) {
        try {
            return new Timestamp(Long.parseLong(value));
        } catch (NumberFormatException e) {
            // Try textual timestamps next.
        }

        final String isoTimestamp = value.replace(' ', 'T').replaceFirst("([+-]\\d{2})(\\d{2})$", "$1:$2");
        try {
            return Timestamp.from(OffsetDateTime.parse(isoTimestamp).toInstant());
        } catch (DateTimeParseException e) {
            // An offset-free value represents local database time.
        }

        return Timestamp.valueOf(value.contains(" ")
                ? LocalDateTime.parse(value, SQL_TIMESTAMP)
                : LocalDateTime.parse(value));
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
