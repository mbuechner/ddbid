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
package de.ddb.labs.ddbid.controller;

import java.time.Instant;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(QueryTimeoutException.class)
    public ResponseEntity<Map<String, Object>> handleQueryTimeout(QueryTimeoutException e) {
        log.warn("Database query timed out", e);
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(errorBody(
                        "Database query timed out",
                        "The request took too long. Add a more specific filter or check database indexes."));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<Map<String, Object>> handleDataAccess(DataAccessException e) {
        log.warn("Database access failed", e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(errorBody(
                        "Database access failed",
                        "The database request could not be completed. Check the server logs for the failing SQL query."));
    }

    private static Map<String, Object> errorBody(String error, String message) {
        return Map.of(
                "timestamp", Instant.now().toString(),
                "error", error,
                "message", message);
    }
}