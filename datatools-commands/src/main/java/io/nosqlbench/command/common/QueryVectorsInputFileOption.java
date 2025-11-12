/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import picocli.CommandLine;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

/**
 * Mixin for query vectors input file option (optional variant).
 * Provides an optional option for specifying the query vectors file path with inline range support.
 */
public class QueryVectorsInputFileOption {

    @CommandLine.Option(
        names = {"--query"},
        description = "Query vectors input file path (supports inline range e.g., file.fvec:1000 or file.fvec:[0,1000))",
        converter = QueryVectorsConverter.class
    )
    private QueryVectors queryVectors;

    /**
     * Gets the query vectors file path
     * @return The query vectors file path, or null if not specified
     */
    public Path getQueryPath() {
        return queryVectors != null ? queryVectors.path() : null;
    }

    /**
     * Gets the normalized query vectors file path
     * @return The normalized path to the query vectors file, or null if not specified
     */
    public Path getNormalizedQueryPath() {
        return queryVectors != null ? queryVectors.path().normalize() : null;
    }

    /**
     * Gets the inline range specification if present
     * @return The range specification or null if not specified
     */
    public String getInlineRange() {
        return queryVectors != null ? queryVectors.rangeSpec() : null;
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return queryVectors != null && queryVectors.rangeSpec() != null && !queryVectors.rangeSpec().isEmpty();
    }

    /**
     * Checks if query path was specified
     * @return true if specified, false otherwise
     */
    public boolean isSpecified() {
        return queryVectors != null;
    }

    /**
     * Validates that the query vectors file exists
     * @throws IllegalArgumentException if the file doesn't exist
     */
    public void validateQueryVectors() {
        if (queryVectors != null && !Files.exists(queryVectors.path())) {
            throw new IllegalArgumentException("Query vectors input file does not exist: " + queryVectors.path());
        }
    }

    @Override
    public String toString() {
        return "QueryVectorsInputFileOption{queryVectors=" + queryVectors + "}";
    }

    /**
     * Record representing query vectors file configuration
     */
    public record QueryVectors(Path path, String rangeSpec) {
        public QueryVectors {
            if (path == null) {
                throw new IllegalArgumentException("Query vectors path cannot be null");
            }
            path = path.normalize();
        }

        @Override
        public String toString() {
            if (rangeSpec != null && !rangeSpec.isEmpty()) {
                return path + ":" + rangeSpec;
            }
            return path.toString();
        }
    }

    /**
     * Custom converter for parsing query vectors file specification
     */
    public static class QueryVectorsConverter implements CommandLine.ITypeConverter<QueryVectors> {
        @Override
        public QueryVectors convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Query vectors file path cannot be empty");
            }

            // Check for inline range specification
            int colonIndex = value.lastIndexOf(':');
            if (colonIndex > 0) {
                // Check if this might be a range specification
                String potentialRange = value.substring(colonIndex + 1);
                // Simple heuristic: if it starts with a digit or '[', it's likely a range
                if (!potentialRange.isEmpty() &&
                    (Character.isDigit(potentialRange.charAt(0)) || potentialRange.charAt(0) == '[')) {
                    String pathPart = value.substring(0, colonIndex);
                    return new QueryVectors(Paths.get(pathPart), potentialRange);
                }
            }

            // No range specification found
            return new QueryVectors(Paths.get(value), null);
        }
    }
}

