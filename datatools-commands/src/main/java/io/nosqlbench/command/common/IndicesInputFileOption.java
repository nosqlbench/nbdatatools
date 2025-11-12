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
 * Mixin for indices input file option.
 * Provides an optional option for specifying the neighbor indices input file path with inline range support.
 */
public class IndicesInputFileOption {

    @CommandLine.Option(
        names = {"--indices"},
        description = "Input file path for neighbor indices (supports inline range e.g., file.ivec:1000 or file.ivec:[0,1000))",
        converter = IndicesConverter.class
    )
    private Indices indices;

    /**
     * Gets the indices file path
     * @return The indices file path, or null if not specified
     */
    public Path getIndicesPath() {
        return indices != null ? indices.path() : null;
    }

    /**
     * Gets the normalized indices file path
     * @return The normalized path to the indices file, or null if not specified
     */
    public Path getNormalizedIndicesPath() {
        return indices != null ? indices.path().normalize() : null;
    }

    /**
     * Gets the inline range specification if present
     * @return The range specification or null if not specified
     */
    public String getInlineRange() {
        return indices != null ? indices.rangeSpec() : null;
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return indices != null && indices.rangeSpec() != null && !indices.rangeSpec().isEmpty();
    }

    /**
     * Checks if indices path was specified
     * @return true if specified, false otherwise
     */
    public boolean isSpecified() {
        return indices != null;
    }

    /**
     * Validates that the indices file exists
     * @throws IllegalArgumentException if the file doesn't exist
     */
    public void validateIndicesInput() {
        if (indices != null && !Files.exists(indices.path())) {
            throw new IllegalArgumentException("Indices input file does not exist: " + indices.path());
        }
    }

    @Override
    public String toString() {
        return "IndicesInputFileOption{indices=" + indices + "}";
    }

    /**
     * Record representing indices file configuration
     */
    public record Indices(Path path, String rangeSpec) {
        public Indices {
            if (path == null) {
                throw new IllegalArgumentException("Indices path cannot be null");
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
     * Custom converter for parsing indices file specification
     */
    public static class IndicesConverter implements CommandLine.ITypeConverter<Indices> {
        @Override
        public Indices convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Indices file path cannot be empty");
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
                    return new Indices(Paths.get(pathPart), potentialRange);
                }
            }

            // No range specification found
            return new Indices(Paths.get(value), null);
        }
    }
}

