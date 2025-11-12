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
 * Mixin for base vectors input file option (optional variant).
 * Provides an optional option for specifying the base vectors file path with inline range support.
 */
public class BaseVectorsInputFileOption {

    @CommandLine.Option(
        names = {"--base"},
        description = "Base vectors input file path (supports inline range e.g., file.fvec:1000 or file.fvec:[0,1000))",
        converter = BaseVectorsConverter.class
    )
    private BaseVectors baseVectors;

    /**
     * Gets the base vectors file path
     * @return The base vectors file path, or null if not specified
     */
    public Path getBasePath() {
        return baseVectors != null ? baseVectors.path() : null;
    }

    /**
     * Gets the normalized base vectors file path
     * @return The normalized path to the base vectors file, or null if not specified
     */
    public Path getNormalizedBasePath() {
        return baseVectors != null ? baseVectors.path().normalize() : null;
    }

    /**
     * Gets the inline range specification if present
     * @return The range specification or null if not specified
     */
    public String getInlineRange() {
        return baseVectors != null ? baseVectors.rangeSpec() : null;
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return baseVectors != null && baseVectors.rangeSpec() != null && !baseVectors.rangeSpec().isEmpty();
    }

    /**
     * Checks if base path was specified
     * @return true if specified, false otherwise
     */
    public boolean isSpecified() {
        return baseVectors != null;
    }

    /**
     * Validates that the base vectors file exists
     * @throws IllegalArgumentException if the file doesn't exist
     */
    public void validateBaseVectors() {
        if (baseVectors != null && !Files.exists(baseVectors.path())) {
            throw new IllegalArgumentException("Base vectors input file does not exist: " + baseVectors.path());
        }
    }

    @Override
    public String toString() {
        return "BaseVectorsInputFileOption{baseVectors=" + baseVectors + "}";
    }

    /**
     * Record representing base vectors file configuration
     */
    public record BaseVectors(Path path, String rangeSpec) {
        public BaseVectors {
            if (path == null) {
                throw new IllegalArgumentException("Base vectors path cannot be null");
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
     * Custom converter for parsing base vectors file specification
     */
    public static class BaseVectorsConverter implements CommandLine.ITypeConverter<BaseVectors> {
        @Override
        public BaseVectors convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Base vectors file path cannot be empty");
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
                    return new BaseVectors(Paths.get(pathPart), potentialRange);
                }
            }

            // No range specification found
            return new BaseVectors(Paths.get(value), null);
        }
    }
}
