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
 * Mixin for base vectors file option.
 * Provides a required option for specifying the base vectors file path
 * with optional inline range specification support.
 */
public class BaseVectorsFileOption {

    @CommandLine.Option(
        names = {"-b", "--base"},
        description = "Base vectors file path (supports inline range e.g., file.fvec:1000 or file.fvec:[0,1000))",
        required = true,
        converter = BaseVectorsConverter.class
    )
    private BaseVectors baseVectors;

    /**
     * Gets the base vectors configuration
     * @return The parsed base vectors configuration
     */
    public BaseVectors getBaseVectors() {
        return baseVectors;
    }

    /**
     * Gets the path to the base vectors file
     * @return The normalized path to the base vectors file
     */
    public Path getBasePath() {
        return baseVectors.path();
    }

    /**
     * Gets the inline range specification if present
     * @return The range specification or null if not specified
     */
    public String getInlineRange() {
        return baseVectors.rangeSpec();
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return baseVectors.rangeSpec() != null && !baseVectors.rangeSpec().isEmpty();
    }

    /**
     * Validates that the base vectors file exists
     * @throws IllegalArgumentException if the file doesn't exist
     */
    public void validateBaseVectors() {
        if (!Files.exists(baseVectors.path())) {
            throw new IllegalArgumentException("Base vectors file does not exist: " + baseVectors.path());
        }
    }

    /**
     * Gets the normalized path as a string
     * @return The normalized path string
     */
    public String getNormalizedPath() {
        return baseVectors.path().normalize().toString();
    }

    @Override
    public String toString() {
        return "BaseVectorsFileOption{baseVectors=" + baseVectors + "}";
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