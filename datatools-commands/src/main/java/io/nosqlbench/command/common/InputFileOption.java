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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Shared input file option with optional inline range specification support.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class InputFileOption {

    /**
     * Immutable input file specification with optional inline range.
     * Implements CharSequence to allow direct use as a string in most contexts.
     * Supports file paths with optional range specifications in the format: {@code path/to/file:rangespec}
     * <p>
     * Examples:
     * <ul>
     *   <li>{@code input.fvec} - plain file path</li>
     *   <li>{@code input.fvec:1000} - first 1000 elements</li>
     *   <li>{@code input.fvec:[0,1000)} - elements 0 to 999</li>
     *   <li>{@code input.fvec:10..99} - elements 10 to 99 inclusive</li>
     * </ul>
     *
     * @param path            the input file path (never null)
     * @param inlineRangeSpec optional range specification extracted from path, or null
     */
    public record InputFile(Path path, String inlineRangeSpec) implements CharSequence {

        /**
         * Compact constructor with validation.
         */
        public InputFile {
            if (path == null) {
                throw new IllegalArgumentException("Input path cannot be null");
            }
        }

        /**
         * Creates an InputFile without inline range specification.
         */
        public InputFile(Path path) {
            this(path, null);
        }

        /**
         * Gets the normalized absolute path.
         */
        public Path normalizedPath() {
            return path.normalize().toAbsolutePath();
        }

        /**
         * Checks if an inline range specification was provided.
         */
        public boolean hasInlineRange() {
            return inlineRangeSpec != null && !inlineRangeSpec.isEmpty();
        }

        /**
         * Checks if the input file exists.
         */
        public boolean exists() {
            return Files.exists(path);
        }

        /**
         * Validates that the input file exists.
         */
        public void validate() {
            if (!exists()) {
                throw new IllegalStateException("Input file does not exist: " + path);
            }
        }

        // CharSequence implementation - delegates to path string

        @Override
        public int length() {
            return path.toString().length();
        }

        @Override
        public char charAt(int index) {
            return path.toString().charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return path.toString().subSequence(start, end);
        }

        /**
         * Returns the path as a string with inline range if present.
         */
        @Override
        public String toString() {
            if (hasInlineRange()) {
                return path + " (range: " + inlineRangeSpec + ")";
            }
            return path.toString();
        }
    }

    /**
     * Picocli type converter for {@link InputFile} specifications.
     * Parses input strings that may include inline range specifications.
     */
    public static class InputFileConverter implements CommandLine.ITypeConverter<InputFile> {

        @Override
        public InputFile convert(String value) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Input file path cannot be empty");
            }

            String[] parts = parsePathWithRange(value);
            Path actualPath = Paths.get(parts[0]);
            String rangeSpec = parts[1];

            return new InputFile(actualPath, rangeSpec);
        }

        /**
         * Parses a path string that may include an inline range specification.
         * Format: "path/to/file:rangespec" where rangespec uses the same formats as RangeOption.
         *
         * @param pathString the path string to parse
         * @return an array with [actualPath, rangeSpec], where rangeSpec may be null
         */
        private String[] parsePathWithRange(String pathString) {
            if (pathString == null || pathString.isEmpty()) {
                return new String[]{pathString, null};
            }

            // Find the last colon that's not part of a Windows drive letter (e.g., C:)
            int colonIndex = -1;

            // Skip potential Windows drive letter (e.g., "C:")
            int searchStart = 0;
            if (pathString.length() >= 2 && pathString.charAt(1) == ':') {
                searchStart = 2;
            }

            // Look for a colon after the drive letter position
            colonIndex = pathString.indexOf(':', searchStart);

            if (colonIndex == -1) {
                // No range specification found
                return new String[]{pathString, null};
            }

            // Split into path and range spec
            String actualPath = pathString.substring(0, colonIndex);
            String rangeSpec = pathString.substring(colonIndex + 1);

            return new String[]{actualPath, rangeSpec};
        }
    }

    @CommandLine.Option(
        names = {"-i", "--input"},
        description = "The input file path. Can include inline range: 'file:rangespec' (e.g., 'input.fvec:1000' or 'input.fvec:[0,1000)')",
        required = true,
        converter = InputFileConverter.class
    )
    private InputFile inputFile;

    /**
     * Gets the InputFile record constructed from the options.
     * Parsing happens automatically via picocli - no manual parse() call needed.
     */
    public InputFile getInputFile() {
        return inputFile;
    }

    /**
     * Gets the input file path.
     */
    public Path getInputPath() {
        return inputFile != null ? inputFile.path() : null;
    }

    /**
     * Gets the normalized input file path.
     */
    public Path getNormalizedInputPath() {
        return inputFile != null ? inputFile.normalizedPath() : null;
    }

    /**
     * Gets the inline range specification, if any.
     */
    public String getInlineRangeSpec() {
        return inputFile != null ? inputFile.inlineRangeSpec() : null;
    }

    /**
     * Checks if an inline range specification was provided.
     */
    public boolean hasInlineRange() {
        return inputFile != null && inputFile.hasInlineRange();
    }

    /**
     * Validates the input file exists.
     */
    public void validate() {
        if (inputFile == null) {
            throw new IllegalStateException("Input file is required");
        }
        inputFile.validate();
    }

    /**
     * Returns string representation of the input file.
     */
    @Override
    public String toString() {
        return inputFile != null ? inputFile.toString() : "null";
    }
}
