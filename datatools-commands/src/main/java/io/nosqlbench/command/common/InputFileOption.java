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
import java.util.Optional;

/**
 * Shared input file option with optional inline range specification support.
 * All supporting types are inner classes for self-contained encapsulation.
 */
public class InputFileOption {

    /**
     * Immutable input spec with optional inline range.
     * Implements CharSequence to allow direct use as a string in most contexts.
     * Supports specs with optional range suffixes in the format: {@code spec[range]}
     * <p>
     * Examples:
     * <ul>
     *   <li>{@code input.fvec} - plain file path</li>
     *   <li>{@code input.fvec[0,1000)} - elements 0 to 999</li>
     *   <li>{@code dataset.profile.base[0,1000)} - dataset facet with range</li>
     * </ul>
     *
     * @param spec            the vector data spec (never null)
     * @param inlineRangeSpec optional range specification extracted from path, or null
     */
    public record InputFile(VectorDataSpec spec, RangeOption.Range range, String inlineRangeSpec)
        implements CharSequence {

        /**
         * Compact constructor with validation.
         */
        public InputFile {
            if (spec == null) {
                throw new IllegalArgumentException("Input spec cannot be null");
            }
        }

        /**
         * Creates an InputFile without inline range specification.
         */
        public InputFile(VectorDataSpec spec) {
            this(spec, null, null);
        }

        /**
         * Gets the normalized absolute path.
         */
        public Path normalizedPath() {
            return spec.getLocalPath().orElseThrow().normalize().toAbsolutePath();
        }

        /**
         * Checks if an inline range specification was provided.
         */
        public boolean hasInlineRange() {
            return range != null;
        }

        /**
         * Checks if the input file exists.
         */
        public boolean exists() {
            return spec.isLocalFile() && Files.exists(spec.getLocalPath().orElseThrow());
        }

        /**
         * Validates that the input file exists.
         */
        public void validate() {
            if (!exists()) {
                throw new IllegalStateException("Input file does not exist: " + spec);
            }
        }

        // CharSequence implementation - delegates to path string

        @Override
        public int length() {
            return spec.toString().length();
        }

        @Override
        public char charAt(int index) {
            return spec.toString().charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return spec.toString().subSequence(start, end);
        }

        /**
         * Returns the path as a string with inline range if present.
         */
        @Override
        public String toString() {
            return inlineRangeSpec != null && !inlineRangeSpec.isEmpty()
                ? spec + inlineRangeSpec
                : spec.toString();
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

            VectorDataSpecParser.Parsed parsed = VectorDataSpecParser.parse(value);
            return new InputFile(parsed.spec(), parsed.range(), parsed.rangeSpec());
        }
    }

    @CommandLine.Option(
        names = {"-i", "--input"},
        description = "The input vector spec with optional range suffix (e.g., input.fvec[0,1000))",
        required = true,
        converter = InputFileConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class
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
        return inputFile != null ? inputFile.spec().getLocalPath().orElseThrow() : null;
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
     * Gets the vector spec.
     */
    public VectorDataSpec getSpec() {
        return inputFile != null ? inputFile.spec() : null;
    }

    /**
     * Gets the local file path if this is a local file spec.
     */
    public Optional<Path> getLocalPath() {
        return inputFile != null ? inputFile.spec().getLocalPath() : Optional.empty();
    }

    /**
     * Gets the parsed range, if any.
     */
    public Optional<RangeOption.Range> getRange() {
        return inputFile != null ? Optional.ofNullable(inputFile.range()) : Optional.empty();
    }

    /**
     * Validates the input file exists.
     */
    public void validate() {
        if (inputFile == null) {
            throw new IllegalStateException("Input file is required");
        }
        if (inputFile.spec().isLocalFile()) {
            inputFile.validate();
        }
    }

    /**
     * Returns string representation of the input file.
     */
    @Override
    public String toString() {
        return inputFile != null ? inputFile.toString() : "null";
    }
}
