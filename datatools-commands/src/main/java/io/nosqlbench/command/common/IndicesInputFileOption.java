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
import java.nio.file.Files;
import java.util.Optional;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

/**
 * Mixin for indices input file option.
 * Provides an optional option for specifying the neighbor indices input file path with inline range support.
 */
public class IndicesInputFileOption {

    /// Creates a new IndicesInputFileOption instance.
    public IndicesInputFileOption() {
    }

    @CommandLine.Option(
        names = {"--indices"},
        description = "Neighbor indices spec with optional range suffix (e.g., file.ivec[0,1000) or dataset.profile.indices[0,1000))",
        converter = IndicesConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class
    )
    private Indices indices;

    /**
     * Gets the indices file path
     * @return The indices file path, or null if not specified
     */
    public VectorDataSpec getSpec() {
        return indices != null ? indices.spec() : null;
    }

    /**
     * Gets the local file path if this is a local file spec.
     * @return the local path, or empty if not specified
     */
    public Optional<Path> getLocalPath() {
        return indices != null ? indices.spec().getLocalPath() : Optional.empty();
    }

    /**
     * Gets the normalized indices path (local file only).
     * @return the normalized absolute path to the indices file
     */
    public Path getNormalizedIndicesPath() {
        if (indices == null) {
            return null;
        }
        if (!indices.spec().isLocalFile()) {
            throw new IllegalArgumentException("Indices spec must be a local file: " + indices.spec());
        }
        return indices.spec().getLocalPath().orElseThrow().normalize();
    }

    /**
     * Gets the inline range specification, if any.
     * @return the range spec string, or null if not specified
     */
    public String getInlineRange() {
        return indices != null ? indices.rangeSpec() : null;
    }

    /**
     * Gets the inline range specification if present
     * @return The range specification or null if not specified
     */
    public Optional<RangeOption.Range> getRange() {
        return indices != null ? Optional.ofNullable(indices.range()) : Optional.empty();
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return indices != null && indices.range() != null;
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
        if (indices != null && indices.spec().isLocalFile()) {
            Path path = indices.spec().getLocalPath().orElseThrow();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Indices input file does not exist: " + path);
            }
        }
    }

    @Override
    public String toString() {
        return "IndicesInputFileOption{indices=" + indices + "}";
    }

    /**
     * Record representing indices file configuration
     * @param spec the vector data spec
     * @param range the parsed range, or null
     * @param rangeSpec the raw range spec string, or null
     */
    public record Indices(VectorDataSpec spec, RangeOption.Range range, String rangeSpec) {
        /// Compact constructor that validates the spec is non-null.
        public Indices {
            if (spec == null) {
                throw new IllegalArgumentException("Indices spec cannot be null");
            }
        }

        @Override
        public String toString() {
            return rangeSpec != null && !rangeSpec.isEmpty()
                ? spec + rangeSpec
                : spec.toString();
        }
    }

    /**
     * Custom converter for parsing indices file specification.
     */
    public static class IndicesConverter implements CommandLine.ITypeConverter<Indices> {
        /// Creates a new IndicesConverter instance.
        public IndicesConverter() {
        }

        @Override
        public Indices convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Indices file path cannot be empty");
            }

            VectorDataSpecParser.Parsed parsed = VectorDataSpecParser.parse(value);
            VectorDataSpec spec = parsed.spec();
            if (spec.isFacet()) {
                TestDataKind kind = spec.getFacetKind().orElseThrow();
                if (kind != TestDataKind.neighbor_indices) {
                    throw new CommandLine.TypeConversionException(
                        "Indices spec must use facet 'neighbor_indices', got: " + kind.name());
                }
            }
            if (spec.isRemote()) {
                throw new CommandLine.TypeConversionException(
                    "Remote vector specs are not supported for --indices: " + spec);
            }
            return new Indices(spec, parsed.range(), parsed.rangeSpec());
        }
    }
}
