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
 * Mixin for distances input file option.
 * Provides an optional option for specifying the neighbor distances input file path.
 */
public class DistancesInputFileOption {

    /// Creates a new DistancesInputFileOption instance.
    public DistancesInputFileOption() {
    }

    @CommandLine.Option(
        names = {"--distances"},
        description = "Neighbor distances spec (optional)",
        converter = DistancesConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class
    )
    private Distances distances;

    /**
     * Gets the distances file path
     * @return The distances file path, or null if not specified
     */
    public VectorDataSpec getSpec() {
        return distances != null ? distances.spec() : null;
    }

    /**
     * Gets the normalized distances file path
     * @return The normalized path to the distances file, or null if not specified
     */
    public Optional<Path> getLocalPath() {
        return distances != null ? distances.spec().getLocalPath() : Optional.empty();
    }

    /**
     * Checks if distances path was specified
     * @return true if specified, false otherwise
     */
    public boolean isSpecified() {
        return distances != null;
    }

    /**
     * Validates that the distances file exists
     * @throws IllegalArgumentException if the file doesn't exist
     */
    public void validateDistancesInput() {
        if (distances != null && distances.spec().isLocalFile()) {
            Path path = distances.spec().getLocalPath().orElseThrow();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Distances input file does not exist: " + path);
            }
        }
    }

    @Override
    public String toString() {
        return "DistancesInputFileOption{distances=" + distances + "}";
    }

    /**
     * Record representing distances file configuration
     * @param spec the vector data spec
     * @param range the parsed range, or null
     * @param rangeSpec the raw range spec string, or null
     */
    public record Distances(VectorDataSpec spec, RangeOption.Range range, String rangeSpec) {
        /// Compact constructor with validation.
        public Distances {
            if (spec == null) {
                throw new IllegalArgumentException("Distances spec cannot be null");
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
     * Custom converter for parsing distances file specification
     */
    public static class DistancesConverter implements CommandLine.ITypeConverter<Distances> {
        /// Creates a new DistancesConverter instance.
        public DistancesConverter() {
        }

        @Override
        public Distances convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Distances file path cannot be empty");
            }

            VectorDataSpecParser.Parsed parsed = VectorDataSpecParser.parse(value);
            VectorDataSpec spec = parsed.spec();
            if (spec.isFacet()) {
                TestDataKind kind = spec.getFacetKind().orElseThrow();
                if (kind != TestDataKind.neighbor_distances) {
                    throw new CommandLine.TypeConversionException(
                        "Distances spec must use facet 'neighbor_distances', got: " + kind.name());
                }
            }
            if (spec.isRemote()) {
                throw new CommandLine.TypeConversionException(
                    "Remote vector specs are not supported for --distances: " + spec);
            }
            return new Distances(spec, parsed.range(), parsed.rangeSpec());
        }
    }
}
