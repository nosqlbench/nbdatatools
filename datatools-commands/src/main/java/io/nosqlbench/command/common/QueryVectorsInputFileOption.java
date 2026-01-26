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
 * Mixin for query vectors input file option (optional variant).
 * Provides an optional option for specifying the query vectors file path with inline range support.
 */
public class QueryVectorsInputFileOption {

    @CommandLine.Option(
        names = {"--query"},
        description = "Query vectors spec with optional range suffix (e.g., file.fvec[0,1000) or dataset.profile.query[0,1000))",
        converter = QueryVectorsConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class
    )
    private QueryVectors queryVectors;

    /**
     * Gets the query vectors file path
     * @return The query vectors file path, or null if not specified
     */
    public VectorDataSpec getSpec() {
        return queryVectors != null ? queryVectors.spec() : null;
    }

    /**
     * Gets the local file path if this is a local file spec
     */
    public Optional<Path> getLocalPath() {
        return queryVectors != null ? queryVectors.spec().getLocalPath() : Optional.empty();
    }

    /**
     * Gets the normalized query vectors path (local file only).
     */
    public Path getNormalizedQueryPath() {
        if (queryVectors == null) {
            return null;
        }
        if (!queryVectors.spec().isLocalFile()) {
            throw new IllegalArgumentException("Query vectors spec must be a local file: " + queryVectors.spec());
        }
        return queryVectors.spec().getLocalPath().orElseThrow().normalize();
    }

    /**
     * Gets the inline range specification, if any.
     */
    public String getInlineRange() {
        return queryVectors != null ? queryVectors.rangeSpec() : null;
    }

    /**
     * Gets the inline range if present
     */
    public Optional<RangeOption.Range> getRange() {
        return queryVectors != null ? Optional.ofNullable(queryVectors.range()) : Optional.empty();
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return queryVectors != null && queryVectors.range() != null;
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
        if (queryVectors != null && queryVectors.spec().isLocalFile()) {
            Path path = queryVectors.spec().getLocalPath().orElseThrow();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Query vectors input file does not exist: " + path);
            }
        }
    }

    @Override
    public String toString() {
        return "QueryVectorsInputFileOption{queryVectors=" + queryVectors + "}";
    }

    /**
     * Record representing query vectors file configuration
     */
    public record QueryVectors(VectorDataSpec spec, RangeOption.Range range, String rangeSpec) {
        public QueryVectors {
            if (spec == null) {
                throw new IllegalArgumentException("Query vectors spec cannot be null");
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
     * Custom converter for parsing query vectors file specification
     */
    public static class QueryVectorsConverter implements CommandLine.ITypeConverter<QueryVectors> {
        @Override
        public QueryVectors convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Query vectors file path cannot be empty");
            }

            VectorDataSpecParser.Parsed parsed = VectorDataSpecParser.parse(value);
            VectorDataSpec spec = parsed.spec();
            if (spec.isFacet()) {
                TestDataKind kind = spec.getFacetKind().orElseThrow();
                if (kind != TestDataKind.query_vectors) {
                    throw new CommandLine.TypeConversionException(
                        "Query vectors spec must use facet 'query_vectors', got: " + kind.name());
                }
            }
            if (spec.isRemote()) {
                throw new CommandLine.TypeConversionException(
                    "Remote vector specs are not supported for --query: " + spec);
            }
            return new QueryVectors(spec, parsed.range(), parsed.rangeSpec());
        }
    }
}
