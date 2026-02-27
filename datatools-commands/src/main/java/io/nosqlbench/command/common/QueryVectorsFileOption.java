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
 * Mixin for query vectors file option.
 * Provides a required option for specifying the query vectors file path
 * with optional inline range specification support.
 */
public class QueryVectorsFileOption {

    /// Creates a new QueryVectorsFileOption instance.
    public QueryVectorsFileOption() {
    }

    @CommandLine.Option(
        names = {"-q", "--query"},
        description = "Query vectors spec with optional range suffix (e.g., file.fvec[0,1000) or dataset.profile.query[0,1000))",
        required = true,
        converter = QueryVectorsConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class
    )
    private QueryVectors queryVectors;

    /**
     * Gets the query vectors configuration
     * @return The parsed query vectors configuration
     */
    public QueryVectors getQueryVectors() {
        return queryVectors;
    }

    /**
     * Gets the path to the query vectors file
     * @return The normalized path to the query vectors file
     */
    public VectorDataSpec getSpec() {
        return queryVectors.spec();
    }

    /**
     * Gets the local file path if this is a local file spec.
     * @return the local path, or empty if not a local file
     */
    public Optional<Path> getLocalPath() {
        return queryVectors.spec().getLocalPath();
    }

    /**
     * Gets the query vectors path (local file only).
     * @return the query vectors file path
     */
    public Path getQueryPath() {
        if (!queryVectors.spec().isLocalFile()) {
            throw new IllegalArgumentException("Query vectors spec must be a local file: " + queryVectors.spec());
        }
        return queryVectors.spec().getLocalPath().orElseThrow();
    }

    /**
     * Gets the normalized query vectors path (local file only).
     * @return the normalized query vectors file path
     */
    public Path getNormalizedQueryPath() {
        return getQueryPath().normalize();
    }

    /**
     * Gets the inline range spec, if provided.
     * @return the range spec string, or null
     */
    public String getInlineRange() {
        return queryVectors.rangeSpec();
    }

    /**
     * Gets the inline range if present.
     * @return the range, or empty if not specified
     */
    public Optional<RangeOption.Range> getRange() {
        return Optional.ofNullable(queryVectors.range());
    }

    /**
     * Checks if a range was specified.
     * @return true if a range was specified
     */
    public boolean hasRange() {
        return queryVectors.range() != null;
    }

    /**
     * Validates that the query vectors file exists for local file specs
     */
    public void validateQueryVectors() {
        if (queryVectors.spec().isLocalFile()) {
            Path path = queryVectors.spec().getLocalPath().orElseThrow();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Query vectors file does not exist: " + path);
            }
        }
    }

    @Override
    public String toString() {
        return "QueryVectorsFileOption{queryVectors=" + queryVectors + "}";
    }

    /**
     * Record representing query vectors file configuration
     * @param spec the vector data spec
     * @param range the parsed range, or null
     * @param rangeSpec the raw range spec string, or null
     */
    public record QueryVectors(VectorDataSpec spec, RangeOption.Range range, String rangeSpec) {
        /// Compact constructor that validates the spec is non-null.
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
     * Custom converter for parsing query vectors file specification.
     */
    public static class QueryVectorsConverter implements CommandLine.ITypeConverter<QueryVectors> {
        /// Creates a new QueryVectorsConverter instance.
        public QueryVectorsConverter() {
        }

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
