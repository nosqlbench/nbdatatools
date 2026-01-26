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
 * Mixin for base vectors input file option (optional variant).
 * Provides an optional option for specifying the base vectors file path with inline range support.
 */
public class BaseVectorsInputFileOption {

    @CommandLine.Option(
        names = {"--base"},
        description = "Base vectors spec with optional range suffix (e.g., file.fvec[0,1000) or dataset.profile.base[0,1000))",
        converter = BaseVectorsConverter.class,
        completionCandidates = VectorDataCompletionCandidates.class
    )
    private BaseVectors baseVectors;

    /**
     * Gets the base vectors file path
     * @return The base vectors file path, or null if not specified
     */
    public VectorDataSpec getSpec() {
        return baseVectors != null ? baseVectors.spec() : null;
    }

    /**
     * Gets the local file path if this is a local file spec
     */
    public Optional<Path> getLocalPath() {
        return baseVectors != null ? baseVectors.spec().getLocalPath() : Optional.empty();
    }

    /**
     * Gets the normalized base vectors path (local file only).
     */
    public Path getNormalizedBasePath() {
        if (baseVectors == null) {
            return null;
        }
        if (!baseVectors.spec().isLocalFile()) {
            throw new IllegalArgumentException("Base vectors spec must be a local file: " + baseVectors.spec());
        }
        return baseVectors.spec().getLocalPath().orElseThrow().normalize();
    }

    /**
     * Gets the inline range specification, if any.
     */
    public String getInlineRange() {
        return baseVectors != null ? baseVectors.rangeSpec() : null;
    }

    /**
     * Gets the inline range if present
     */
    public Optional<RangeOption.Range> getRange() {
        return baseVectors != null ? Optional.ofNullable(baseVectors.range()) : Optional.empty();
    }

    /**
     * Checks if an inline range was specified
     * @return true if a range was specified, false otherwise
     */
    public boolean hasInlineRange() {
        return baseVectors != null && baseVectors.range() != null;
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
        if (baseVectors != null && baseVectors.spec().isLocalFile()) {
            Path path = baseVectors.spec().getLocalPath().orElseThrow();
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Base vectors input file does not exist: " + path);
            }
        }
    }

    @Override
    public String toString() {
        return "BaseVectorsInputFileOption{baseVectors=" + baseVectors + "}";
    }

    /**
     * Record representing base vectors file configuration
     */
    public record BaseVectors(VectorDataSpec spec, RangeOption.Range range, String rangeSpec) {
        public BaseVectors {
            if (spec == null) {
                throw new IllegalArgumentException("Base vectors spec cannot be null");
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
     * Custom converter for parsing base vectors file specification
     */
    public static class BaseVectorsConverter implements CommandLine.ITypeConverter<BaseVectors> {
        @Override
        public BaseVectors convert(String value) throws Exception {
            if (value == null || value.isEmpty()) {
                throw new CommandLine.TypeConversionException("Base vectors file path cannot be empty");
            }

            VectorDataSpecParser.Parsed parsed = VectorDataSpecParser.parse(value);
            VectorDataSpec spec = parsed.spec();
            if (spec.isFacet()) {
                TestDataKind kind = spec.getFacetKind().orElseThrow();
                if (kind != TestDataKind.base_vectors) {
                    throw new CommandLine.TypeConversionException(
                        "Base vectors spec must use facet 'base_vectors', got: " + kind.name());
                }
            }
            if (spec.isRemote()) {
                throw new CommandLine.TypeConversionException(
                    "Remote vector specs are not supported for --base: " + spec);
            }
            return new BaseVectors(spec, parsed.range(), parsed.rangeSpec());
        }
    }
}
