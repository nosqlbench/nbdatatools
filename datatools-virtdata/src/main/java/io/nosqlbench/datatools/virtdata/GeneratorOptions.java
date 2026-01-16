/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.nosqlbench.datatools.virtdata;

import io.nosqlbench.datatools.virtdata.sampling.LerpSampler;

/// Configuration options for vector generators.
///
/// # Overview
///
/// GeneratorOptions controls optional features for vector generation:
/// - **Implementation mode**: AUTO, SCALAR, or PANAMA
/// - **LERP optimization**: Pre-computed lookup tables for O(1) sampling
/// - **L2 normalization**: Output unit vectors
///
/// # Usage
///
/// ```java
/// // Default options (no LERP, no normalization, AUTO mode)
/// GeneratorOptions defaults = GeneratorOptions.defaults();
///
/// // Custom options using builder
/// GeneratorOptions options = GeneratorOptions.builder()
///     .mode(VectorGenFactory.Mode.AUTO)
///     .useLerp(true)
///     .lerpTableSize(2048)
///     .normalizeL2(true)
///     .build();
///
/// // Create generator with options
/// VectorGenerator<VectorSpaceModel> gen = VectorGenFactory.create(model, options);
/// ```
///
/// @see VectorGenFactory
public final class GeneratorOptions {

    private final VectorGenFactory.Mode mode;
    private final boolean useLerp;
    private final int lerpTableSize;
    private final boolean normalizeL2;

    private GeneratorOptions(Builder builder) {
        this.mode = builder.mode;
        this.useLerp = builder.useLerp;
        this.lerpTableSize = builder.lerpTableSize;
        this.normalizeL2 = builder.normalizeL2;
    }

    /// Returns the implementation mode (AUTO, SCALAR, PANAMA).
    ///
    /// @return the implementation mode
    public VectorGenFactory.Mode mode() {
        return mode;
    }

    /// Returns whether LERP optimization is enabled.
    ///
    /// When enabled, each dimension's sampler is wrapped with a pre-computed
    /// lookup table for O(1) sampling performance.
    ///
    /// @return true if LERP optimization is enabled
    public boolean useLerp() {
        return useLerp;
    }

    /// Returns the LERP table size (default 1024).
    ///
    /// Only used if [#useLerp()] returns true.
    /// Larger tables provide higher accuracy but use more memory.
    ///
    /// @return the LERP table size
    public int lerpTableSize() {
        return lerpTableSize;
    }

    /// Returns whether L2 normalization is enabled.
    ///
    /// When enabled, output vectors are normalized to unit length
    /// (L2 norm = 1.0) after sampling.
    ///
    /// @return true if L2 normalization is enabled
    public boolean normalizeL2() {
        return normalizeL2;
    }

    /// Returns the default options.
    ///
    /// Defaults:
    /// - mode: AUTO
    /// - useLerp: false
    /// - lerpTableSize: 1024
    /// - normalizeL2: false
    ///
    /// @return the default generator options
    public static GeneratorOptions defaults() {
        return new Builder().build();
    }

    /// Returns a new builder.
    ///
    /// @return a new builder instance
    public static Builder builder() {
        return new Builder();
    }

    /// Returns a builder initialized with this options' values.
    ///
    /// @return a builder pre-populated with current values
    public Builder toBuilder() {
        return new Builder()
            .mode(this.mode)
            .useLerp(this.useLerp)
            .lerpTableSize(this.lerpTableSize)
            .normalizeL2(this.normalizeL2);
    }

    @Override
    public String toString() {
        return "GeneratorOptions{" +
            "mode=" + mode +
            ", useLerp=" + useLerp +
            ", lerpTableSize=" + lerpTableSize +
            ", normalizeL2=" + normalizeL2 +
            '}';
    }

    /// Builder for GeneratorOptions.
    public static final class Builder {
        private VectorGenFactory.Mode mode = VectorGenFactory.Mode.AUTO;
        private boolean useLerp = false;
        private int lerpTableSize = LerpSampler.DEFAULT_TABLE_SIZE;
        private boolean normalizeL2 = false;

        Builder() {
        }

        /// Sets the implementation mode.
        ///
        /// @param mode AUTO, SCALAR, or PANAMA
        /// @return this builder
        public Builder mode(VectorGenFactory.Mode mode) {
            this.mode = mode;
            return this;
        }

        /// Enables or disables LERP optimization.
        ///
        /// @param useLerp true to enable LERP
        /// @return this builder
        public Builder useLerp(boolean useLerp) {
            this.useLerp = useLerp;
            return this;
        }

        /// Sets the LERP table size.
        ///
        /// @param size number of pre-computed points (must be >= 16)
        /// @return this builder
        /// @throws IllegalArgumentException if size < 16
        public Builder lerpTableSize(int size) {
            if (size < 16) {
                throw new IllegalArgumentException("LERP table size must be >= 16, got: " + size);
            }
            this.lerpTableSize = size;
            return this;
        }

        /// Enables or disables L2 normalization.
        ///
        /// @param normalize true to normalize output vectors
        /// @return this builder
        public Builder normalizeL2(boolean normalize) {
            this.normalizeL2 = normalize;
            return this;
        }

        /// Builds the GeneratorOptions.
        ///
        /// @return the configured options
        public GeneratorOptions build() {
            return new GeneratorOptions(this);
        }
    }
}
