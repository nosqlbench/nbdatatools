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

package io.nosqlbench.datatools.virtdata.sampling;

/// Wrapper that pre-computes a lookup table for O(1) sampling.
///
/// LERP (Linear intERPolation) sampling pre-computes inverse CDF values at fixed
/// points and uses linear interpolation for intermediate values.
///
/// # Performance Characteristics
///
/// - **Construction**: O(tableSize) calls to delegate.sample()
/// - **Sampling**: O(1) - array index + linear interpolation
/// - **Memory**: tableSize × 8 bytes (double[] array)
///
/// # Accuracy Trade-off
///
/// With 1024 points, maximum interpolation error is bounded by the
/// curvature of the inverse CDF. For most distributions, error is < 0.1%.
/// Distributions with sharp features (heavy tails, discontinuities) may
/// have higher error in those regions.
///
/// # Usage
///
/// ```java
/// ComponentSampler direct = new NormalSampler(model);
/// ComponentSampler lerp = new LerpSampler(direct);
///
/// // O(1) sampling
/// double value = lerp.sample(0.5);
/// ```
public final class LerpSampler implements ComponentSampler {

    /// Default table size (1024 points = ~8KB memory).
    public static final int DEFAULT_TABLE_SIZE = 1024;

    private final double[] lookupTable;
    private final int tableSize;
    private final double indexScale;
    private final double minValue;
    private final double maxValue;

    /// Creates a LERP sampler wrapping the given delegate with default table size.
    ///
    /// @param delegate the sampler to wrap
    public LerpSampler(ComponentSampler delegate) {
        this(delegate, DEFAULT_TABLE_SIZE);
    }

    /// Creates a LERP sampler with custom table size.
    ///
    /// @param delegate the sampler to wrap
    /// @param tableSize number of pre-computed points (must be >= 16)
    /// @throws IllegalArgumentException if tableSize < 16
    public LerpSampler(ComponentSampler delegate, int tableSize) {
        if (tableSize < 16) {
            throw new IllegalArgumentException("Table size must be >= 16, got: " + tableSize);
        }
        this.tableSize = tableSize;
        this.indexScale = tableSize - 1;
        this.lookupTable = new double[tableSize];

        // Pre-compute table with values at midpoints of each interval
        // This minimizes maximum interpolation error
        for (int i = 0; i < tableSize; i++) {
            // Map index to u ∈ (0, 1), avoiding exact 0 and 1
            double u = (i + 0.5) / tableSize;
            u = Math.max(1e-10, Math.min(1 - 1e-10, u));
            lookupTable[i] = delegate.sample(u);
        }

        // Cache boundary values for edge cases
        this.minValue = lookupTable[0];
        this.maxValue = lookupTable[tableSize - 1];
    }

    @Override
    public double sample(double u) {
        // Handle boundary cases
        if (u <= 0.0) return minValue;
        if (u >= 1.0) return maxValue;

        // Compute continuous index into table
        double indexD = u * indexScale;
        int lo = (int) indexD;
        int hi = Math.min(lo + 1, tableSize - 1);

        // Linear interpolation factor
        double t = indexD - lo;

        // LERP: (1-t) * lo_value + t * hi_value
        return lookupTable[lo] + t * (lookupTable[hi] - lookupTable[lo]);
    }

    /// Returns the table size used by this sampler.
    ///
    /// @return the number of pre-computed points
    public int getTableSize() {
        return tableSize;
    }

    /// Returns a copy of the lookup table (for testing/debugging).
    ///
    /// @return a copy of the pre-computed values
    double[] getLookupTable() {
        return lookupTable.clone();
    }
}
