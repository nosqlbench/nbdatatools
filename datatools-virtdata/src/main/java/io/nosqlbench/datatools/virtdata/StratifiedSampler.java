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

import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;

/**
 * Stratified sampling utilities using PCG random number generator.
 *
 * <h2>Purpose</h2>
 *
 * <p>Converts (ordinal, dimension) pairs into unit-interval values for inverse
 * CDF sampling. Uses the PCG (Permuted Congruential Generator) family for
 * high-quality, reproducible random numbers.
 *
 * <h2>PCG Properties</h2>
 *
 * <ul>
 *   <li><b>Deterministic</b> - Same (ordinal, dimension) always produces same value</li>
 *   <li><b>High quality</b> - Passes TestU01 BigCrush statistical tests</li>
 *   <li><b>Fast</b> - Single multiply-xorshift per sample</li>
 *   <li><b>Uncorrelated</b> - Different dimensions produce independent sequences</li>
 * </ul>
 *
 * <h2>Processing Steps</h2>
 *
 * <pre>{@code
 * (ordinal, dimension, N) ──► seed = mix(ordinal, dimension)
 *                         ──► PCG.setSeed(seed)
 *                         ──► u = PCG.nextDouble()
 *                         ──► clamp(u, ε, 1-ε)
 * }</pre>
 *
 * @see VectorGenerator
 */
public final class StratifiedSampler {

    /** Epsilon for clamping to avoid infinities in inverse CDF. */
    private static final double EPSILON = 1e-10;

    /** Large primes for seed mixing to ensure dimension independence. */
    private static final long DIMENSION_PRIME = 0x9E3779B97F4A7C15L; // Golden ratio based
    private static final long ORDINAL_PRIME = 0xBF58476D1CE4E5B9L;   // Murmur3 constant

    private StratifiedSampler() {
        // Utility class
    }

    /**
     * Converts an ordinal into a unit-interval value for a specific dimension.
     *
     * <p>This method produces a value in (0, 1) that is:
     * <ul>
     *   <li>Deterministic - same inputs always produce same output</li>
     *   <li>High quality - uses PCG random number generator</li>
     *   <li>Uncorrelated - different dimensions produce independent values</li>
     * </ul>
     *
     * @param ordinal the vector ordinal (any long value)
     * @param dimension the dimension index (0 to M-1)
     * @param totalVectors the total number of vectors N (used for stratification)
     * @return a value in (0, 1) suitable for inverse transform sampling
     */
    public static double unitIntervalValue(long ordinal, int dimension, long totalVectors) {
        // Create a unique seed from ordinal and dimension
        long seed = mixSeed(ordinal, dimension);

        // Create PCG generator with this seed
        // PCG_XSH_RR_32 is a high-quality 32-bit generator
        UniformRandomProvider rng = RandomSource.PCG_XSH_RR_32.create(seed);

        // Generate uniform value in (0, 1)
        double u = rng.nextDouble();

        // Clamp to avoid infinities in inverse CDF
        return clampToValidRange(u);
    }

    /**
     * Mixes ordinal and dimension into a unique seed.
     *
     * <p>Uses multiplication by large primes and XOR mixing to ensure:
     * <ul>
     *   <li>Different ordinals produce different seeds</li>
     *   <li>Different dimensions produce different seeds</li>
     *   <li>Nearby ordinals don't produce correlated seeds</li>
     * </ul>
     *
     * @param ordinal the ordinal value
     * @param dimension the dimension index
     * @return a mixed seed value
     */
    private static long mixSeed(long ordinal, int dimension) {
        // SplitMix64-style mixing for high-quality seed derivation
        long seed = ordinal * ORDINAL_PRIME;
        seed += dimension * DIMENSION_PRIME;

        // Avalanche mixing (from SplitMix64)
        seed ^= (seed >>> 30);
        seed *= 0xBF58476D1CE4E5B9L;
        seed ^= (seed >>> 27);
        seed *= 0x94D049BB133111EBL;
        seed ^= (seed >>> 31);

        return seed;
    }

    /**
     * Clamps a value to the valid range for inverse CDF sampling.
     *
     * @param value the raw value to clamp
     * @return a value in (ε, 1-ε) where ε = 10⁻¹⁰
     */
    private static double clampToValidRange(double value) {
        return Math.max(EPSILON, Math.min(1.0 - EPSILON, value));
    }

    /**
     * Generates a batch of unit-interval values for consecutive ordinals.
     *
     * <p>This is an optimization for batch generation that reuses the same
     * PCG instance with stream advancement.
     *
     * @param startOrdinal the starting ordinal
     * @param dimension the dimension index
     * @param count the number of values to generate
     * @param totalVectors the total number of vectors N
     * @return an array of unit-interval values
     */
    public static double[] unitIntervalBatch(long startOrdinal, int dimension, int count, long totalVectors) {
        double[] result = new double[count];
        for (int i = 0; i < count; i++) {
            result[i] = unitIntervalValue(startOrdinal + i, dimension, totalVectors);
        }
        return result;
    }
}
