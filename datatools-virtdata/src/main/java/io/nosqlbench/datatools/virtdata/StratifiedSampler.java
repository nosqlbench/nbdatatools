package io.nosqlbench.datatools.virtdata;

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

/**
 * Stratified sampling utilities for avoiding congruency artifacts in multi-dimensional sampling.
 *
 * <h2>Purpose</h2>
 *
 * <p>When generating N vectors in M-dimensional space, naive approaches create visible
 * lattice patterns. For example, using {@code (ordinal/N, ordinal/N, ...)} for all
 * dimensions produces points along a diagonal line. This class breaks such correlations
 * through permuted-stratified sampling.
 *
 * <h2>The Congruency Problem</h2>
 *
 * <pre>{@code
 * NAIVE APPROACH (creates lattice):        STRATIFIED APPROACH (breaks correlation):
 *
 *     dim1                                     dim1
 *      │    ●                                   │  ●     ●
 *      │   ●                                    │     ●
 *      │  ●                                     │        ●    ●
 *      │ ●                                      │  ●
 *      │●                                       │       ●   ●
 *      └────────── dim0                         └────────────── dim0
 *
 *   Points fall on diagonal                  Points scattered uniformly
 *   (correlated dimensions)                  (independent dimensions)
 * }</pre>
 *
 * <h2>Processing Steps</h2>
 *
 * <p>The {@link #unitIntervalValue} method transforms an ordinal into a unit-interval
 * value through three sub-stages:
 *
 * <pre>{@code
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │              STRATIFIED SAMPLING PIPELINE                          │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 *  INPUT: ordinal, dimension, totalVectors
 *
 *  ┌─────────────────────────────────────────────────────────────────┐
 *  │ STEP 1: PERMUTE ORDINAL                                         │
 *  │   permuteOrdinal(ordinal, dimension)                            │
 *  │                                                                 │
 *  │   ordinal ──► [prime multiply] ──► [xorshift mix] ──► [rotate]  │
 *  │                    │                     │                │     │
 *  │            dimension-specific      bit avalanche    dimension   │
 *  │                prime                  mixing       rotation     │
 *  └───────────────────────────────────────────────────────┬─────────┘
 *                                                          │
 *                                                          ▼
 *  ┌─────────────────────────────────────────────────────────────────┐
 *  │ STEP 2: COMPUTE STRATUM + JITTER                                │
 *  │                                                                 │
 *  │   stratum = permutedOrdinal % totalVectors                      │
 *  │   jitter  = weylSequence(permutedOrdinal, dimension)  ∈ [0,1)   │
 *  │                                                                 │
 *  │   ├─── stratum 0 ───┼─── stratum 1 ───┼─── stratum 2 ───┤       │
 *  │   │        ●        │        ●        │        ●        │       │
 *  │   │     (jitter)    │     (jitter)    │     (jitter)    │       │
 *  │   0               1/N              2/N              3/N         │
 *  └───────────────────────────────────────────────────────┬─────────┘
 *                                                          │
 *                                                          ▼
 *  ┌─────────────────────────────────────────────────────────────────┐
 *  │ STEP 3: MAP TO UNIT INTERVAL                                    │
 *  │                                                                 │
 *  │   rawValue = (stratum + jitter) / totalVectors                  │
 *  │   result   = clamp(rawValue, ε, 1-ε)                            │
 *  │                                                                 │
 *  │   0 ◄─────────────── result ───────────────► 1                  │
 *  │   │ε                                      1-ε│                  │
 *  └───────────────────────────────────────────────────────┬─────────┘
 *                                                          │
 *                                                          ▼
 *  OUTPUT: u ∈ (0, 1) ready for inverse CDF transform
 * }</pre>
 *
 * <h2>Key Techniques</h2>
 *
 * <ul>
 *   <li><b>Dimension-specific primes</b> - Each dimension uses a different prime multiplier
 *       to ensure ordinals map to different permuted values across dimensions</li>
 *   <li><b>Xorshift mixing</b> - Provides bit avalanche effect so small ordinal changes
 *       produce large permuted value changes</li>
 *   <li><b>Weyl sequence jitter</b> - Uses irrational-number-based sequences for
 *       low-discrepancy sampling within each stratum</li>
 *   <li><b>Clamping</b> - Avoids exact 0 and 1 which would cause infinity in inverse CDF</li>
 * </ul>
 *
 * @see VectorGenerator
 */
public final class StratifiedSampler {

    /** Golden ratio minus one, used for Weyl sequence generation. */
    private static final double PHI_MINUS_ONE = 0.6180339887498949;

    /** Large primes for dimension-specific ordinal permutation. */
    private static final long[] DIMENSION_PRIMES = {
        2654435761L, 805459861L, 3266489917L, 668265263L,
        374761393L, 2246822519L, 3266489917L, 668265263L,
        2246822519L, 3266489917L, 668265263L, 2654435761L,
        805459861L, 3266489917L, 374761393L, 2246822519L
    };

    private StratifiedSampler() {
        // Utility class
    }

    /**
     * Converts an ordinal into a unit-interval value for a specific dimension.
     *
     * <p>This is the main entry point for Stage 2 of the vector generation pipeline.
     * It produces a value in (0, 1) that is:
     * <ul>
     *   <li>Deterministic (same inputs always produce same output)</li>
     *   <li>Uncorrelated across dimensions (different dimensions get different permutations)</li>
     *   <li>Well-distributed within each stratum (via Weyl sequence jitter)</li>
     * </ul>
     *
     * <pre>{@code
     * (ordinal, dimension, N) ──► u ∈ (0, 1)
     *
     * Example for N=4:
     *   ordinal=0, dim=0 → u=0.15   (stratum 0 + jitter)
     *   ordinal=0, dim=1 → u=0.62   (different stratum due to permutation)
     *   ordinal=1, dim=0 → u=0.38   (stratum 1 + jitter)
     * }</pre>
     *
     * @param ordinal the vector ordinal (0 to N-1)
     * @param dimension the dimension index (0 to M-1)
     * @param totalVectors the total number of vectors N
     * @return a value in (0, 1) suitable for inverse transform sampling
     */
    public static double unitIntervalValue(long ordinal, int dimension, long totalVectors) {
        // Step 1: Permute ordinal based on dimension
        long permutedOrdinal = permuteOrdinal(ordinal, dimension);

        // Step 2: Compute stratum and jitter
        long stratum = permutedOrdinal % totalVectors;
        if (stratum < 0) {
            stratum += totalVectors;
        }
        double jitter = weylSequence(permutedOrdinal, dimension);

        // Step 3: Map to unit interval
        double rawValue = (stratum + jitter) / totalVectors;
        return clampToValidRange(rawValue);
    }

    /**
     * Computes Weyl sequence value for quasi-random jitter within a stratum.
     *
     * <p>The Weyl sequence {@code {n * α} mod 1} where α is irrational produces
     * a low-discrepancy sequence that fills the unit interval uniformly without
     * obvious patterns.
     *
     * <pre>{@code
     * Weyl sequence with α = φ-1 (golden ratio):
     *
     *   n=0: 0.000     ●
     *   n=1: 0.618       ●
     *   n=2: 0.236    ●
     *   n=3: 0.854          ●
     *   n=4: 0.472      ●
     *        ├────────────────┤
     *        0                1
     * }</pre>
     *
     * @param ordinal the ordinal value
     * @param dimension the dimension index (modifies the base)
     * @return a value in [0, 1) for jitter within a stratum
     */
    private static double weylSequence(long ordinal, int dimension) {
        double base = PHI_MINUS_ONE * (dimension + 1);
        double value = (ordinal * base) % 1.0;
        return value < 0 ? value + 1.0 : value;
    }

    /**
     * Permutes an ordinal based on dimension to break inter-dimension correlation.
     *
     * <p>This ensures that ordinal 42 maps to completely different positions
     * in dimension 0 vs dimension 1, preventing lattice artifacts.
     *
     * <pre>{@code
     * Permutation process:
     *
     *   ordinal ──► × prime[dim] ──► xorshift³ ──► rotate(dim×7)
     *                    │               │              │
     *              dimension-        avalanche     dimension-
     *              specific          mixing        specific
     *              scramble                        rotation
     * }</pre>
     *
     * @param ordinal the input ordinal
     * @param dimension the dimension index
     * @return a permuted ordinal value with good bit distribution
     */
    public static long permuteOrdinal(long ordinal, int dimension) {
        // Dimension-specific prime multiplication
        long prime = DIMENSION_PRIMES[dimension % DIMENSION_PRIMES.length];
        long mixed = ordinal * prime;

        // Xorshift mixing (Murmur3 finalizer)
        mixed ^= (mixed >>> 33);
        mixed *= 0xff51afd7ed558ccdL;
        mixed ^= (mixed >>> 33);
        mixed *= 0xc4ceb9fe1a85ec53L;
        mixed ^= (mixed >>> 33);

        // Dimension-specific rotation
        int rotation = (dimension * 7) % 64;
        return Long.rotateLeft(mixed, rotation);
    }

    /**
     * Clamps a value to the valid range for inverse CDF sampling.
     *
     * <p>The inverse Gaussian CDF approaches ±∞ as p approaches 0 or 1.
     * This method ensures values stay in (ε, 1-ε) to avoid numerical issues.
     *
     * @param value the raw value to clamp
     * @return a value in (ε, 1-ε) where ε = 10⁻¹⁰
     */
    private static double clampToValidRange(double value) {
        final double EPSILON = 1e-10;
        return Math.max(EPSILON, Math.min(1.0 - EPSILON, value));
    }
}
