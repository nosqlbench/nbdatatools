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

import java.util.function.LongFunction;

/**
 * Procedural vector generator using permuted-stratified sampling with Gaussian components.
 *
 * <p>This class generates M-dimensional vectors from ordinals in a deterministic,
 * replayable manner. Each unique ordinal (0 to N-1) maps to a unique vector
 * sampled from the configured vector space model.
 *
 * <h2>Processing Pipeline</h2>
 *
 * <p>The vector generation pipeline transforms an ordinal through several stages
 * to produce a vector with proper statistical properties:
 *
 * <pre>{@code
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │                     VECTOR GENERATION PIPELINE                              │
 * └─────────────────────────────────────────────────────────────────────────────┘
 *
 *   INPUT                                                              OUTPUT
 *  ┌───────┐                                                         ┌─────────┐
 *  │ordinal│                                                         │ float[] │
 *  │ (long)│                                                         │ vector  │
 *  └───┬───┘                                                         └────▲────┘
 *      │                                                                  │
 *      ▼                                                                  │
 *  ┌───────────────────┐                                                  │
 *  │ STAGE 1: NORMALIZE│  ordinal % N (handle negatives)                  │
 *  │   [0, N-1]        │                                                  │
 *  └─────────┬─────────┘                                                  │
 *            │                                                            │
 *            ▼                                                            │
 *  ┌─────────────────────────────────────────────────┐                    │
 *  │         FOR EACH DIMENSION d = 0 to M-1         │                    │
 *  │  ┌─────────────────────────────────────────┐    │                    │
 *  │  │ STAGE 2: STRATIFIED SAMPLING            │    │                    │
 *  │  │   StratifiedSampler.unitIntervalValue() │    │                    │
 *  │  │   ordinal,d,N → u ∈ (0,1)               │    │                    │
 *  │  └──────────────────┬──────────────────────┘    │                    │
 *  │                     │                           │                    │
 *  │                     ▼                           │                    │
 *  │  ┌─────────────────────────────────────────┐    │                    │
 *  │  │ STAGE 3: INVERSE CDF TRANSFORM          │    │    ┌──────────┐    │
 *  │  │   InverseGaussianCDF.quantile()         │────┼───►│ vector[d]│────┘
 *  │  │   u,mean[d],stdDev[d] → value           │    │    └──────────┘
 *  │  └─────────────────────────────────────────┘    │
 *  └─────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>Pipeline Stages</h2>
 *
 * <table border="1">
 * <caption>Stage descriptions</caption>
 * <tr><th>Stage</th><th>Component</th><th>Input</th><th>Output</th><th>Purpose</th></tr>
 * <tr><td>1</td><td>Normalize</td><td>any long</td><td>[0, N-1]</td><td>Wrap ordinal to valid range</td></tr>
 * <tr><td>2</td><td>{@link StratifiedSampler}</td><td>ordinal, dim</td><td>(0, 1)</td><td>Anti-congruent unit value</td></tr>
 * <tr><td>3</td><td>{@link InverseGaussianCDF}</td><td>u, mean, stdDev</td><td>float</td><td>Gaussian quantile transform</td></tr>
 * </table>
 *
 * <h2>Key Properties</h2>
 * <ul>
 *   <li><b>O(1) cost</b> - constant time per vector regardless of N</li>
 *   <li><b>Deterministic</b> - same ordinal always produces same vector</li>
 *   <li><b>Anti-congruent</b> - no lattice artifacts in multi-dimensional space</li>
 *   <li><b>Per-dimension control</b> - each dimension can have different Gaussian parameters</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create a space of 1M vectors with 128 dimensions, standard normal distribution
 * VectorSpaceModel model = new VectorSpaceModel(1_000_000, 128, 0.0, 1.0);
 * VectorGen gen = new VectorGen(model);
 *
 * // Generate single vectors
 * float[] v42 = gen.apply(42L);      // Always returns same vector for ordinal 42
 * float[] v42again = gen.apply(42L); // Identical to v42
 *
 * // Generate batches for efficiency
 * float[][] batch = gen.generateBatch(0, 1000);  // Vectors 0-999
 * float[] flat = gen.generateFlatBatch(0, 1000); // Same vectors, contiguous layout
 * }</pre>
 *
 * <p>For Java 25+ runtimes with Panama Vector API available, this class is superseded
 * by an optimized implementation in {@code META-INF/versions/25} that uses SIMD
 * operations for batch generation.
 *
 * @see VectorSpaceModel
 * @see StratifiedSampler
 * @see InverseGaussianCDF
 * @see VectorGenFactory
 */
public class VectorGen implements LongFunction<float[]> {

    private final VectorSpaceModel model;
    private final int dimensions;
    private final long uniqueVectors;

    // Component models for sampling (handles both truncated and unbounded)
    private final GaussianComponentModel[] componentModels;

    /**
     * Constructs a vector generator for the given vector space model.
     *
     * <pre>{@code
     * VectorSpaceModel ──► VectorGen
     *    (N, M, distributions)    (ready to generate)
     * }</pre>
     *
     * @param model the vector space model defining N (unique vectors), M (dimensions),
     *              and per-component Gaussian distributions (optionally truncated)
     */
    public VectorGen(VectorSpaceModel model) {
        this.model = model;
        this.dimensions = model.dimensions();
        this.uniqueVectors = model.uniqueVectors();

        // Store component models for sampling (handles truncation automatically)
        this.componentModels = model.componentModels();
    }

    /**
     * Generates a vector for the given ordinal using permuted-stratified Gaussian sampling.
     *
     * <p>This is the main entry point implementing {@code LongFunction<float[]>}.
     *
     * @param ordinal the vector ordinal (0 to N-1); values outside this range are wrapped
     * @return an M-dimensional float array representing the sampled vector
     */
    @Override
    public float[] apply(long ordinal) {
        float[] result = new float[dimensions];
        generateInto(ordinal, result, 0);
        return result;
    }

    /**
     * Generates a vector for the given ordinal into an existing array.
     *
     * <p>This method avoids allocation overhead when generating many vectors
     * into pre-allocated storage, such as when building batches or filling
     * database buffers.
     *
     * <pre>{@code
     * ordinal ──► [normalize] ──► [stratified sampling] ──► [inverse CDF] ──► target[offset..]
     * }</pre>
     *
     * @param ordinal the vector ordinal
     * @param target the target array to write into
     * @param offset the offset within the target array
     * @throws ArrayIndexOutOfBoundsException if target doesn't have enough space
     */
    public void generateInto(long ordinal, float[] target, int offset) {
        // Stage 1: Normalize ordinal to valid range
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            // Stage 2: Get stratified unit-interval value for this dimension
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);

            // Stage 3: Apply inverse Gaussian CDF transform (handles truncation if configured)
            double value = componentModels[d].sample(u);

            target[offset + d] = (float) value;
        }
    }

    /**
     * Generates a vector for the given ordinal as double precision.
     *
     * @param ordinal the vector ordinal
     * @return an M-dimensional double array representing the sampled vector
     */
    public double[] applyAsDouble(long ordinal) {
        double[] result = new double[dimensions];
        generateIntoDouble(ordinal, result, 0);
        return result;
    }

    /**
     * Generates a vector for the given ordinal into an existing double array.
     *
     * @param ordinal the vector ordinal
     * @param target the target array to write into
     * @param offset the offset within the target array
     */
    public void generateIntoDouble(long ordinal, double[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
            double value = componentModels[d].sample(u);
            target[offset + d] = value;
        }
    }

    /**
     * Generates a batch of vectors for consecutive ordinals starting from the given ordinal.
     *
     * <pre>{@code
     * startOrdinal ──► [ v[0] = gen(start)     ]
     *                  [ v[1] = gen(start + 1) ]
     *                  [ ...                   ]
     *                  [ v[count-1]            ]
     * }</pre>
     *
     * @param startOrdinal the starting ordinal
     * @param count the number of vectors to generate
     * @return a 2D array where result[i] is the vector for ordinal (startOrdinal + i)
     */
    public float[][] generateBatch(long startOrdinal, int count) {
        float[][] result = new float[count][dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result[i], 0);
        }
        return result;
    }

    /**
     * Generates a flat batch of vectors for consecutive ordinals.
     *
     * <p>Vectors are stored contiguously, which is optimal for SIMD operations
     * and cache-efficient distance computations:
     *
     * <pre>{@code
     * Layout: [v0_d0, v0_d1, ..., v0_dM-1, v1_d0, v1_d1, ..., v1_dM-1, ...]
     *          ├────── vector 0 ────────┤├────── vector 1 ────────┤
     * }</pre>
     *
     * @param startOrdinal the starting ordinal
     * @param count the number of vectors to generate
     * @return a flat array containing count * dimensions floats
     */
    public float[] generateFlatBatch(long startOrdinal, int count) {
        float[] result = new float[count * dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result, i * dimensions);
        }
        return result;
    }

    /**
     * Returns the underlying vector space model.
     * @return the vector space model
     */
    public VectorSpaceModel model() {
        return model;
    }

    /**
     * Returns the dimensionality of generated vectors.
     * @return the number of dimensions M
     */
    public int dimensions() {
        return dimensions;
    }

    /**
     * Returns the number of unique vectors in the space.
     * @return the number of unique vectors N
     */
    public long uniqueVectors() {
        return uniqueVectors;
    }

    /**
     * Normalizes ordinal to valid range [0, uniqueVectors).
     *
     * <p>Handles both positive overflow (ordinal >= N wraps to ordinal % N)
     * and negative values (-1 wraps to N-1).
     */
    private long normalizeOrdinal(long ordinal) {
        long normalized = ordinal % uniqueVectors;
        return normalized < 0 ? normalized + uniqueVectors : normalized;
    }
}
