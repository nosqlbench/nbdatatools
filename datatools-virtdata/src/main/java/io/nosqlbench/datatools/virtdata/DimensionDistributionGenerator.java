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

import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.model.VectorSpaceModel;

import java.util.Objects;

/**
 * Generator that produces vectors using per-dimension distribution sampling.
 *
 * <p>This class generates M-dimensional vectors from ordinals in a deterministic,
 * replayable manner. Each unique ordinal (0 to N-1) maps to a unique vector
 * sampled from the configured vector space model's per-dimension distributions.
 *
 * <h2>Key Insight</h2>
 *
 * <p>This generator samples each dimension independently according to its fitted
 * distribution model. This complements the {@code DimensionDistributionAnalyzer}
 * which extracts these per-dimension distributions from source data.
 *
 * <h2>Processing Pipeline</h2>
 *
 * <p>The vector generation pipeline transforms an ordinal through several stages
 * to produce a vector with proper statistical properties:
 *
 * <pre>{@code
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │                     DIMENSION DISTRIBUTION GENERATION                       │
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
 *  │  │ STAGE 3: DIMENSION DISTRIBUTION SAMPLE  │    │    ┌──────────┐    │
 *  │  │   componentModel[d].sample(u)           │────┼───►│ vector[d]│────┘
 *  │  │   u → value (Gaussian/Uniform/Empirical)│    │    └──────────┘
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
 * <tr><td>3</td><td>ComponentModel</td><td>u ∈ (0,1)</td><td>float</td><td>Per-dimension distribution sample</td></tr>
 * </table>
 *
 * <h2>Key Properties</h2>
 * <ul>
 *   <li><b>O(1) cost</b> - constant time per vector regardless of N</li>
 *   <li><b>Deterministic</b> - same ordinal always produces same vector</li>
 *   <li><b>Anti-congruent</b> - no lattice artifacts in multi-dimensional space</li>
 *   <li><b>Per-dimension control</b> - each dimension uses its own distribution model</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create a space of 1M vectors with 128 dimensions, standard normal distribution
 * VectorSpaceModel model = new VectorSpaceModel(1_000_000, 128, 0.0, 1.0);
 * DimensionDistributionGenerator gen = new DimensionDistributionGenerator(model);
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
 * @see VectorGeneratorIO
 */
@GeneratorName(DimensionDistributionGenerator.GENERATOR_TYPE)
@ModelType(VectorSpaceModel.class)
public class DimensionDistributionGenerator implements VectorGenerator<VectorSpaceModel> {

    /** The generator type identifier. */
    public static final String GENERATOR_TYPE = "dimension-distribution";

    private VectorSpaceModel model;
    private int dimensions;
    private long uniqueVectors;
    private ComponentSampler[] samplers;
    private boolean initialized = false;

    /**
     * Default constructor for SPI instantiation.
     *
     * <p>When using this constructor, you must call {@link #initialize(VectorSpaceModel)}
     * before generating vectors.
     */
    public DimensionDistributionGenerator() {
    }

    /**
     * Constructs a dimension distribution generator for the given vector space model.
     *
     * <pre>{@code
     * VectorSpaceModel ──► DimensionDistributionGenerator
     *    (N, M, distributions)    (ready to generate)
     * }</pre>
     *
     * @param model the vector space model defining N (unique vectors), M (dimensions),
     *              and per-dimension distribution models
     */
    public DimensionDistributionGenerator(VectorSpaceModel model) {
        initialize(model);
    }

    @Override
    public String getGeneratorType() {
        return GENERATOR_TYPE;
    }

    @Override
    public String getDescription() {
        return "Generates vectors by sampling each dimension's fitted distribution";
    }

    @Override
    public void initialize(VectorSpaceModel model) {
        if (this.initialized) {
            throw new IllegalStateException("Generator already initialized");
        }
        Objects.requireNonNull(model, "model cannot be null");
        this.model = model;
        this.dimensions = model.dimensions();
        this.uniqueVectors = model.uniqueVectors();
        this.samplers = ComponentSamplerFactory.forModels(model.componentModels());
        this.initialized = true;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Generator not initialized. Call initialize(model) first.");
        }
    }

    /**
     * Generates a vector for the given ordinal using permuted-stratified Gaussian sampling.
     *
     * <p>This is the main entry point implementing {@code LongFunction<float[]>}.
     *
     * @param ordinal the vector ordinal (0 to N-1); values outside this range are wrapped
     * @return an M-dimensional float array representing the sampled vector
     * @throws IllegalStateException if not initialized
     */
    @Override
    public float[] apply(long ordinal) {
        checkInitialized();
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
     * @throws IllegalStateException if not initialized
     */
    @Override
    public void generateInto(long ordinal, float[] target, int offset) {
        // Stage 1: Normalize ordinal to valid range
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            // Stage 2: Get stratified unit-interval value for this dimension
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);

            // Stage 3: Apply inverse CDF transform via sampler
            double value = samplers[d].sample(u);

            target[offset + d] = (float) value;
        }
    }

    /**
     * Generates a vector for the given ordinal as double precision.
     *
     * @param ordinal the vector ordinal
     * @return an M-dimensional double array representing the sampled vector
     * @throws IllegalStateException if not initialized
     */
    @Override
    public double[] applyAsDouble(long ordinal) {
        checkInitialized();
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
     * @throws IllegalStateException if not initialized
     */
    @Override
    public void generateIntoDouble(long ordinal, double[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
            target[offset + d] = samplers[d].sample(u);
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
     * @throws IllegalStateException if not initialized
     */
    @Override
    public float[][] generateBatch(long startOrdinal, int count) {
        checkInitialized();
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
     * @throws IllegalStateException if not initialized
     */
    @Override
    public float[] generateFlatBatch(long startOrdinal, int count) {
        checkInitialized();
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
    @Override
    public VectorSpaceModel model() {
        return model;
    }

    /**
     * Returns the dimensionality of generated vectors.
     * @return the number of dimensions M
     * @throws IllegalStateException if not initialized
     */
    @Override
    public int dimensions() {
        checkInitialized();
        return dimensions;
    }

    /**
     * Returns the number of unique vectors in the space.
     * @return the number of unique vectors N
     * @throws IllegalStateException if not initialized
     */
    @Override
    public long uniqueVectors() {
        checkInitialized();
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
