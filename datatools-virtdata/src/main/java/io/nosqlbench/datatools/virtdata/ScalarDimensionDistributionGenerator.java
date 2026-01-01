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
 * Scalar (non-SIMD) dimension distribution generator implementation.
 *
 * <p>This class provides the pure Java 11 implementation that can be explicitly
 * selected even when running on a Panama-capable JVM. It uses the same algorithm
 * as the base DimensionDistributionGenerator but is guaranteed to never use SIMD optimizations.
 *
 * <p>Use this for:
 * <ul>
 *   <li>Benchmarking to compare scalar vs SIMD performance</li>
 *   <li>Debugging when SIMD behavior differs unexpectedly</li>
 *   <li>Environments where SIMD has overhead (very small batches)</li>
 * </ul>
 */
@GeneratorName(ScalarDimensionDistributionGenerator.GENERATOR_TYPE)
@ModelType(VectorSpaceModel.class)
public final class ScalarDimensionDistributionGenerator implements VectorGenerator<VectorSpaceModel> {

    /** The generator type identifier. */
    public static final String GENERATOR_TYPE = "scalar-dimension-distribution";

    private VectorSpaceModel model;
    private int dimensions;
    private long uniqueVectors;
    private ComponentSampler[] samplers;
    private boolean initialized = false;

    /**
     * Default constructor for SPI instantiation.
     */
    public ScalarDimensionDistributionGenerator() {
    }

    /**
     * Constructs a scalar dimension distribution generator.
     * @param model the vector space model
     */
    public ScalarDimensionDistributionGenerator(VectorSpaceModel model) {
        initialize(model);
    }

    @Override
    public String getGeneratorType() {
        return GENERATOR_TYPE;
    }

    @Override
    public String getDescription() {
        return "Scalar (non-SIMD) dimension distribution generator for benchmarking";
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

    @Override
    public float[] apply(long ordinal) {
        checkInitialized();
        float[] result = new float[dimensions];
        generateInto(ordinal, result, 0);
        return result;
    }

    @Override
    public void generateInto(long ordinal, float[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
            target[offset + d] = (float) samplers[d].sample(u);
        }
    }

    @Override
    public double[] applyAsDouble(long ordinal) {
        checkInitialized();
        double[] result = new double[dimensions];
        generateIntoDouble(ordinal, result, 0);
        return result;
    }

    @Override
    public void generateIntoDouble(long ordinal, double[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
            target[offset + d] = samplers[d].sample(u);
        }
    }

    @Override
    public float[][] generateBatch(long startOrdinal, int count) {
        checkInitialized();
        float[][] result = new float[count][dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result[i], 0);
        }
        return result;
    }

    @Override
    public float[] generateFlatBatch(long startOrdinal, int count) {
        checkInitialized();
        float[] result = new float[count * dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result, i * dimensions);
        }
        return result;
    }

    @Override
    public VectorSpaceModel model() {
        return model;
    }

    @Override
    public int dimensions() {
        checkInitialized();
        return dimensions;
    }

    @Override
    public long uniqueVectors() {
        checkInitialized();
        return uniqueVectors;
    }

    private long normalizeOrdinal(long ordinal) {
        long normalized = ordinal % uniqueVectors;
        return normalized < 0 ? normalized + uniqueVectors : normalized;
    }
}
