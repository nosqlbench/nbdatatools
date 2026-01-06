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

import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.vshapes.model.VectorSpaceModel;

import java.util.Objects;

/**
 * Dimension distribution generator that accepts pre-configured samplers.
 *
 * <p>This generator is similar to {@link DimensionDistributionGenerator} but
 * allows injection of pre-built samplers. This enables optional features like
 * LERP optimization without modifying the core generation logic.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Create with LERP-optimized samplers
 * ComponentSampler[] lerpSamplers = LerpSamplerFactory.forModels(model.scalarModels());
 * VectorGenerator<VectorSpaceModel> gen = new ConfigurableDimensionDistributionGenerator(model, lerpSamplers);
 * }</pre>
 *
 * @see DimensionDistributionGenerator
 * @see VectorGenFactory#create(VectorSpaceModel, GeneratorOptions)
 */
public final class ConfigurableDimensionDistributionGenerator implements VectorGenerator<VectorSpaceModel> {

    /** The generator type identifier. */
    public static final String GENERATOR_TYPE = "configurable-dimension-distribution";

    private final VectorSpaceModel model;
    private final int dimensions;
    private final long uniqueVectors;
    private final ComponentSampler[] samplers;

    /**
     * Constructs a configurable dimension distribution generator.
     *
     * @param model the vector space model
     * @param samplers the pre-configured samplers (one per dimension)
     * @throws NullPointerException if model or samplers is null
     * @throws IllegalArgumentException if samplers.length != model.dimensions()
     */
    public ConfigurableDimensionDistributionGenerator(VectorSpaceModel model, ComponentSampler[] samplers) {
        Objects.requireNonNull(model, "model cannot be null");
        Objects.requireNonNull(samplers, "samplers cannot be null");
        if (samplers.length != model.dimensions()) {
            throw new IllegalArgumentException(
                "Sampler count (" + samplers.length + ") must match model dimensions (" + model.dimensions() + ")");
        }
        this.model = model;
        this.dimensions = model.dimensions();
        this.uniqueVectors = model.uniqueVectors();
        this.samplers = samplers;
    }

    @Override
    public String getGeneratorType() {
        return GENERATOR_TYPE;
    }

    @Override
    public String getDescription() {
        return "Generates vectors using pre-configured per-dimension samplers";
    }

    @Override
    public void initialize(VectorSpaceModel model) {
        throw new IllegalStateException(
            "ConfigurableDimensionDistributionGenerator is fully initialized at construction");
    }

    @Override
    public boolean isInitialized() {
        return true; // Always initialized at construction
    }

    @Override
    public VectorSpaceModel model() {
        return model;
    }

    @Override
    public float[] apply(long ordinal) {
        float[] result = new float[dimensions];
        generateInto(ordinal, result, 0);
        return result;
    }

    @Override
    public void generateInto(long ordinal, float[] target, int offset) {
        // Normalize ordinal to valid range
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            // Get stratified unit-interval value for this dimension
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);

            // Apply inverse CDF transform via sampler
            double value = samplers[d].sample(u);

            target[offset + d] = (float) value;
        }
    }

    @Override
    public double[] applyAsDouble(long ordinal) {
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
        float[][] result = new float[count][dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result[i], 0);
        }
        return result;
    }

    @Override
    public float[] generateFlatBatch(long startOrdinal, int count) {
        float[] result = new float[count * dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result, i * dimensions);
        }
        return result;
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public long uniqueVectors() {
        return uniqueVectors;
    }

    /**
     * Normalizes ordinal to valid range [0, uniqueVectors).
     */
    private long normalizeOrdinal(long ordinal) {
        long normalized = ordinal % uniqueVectors;
        return normalized < 0 ? normalized + uniqueVectors : normalized;
    }
}
