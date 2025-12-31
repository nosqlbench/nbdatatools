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
 * Scalar (non-SIMD) vector generator implementation.
 *
 * <p>This class provides the pure Java 11 implementation that can be explicitly
 * selected even when running on a Panama-capable JVM. It uses the same algorithm
 * as the base VectorGen class but is guaranteed to never use SIMD optimizations.
 *
 * <p>Use this for:
 * <ul>
 *   <li>Benchmarking to compare scalar vs SIMD performance</li>
 *   <li>Debugging when SIMD behavior differs unexpectedly</li>
 *   <li>Environments where SIMD has overhead (very small batches)</li>
 * </ul>
 */
public final class ScalarVectorGen implements LongFunction<float[]> {

    private final VectorSpaceModel model;
    private final int dimensions;
    private final long uniqueVectors;
    private final double[] means;
    private final double[] stdDevs;

    /**
     * Constructs a scalar vector generator.
     * @param model the vector space model
     */
    public ScalarVectorGen(VectorSpaceModel model) {
        this.model = model;
        this.dimensions = model.dimensions();
        this.uniqueVectors = model.uniqueVectors();

        this.means = new double[dimensions];
        this.stdDevs = new double[dimensions];
        for (int d = 0; d < dimensions; d++) {
            GaussianComponentModel cm = model.componentModel(d);
            means[d] = cm.mean();
            stdDevs[d] = cm.stdDev();
        }
    }

    @Override
    public float[] apply(long ordinal) {
        float[] result = new float[dimensions];
        generateInto(ordinal, result, 0);
        return result;
    }

    /**
     * Generates a vector into an existing array.
     * @param ordinal the vector ordinal
     * @param target the target array
     * @param offset the offset within target
     */
    public void generateInto(long ordinal, float[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
            double value = InverseGaussianCDF.quantile(u, means[d], stdDevs[d]);
            target[offset + d] = (float) value;
        }
    }

    /**
     * Generates a vector as double precision.
     * @param ordinal the vector ordinal
     * @return an M-dimensional double array
     */
    public double[] applyAsDouble(long ordinal) {
        double[] result = new double[dimensions];
        generateIntoDouble(ordinal, result, 0);
        return result;
    }

    /**
     * Generates a vector into an existing double array.
     * @param ordinal the vector ordinal
     * @param target the target array
     * @param offset the offset within target
     */
    public void generateIntoDouble(long ordinal, double[] target, int offset) {
        long normalizedOrdinal = normalizeOrdinal(ordinal);

        for (int d = 0; d < dimensions; d++) {
            double u = StratifiedSampler.unitIntervalValue(normalizedOrdinal, d, uniqueVectors);
            double value = InverseGaussianCDF.quantile(u, means[d], stdDevs[d]);
            target[offset + d] = value;
        }
    }

    /**
     * Generates a batch of vectors.
     * @param startOrdinal the starting ordinal
     * @param count the number of vectors
     * @return a 2D array of vectors
     */
    public float[][] generateBatch(long startOrdinal, int count) {
        float[][] result = new float[count][dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result[i], 0);
        }
        return result;
    }

    /**
     * Generates a flat batch of vectors.
     * @param startOrdinal the starting ordinal
     * @param count the number of vectors
     * @return a flat array of count * dimensions floats
     */
    public float[] generateFlatBatch(long startOrdinal, int count) {
        float[] result = new float[count * dimensions];
        for (int i = 0; i < count; i++) {
            generateInto(startOrdinal + i, result, i * dimensions);
        }
        return result;
    }

    /**
     * Returns the vector space model.
     * @return the model
     */
    public VectorSpaceModel model() {
        return model;
    }

    /**
     * Returns the dimensionality.
     * @return number of dimensions
     */
    public int dimensions() {
        return dimensions;
    }

    /**
     * Returns the unique vector count.
     * @return number of unique vectors
     */
    public long uniqueVectors() {
        return uniqueVectors;
    }

    private long normalizeOrdinal(long ordinal) {
        long normalized = ordinal % uniqueVectors;
        return normalized < 0 ? normalized + uniqueVectors : normalized;
    }
}
