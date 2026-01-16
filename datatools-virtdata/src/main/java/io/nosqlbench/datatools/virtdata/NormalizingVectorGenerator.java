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

/**
 * Wrapper that applies L2 normalization to generated vectors.
 *
 * <p>Transforms vectors to unit length while preserving direction:
 * <pre>{@code
 * v_normalized = v / ||v||_2
 * where ||v||_2 = sqrt(sum(v_i^2))
 * }</pre>
 *
 * <p>All generated vectors will have L2 norm equal to 1.0 (within
 * floating-point precision).
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * VectorGenerator<VectorSpaceModel> base = VectorGenFactory.create(model);
 * VectorGenerator<VectorSpaceModel> normalized = new NormalizingVectorGenerator<>(base);
 *
 * float[] v = normalized.apply(42L);
 * // ||v||_2 = 1.0
 * }</pre>
 *
 * <h2>Edge Cases</h2>
 * <ul>
 *   <li><b>Zero vectors</b>: Vectors with zero or near-zero magnitude are left unchanged</li>
 *   <li><b>NaN/Infinity</b>: If any component is NaN or infinite, normalization may produce unexpected results</li>
 * </ul>
 *
 * @param <M> the model type
 */
public final class NormalizingVectorGenerator<M> implements VectorGenerator<M> {

    /** Epsilon for detecting near-zero vectors. */
    private static final double EPSILON = 1e-10;

    private final VectorGenerator<M> delegate;
    private final int dimensions;

    /**
     * Creates a normalizing wrapper around the given generator.
     *
     * @param delegate the generator to wrap (must be initialized)
     * @throws IllegalStateException if delegate is not initialized
     */
    public NormalizingVectorGenerator(VectorGenerator<M> delegate) {
        if (!delegate.isInitialized()) {
            throw new IllegalStateException("Delegate generator must be initialized");
        }
        this.delegate = delegate;
        this.dimensions = delegate.dimensions();
    }

    @Override
    public String getGeneratorType() {
        return "normalizing-" + delegate.getGeneratorType();
    }

    @Override
    public String getDescription() {
        return "L2-normalizing wrapper for: " + delegate.getDescription();
    }

    @Override
    public void initialize(M model) {
        throw new IllegalStateException(
            "NormalizingVectorGenerator wraps an initialized generator. " +
            "Initialize the delegate generator instead.");
    }

    @Override
    public boolean isInitialized() {
        return true; // Always initialized since we require initialized delegate
    }

    @Override
    public M model() {
        return delegate.model();
    }

    @Override
    public float[] apply(long ordinal) {
        float[] result = delegate.apply(ordinal);
        normalizeInPlace(result, 0, dimensions);
        return result;
    }

    @Override
    public void generateInto(long ordinal, float[] target, int offset) {
        delegate.generateInto(ordinal, target, offset);
        normalizeInPlace(target, offset, dimensions);
    }

    @Override
    public double[] applyAsDouble(long ordinal) {
        double[] result = delegate.applyAsDouble(ordinal);
        normalizeInPlaceDouble(result, 0, dimensions);
        return result;
    }

    @Override
    public void generateIntoDouble(long ordinal, double[] target, int offset) {
        delegate.generateIntoDouble(ordinal, target, offset);
        normalizeInPlaceDouble(target, offset, dimensions);
    }

    @Override
    public float[][] generateBatch(long startOrdinal, int count) {
        float[][] result = delegate.generateBatch(startOrdinal, count);
        for (float[] vector : result) {
            normalizeInPlace(vector, 0, dimensions);
        }
        return result;
    }

    @Override
    public float[] generateFlatBatch(long startOrdinal, int count) {
        float[] result = delegate.generateFlatBatch(startOrdinal, count);
        for (int i = 0; i < count; i++) {
            normalizeInPlace(result, i * dimensions, dimensions);
        }
        return result;
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public long uniqueVectors() {
        return delegate.uniqueVectors();
    }

    /**
     * Normalizes a vector segment in place to unit L2 norm.
     *
     * <p>Computes: v[i] = v[i] / ||v||_2 for i in [offset, offset+length)
     *
     * @param array the array containing the vector
     * @param offset starting index of the vector
     * @param length number of elements (dimensions)
     */
    private static void normalizeInPlace(float[] array, int offset, int length) {
        // Compute sum of squares
        double sumSq = 0.0;
        for (int i = 0; i < length; i++) {
            float v = array[offset + i];
            sumSq += (double) v * v;
        }

        // Compute norm
        double norm = Math.sqrt(sumSq);

        // Handle zero/near-zero vectors
        if (norm < EPSILON) {
            return; // Leave unchanged
        }

        // Normalize
        float scale = (float) (1.0 / norm);
        for (int i = 0; i < length; i++) {
            array[offset + i] *= scale;
        }
    }

    /**
     * Normalizes a double-precision vector segment in place.
     */
    private static void normalizeInPlaceDouble(double[] array, int offset, int length) {
        double sumSq = 0.0;
        for (int i = 0; i < length; i++) {
            double v = array[offset + i];
            sumSq += v * v;
        }

        double norm = Math.sqrt(sumSq);
        if (norm < EPSILON) {
            return;
        }

        double scale = 1.0 / norm;
        for (int i = 0; i < length; i++) {
            array[offset + i] *= scale;
        }
    }

    /**
     * Computes the L2 norm of a vector.
     *
     * @param vector the vector
     * @return the L2 norm (Euclidean length)
     */
    public static double l2Norm(float[] vector) {
        double sumSq = 0.0;
        for (float v : vector) {
            sumSq += (double) v * v;
        }
        return Math.sqrt(sumSq);
    }

    /**
     * Computes the L2 norm of a double-precision vector.
     *
     * @param vector the vector
     * @return the L2 norm (Euclidean length)
     */
    public static double l2Norm(double[] vector) {
        double sumSq = 0.0;
        for (double v : vector) {
            sumSq += v * v;
        }
        return Math.sqrt(sumSq);
    }
}
