package io.nosqlbench.vectordata.views;

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

import io.nosqlbench.datatools.virtdata.VectorGenerator;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.FloatVectors;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;

/// A generator-backed implementation of [FloatVectors] for virtual (on-the-fly) datasets.
///
/// ## Purpose
///
/// This view wraps a [VectorGenerator] to provide lazy, deterministic vector generation
/// through the standard [FloatVectors] interface. This allows virtdata-generated vectors
/// to be used wherever file-backed vectors are expected.
///
/// ## Characteristics
///
/// - **Deterministic**: Same index always produces the same vector
/// - **Lazy**: Vectors are generated on-demand, not pre-materialized
/// - **No prebuffering**: The `prebuffer()` methods return immediately (no-op)
/// - **Bounded**: Requires an explicit count (cardinality) to be useful
///
/// ## Cardinality
///
/// For unbounded datasets (count = -1 or Long.MAX_VALUE), this view can still generate
/// vectors but certain operations like iteration and `toList()` will fail or produce
/// incomplete results. Use bounded windows when ground truth computation is required.
///
/// ## Usage
///
/// ```java
/// // Create from a VectorGenerator
/// VectorGenerator<VectorSpaceModel> gen = VectorGeneratorIO.createForModel(model);
/// FloatVectors view = new VirtdataFloatVectorsView(gen, 1_000_000);
///
/// // Use like any FloatVectors
/// float[] v42 = view.get(42);
/// float[][] batch = view.getRange(0, 1000);
/// ```
///
/// @see VectorGenerator
/// @see FloatVectors
/// @see BaseVectors
/// @see QueryVectors
/// @see io.nosqlbench.vectordata.layout.SourceType#VIRTDATA
public class VirtdataFloatVectorsView implements BaseVectors, QueryVectors {

    private final VectorGenerator<?> generator;
    private final int count;
    private final int dimensions;

    /// Sentinel value indicating unbounded cardinality.
    public static final int UNBOUNDED = -1;

    /// Creates a virtdata-backed FloatVectors view.
    ///
    /// @param generator the initialized vector generator
    /// @param count the number of vectors (use UNBOUNDED for open-ended)
    /// @throws IllegalArgumentException if generator is not initialized
    public VirtdataFloatVectorsView(VectorGenerator<?> generator, int count) {
        if (!generator.isInitialized()) {
            throw new IllegalArgumentException("VectorGenerator must be initialized before creating view");
        }
        this.generator = generator;
        this.count = count;
        this.dimensions = generator.dimensions();
    }

    /// Creates a virtdata-backed FloatVectors view with unbounded cardinality.
    ///
    /// @param generator the initialized vector generator
    /// @throws IllegalArgumentException if generator is not initialized
    public VirtdataFloatVectorsView(VectorGenerator<?> generator) {
        this(generator, UNBOUNDED);
    }

    @Override
    public float[] get(long index) {
        return generator.apply(index);
    }

    @Override
    public Future<float[]> getAsync(long index) {
        return CompletableFuture.completedFuture(get(index));
    }

    @Override
    public float[][] getRange(long startInclusive, long endExclusive) {
        int rangeSize = (int) (endExclusive - startInclusive);
        return generator.generateBatch(startInclusive, rangeSize);
    }

    @Override
    public Future<float[][]> getRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getRange(startInclusive, endExclusive));
    }

    @Override
    public Indexed<float[]> getIndexed(long index) {
        return new Indexed<>(index, get(index));
    }

    @Override
    public Future<Indexed<float[]>> getIndexedAsync(long index) {
        return CompletableFuture.completedFuture(getIndexed(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<float[]>[] getIndexedRange(long startInclusive, long endExclusive) {
        int rangeSize = (int) (endExclusive - startInclusive);
        Indexed<float[]>[] result = new Indexed[rangeSize];
        float[][] vectors = generator.generateBatch(startInclusive, rangeSize);
        for (int i = 0; i < rangeSize; i++) {
            result[i] = new Indexed<>(startInclusive + i, vectors[i]);
        }
        return result;
    }

    @Override
    public Future<Indexed<float[]>[]> getIndexedRangeAsync(long startInclusive, long endExclusive) {
        return CompletableFuture.completedFuture(getIndexedRange(startInclusive, endExclusive));
    }

    @Override
    public int getCount() {
        if (count == UNBOUNDED) {
            // Return a large value but not MAX_VALUE to avoid overflow issues
            long unique = generator.uniqueVectors();
            return unique > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) unique;
        }
        return count;
    }

    @Override
    public int getVectorDimensions() {
        return dimensions;
    }

    @Override
    public Class<?> getDataType() {
        return float[].class;
    }

    /// Returns true if this view has bounded cardinality.
    ///
    /// @return true if count was explicitly specified
    public boolean isBounded() {
        return count != UNBOUNDED;
    }

    /// Prebuffering is a no-op for generator-backed views.
    ///
    /// Since vectors are generated on-demand, there is nothing to prebuffer.
    @Override
    public CompletableFuture<Void> prebuffer(long startIncl, long endExcl) {
        return CompletableFuture.completedFuture(null);
    }

    /// Prebuffering is a no-op for generator-backed views.
    @Override
    public CompletableFuture<Void> prebuffer() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public List<float[]> toList() {
        if (count == UNBOUNDED) {
            throw new UnsupportedOperationException(
                "Cannot convert unbounded virtdata view to list. Specify a window to bound cardinality.");
        }
        List<float[]> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(get(i));
        }
        return result;
    }

    @Override
    public <U> List<U> toList(Function<float[], U> f) {
        if (count == UNBOUNDED) {
            throw new UnsupportedOperationException(
                "Cannot convert unbounded virtdata view to list. Specify a window to bound cardinality.");
        }
        List<U> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(f.apply(get(i)));
        }
        return result;
    }

    @Override
    public Iterator<float[]> iterator() {
        if (count == UNBOUNDED) {
            throw new UnsupportedOperationException(
                "Cannot iterate unbounded virtdata view. Specify a window to bound cardinality.");
        }
        return new Iterator<>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current < count;
            }

            @Override
            public float[] next() {
                return get(current++);
            }
        };
    }

    /// Returns the underlying generator.
    ///
    /// @return the vector generator
    public VectorGenerator<?> getGenerator() {
        return generator;
    }

    @Override
    public String toString() {
        return "VirtdataFloatVectorsView{" +
            "dimensions=" + dimensions +
            ", count=" + (count == UNBOUNDED ? "unbounded" : count) +
            ", generator=" + generator.getGeneratorType() +
            '}';
    }
}
