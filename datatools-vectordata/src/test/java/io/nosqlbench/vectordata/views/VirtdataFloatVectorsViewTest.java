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

import io.nosqlbench.datatools.virtdata.DimensionDistributionGenerator;
import io.nosqlbench.datatools.virtdata.VectorGenerator;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.Indexed;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for the VirtdataFloatVectorsView class.
///
/// Verifies that generator-backed views correctly implement
/// the BaseVectors and QueryVectors interfaces.
public class VirtdataFloatVectorsViewTest {

    private static VectorGenerator<VectorSpaceModel> generator;
    private static final int DIMENSIONS = 64;
    private static final int COUNT = 1000;

    @BeforeAll
    static void setup() {
        // Create a simple Gaussian model for testing
        VectorSpaceModel model = new VectorSpaceModel(100000L, DIMENSIONS, 0.0, 1.0);
        // Directly instantiate generator (both Java 11 and Java 25 versions now implement VectorGenerator)
        generator = new DimensionDistributionGenerator(model);
    }

    @Test
    void testImplementsInterfaces() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        // Should implement both BaseVectors and QueryVectors
        assertThat(view).isInstanceOf(BaseVectors.class);
        assertThat(view).isInstanceOf(QueryVectors.class);
    }

    @Test
    void testGetDimensions() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        assertThat(view.getVectorDimensions()).isEqualTo(DIMENSIONS);
    }

    @Test
    void testGetCount() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        assertThat(view.getCount()).isEqualTo(COUNT);
    }

    @Test
    void testGetUnboundedCount() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator);

        // Unbounded should return model's unique vectors count
        assertThat(view.getCount()).isGreaterThan(0);
        assertThat(view.isBounded()).isFalse();
    }

    @Test
    void testGetSingleVector() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        float[] vector = view.get(42);

        assertThat(vector).hasSize(DIMENSIONS);
    }

    @Test
    void testDeterminism() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        // Same index should always return same vector
        float[] v1 = view.get(42);
        float[] v2 = view.get(42);

        assertThat(v1).isEqualTo(v2);
    }

    @Test
    void testGetRange() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        float[][] batch = view.getRange(0, 10);

        assertThat(batch.length).isEqualTo(10);
        for (float[] vector : batch) {
            assertThat(vector).hasSize(DIMENSIONS);
        }

        // Verify range matches individual gets
        for (int i = 0; i < 10; i++) {
            assertThat(batch[i]).isEqualTo(view.get(i));
        }
    }

    @Test
    void testGetIndexed() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        Indexed<float[]> indexed = view.getIndexed(42);

        assertThat(indexed.index()).isEqualTo(42);
        assertThat(indexed.value()).hasSize(DIMENSIONS);
        assertThat(indexed.value()).isEqualTo(view.get(42));
    }

    @Test
    void testGetIndexedRange() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        Indexed<float[]>[] range = view.getIndexedRange(10, 15);

        assertThat(range).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(range[i].index()).isEqualTo(10 + i);
            assertThat(range[i].value()).isEqualTo(view.get(10 + i));
        }
    }

    @Test
    void testPrebufferIsNoop() throws ExecutionException, InterruptedException {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        // Prebuffer should return immediately (no-op for generator-backed views)
        CompletableFuture<Void> future = view.prebuffer(0, 100);
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isNull();

        CompletableFuture<Void> fullFuture = view.prebuffer();
        assertThat(fullFuture.isDone()).isTrue();
    }

    @Test
    void testToList() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, 100);

        List<float[]> list = view.toList();

        assertThat(list).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(list.get(i)).isEqualTo(view.get(i));
        }
    }

    @Test
    void testToListWithTransform() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, 10);

        List<Integer> lengths = view.toList(v -> v.length);

        assertThat(lengths).hasSize(10);
        assertThat(lengths).containsOnly(DIMENSIONS);
    }

    @Test
    void testIterator() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, 10);

        int count = 0;
        for (float[] vector : view) {
            assertThat(vector).hasSize(DIMENSIONS);
            count++;
        }
        assertThat(count).isEqualTo(10);
    }

    @Test
    void testUnboundedThrowsOnIteration() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator);

        assertThatThrownBy(view::iterator)
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("unbounded");
    }

    @Test
    void testUnboundedThrowsOnToList() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator);

        assertThatThrownBy(view::toList)
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("unbounded");
    }

    @Test
    void testGetDataType() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        assertThat(view.getDataType()).isEqualTo(float[].class);
    }

    @Test
    void testGetGenerator() {
        VirtdataFloatVectorsView view = new VirtdataFloatVectorsView(generator, COUNT);

        assertThat(view.getGenerator()).isSameAs(generator);
    }
}
