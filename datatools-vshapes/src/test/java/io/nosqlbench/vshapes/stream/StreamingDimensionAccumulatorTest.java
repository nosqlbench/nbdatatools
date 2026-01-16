package io.nosqlbench.vshapes.stream;

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

import io.nosqlbench.vshapes.extract.DimensionStatistics;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class StreamingDimensionAccumulatorTest {

    @Test
    void constructor_validDimension() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(5);
        assertEquals(5, acc.getDimension());
        assertEquals(0, acc.getCount());
    }

    @Test
    void constructor_invalidDimension_throws() {
        assertThrows(IllegalArgumentException.class, () -> new StreamingDimensionAccumulator(-1));
    }

    @Test
    void add_singleValue() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        acc.add(5.0f);

        assertEquals(1, acc.getCount());
        assertEquals(5.0, acc.getMean(), 0.001);
        assertEquals(5.0, acc.getMin(), 0.001);
        assertEquals(5.0, acc.getMax(), 0.001);
    }

    @Test
    void add_multipleValues() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        acc.add(1.0f);
        acc.add(2.0f);
        acc.add(3.0f);
        acc.add(4.0f);
        acc.add(5.0f);

        assertEquals(5, acc.getCount());
        assertEquals(3.0, acc.getMean(), 0.001);
        assertEquals(1.0, acc.getMin(), 0.001);
        assertEquals(5.0, acc.getMax(), 0.001);

        // Variance of [1,2,3,4,5] = 2.0
        assertEquals(2.0, acc.getVariance(), 0.001);
    }

    @Test
    void addAll_floatArray() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        float[] values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        acc.addAll(values);

        assertEquals(5, acc.getCount());
        assertEquals(3.0, acc.getMean(), 0.001);
    }

    @Test
    void addAll_emptyArray() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        acc.addAll(new float[0]);

        assertEquals(0, acc.getCount());
    }

    @Test
    void addAll_nullArray() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        acc.addAll((float[]) null);

        assertEquals(0, acc.getCount());
    }

    @Test
    void combine_mergeTwoAccumulators() {
        StreamingDimensionAccumulator acc1 = new StreamingDimensionAccumulator(0);
        acc1.addAll(new float[] {1.0f, 2.0f, 3.0f});

        StreamingDimensionAccumulator acc2 = new StreamingDimensionAccumulator(0);
        acc2.addAll(new float[] {4.0f, 5.0f});

        acc1.combine(acc2);

        assertEquals(5, acc1.getCount());
        assertEquals(3.0, acc1.getMean(), 0.001);
        assertEquals(1.0, acc1.getMin(), 0.001);
        assertEquals(5.0, acc1.getMax(), 0.001);
    }

    @Test
    void combine_differentDimensions_throws() {
        StreamingDimensionAccumulator acc1 = new StreamingDimensionAccumulator(0);
        StreamingDimensionAccumulator acc2 = new StreamingDimensionAccumulator(1);

        assertThrows(IllegalArgumentException.class, () -> acc1.combine(acc2));
    }

    @Test
    void combine_emptyAccumulator() {
        StreamingDimensionAccumulator acc1 = new StreamingDimensionAccumulator(0);
        acc1.addAll(new float[] {1.0f, 2.0f, 3.0f});

        StreamingDimensionAccumulator acc2 = new StreamingDimensionAccumulator(0);

        acc1.combine(acc2);

        assertEquals(3, acc1.getCount());
        assertEquals(2.0, acc1.getMean(), 0.001);
    }

    @Test
    void combine_intoEmptyAccumulator() {
        StreamingDimensionAccumulator acc1 = new StreamingDimensionAccumulator(0);

        StreamingDimensionAccumulator acc2 = new StreamingDimensionAccumulator(0);
        acc2.addAll(new float[] {1.0f, 2.0f, 3.0f});

        acc1.combine(acc2);

        assertEquals(3, acc1.getCount());
        assertEquals(2.0, acc1.getMean(), 0.001);
    }

    @Test
    void toStatistics_producesValidResult() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(5);
        acc.addAll(new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f});

        DimensionStatistics stats = acc.toStatistics();

        assertEquals(5, stats.dimension());
        assertEquals(5, stats.count());
        assertEquals(3.0, stats.mean(), 0.001);
        assertEquals(1.0, stats.min(), 0.001);
        assertEquals(5.0, stats.max(), 0.001);
    }

    @Test
    void toStatistics_noData_throws() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        assertThrows(IllegalStateException.class, acc::toStatistics);
    }

    @Test
    void reset_clearsState() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        acc.addAll(new float[] {1.0f, 2.0f, 3.0f});
        acc.reset();

        assertEquals(0, acc.getCount());
        assertEquals(0.0, acc.getMean(), 0.001);
        assertEquals(Double.POSITIVE_INFINITY, acc.getMin());
        assertEquals(Double.NEGATIVE_INFINITY, acc.getMax());
    }

    @Test
    void welfordAccuracy_matchesBatchComputation() {
        // Generate random data
        Random random = new Random(42);
        float[] data = new float[10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) (random.nextGaussian() * 2.0 + 5.0);
        }

        // Streaming computation
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(0);
        acc.addAll(data);

        // Batch computation
        double sum = 0;
        for (float v : data) sum += v;
        double batchMean = sum / data.length;

        double sumSquares = 0;
        for (float v : data) sumSquares += (v - batchMean) * (v - batchMean);
        double batchVariance = sumSquares / data.length;

        double min = Double.MAX_VALUE, max = Double.MIN_VALUE;
        for (float v : data) {
            if (v < min) min = v;
            if (v > max) max = v;
        }

        // Compare
        assertEquals(batchMean, acc.getMean(), 0.0001, "Mean mismatch");
        assertEquals(batchVariance, acc.getVariance(), 0.0001, "Variance mismatch");
        assertEquals(min, acc.getMin(), 0.0001, "Min mismatch");
        assertEquals(max, acc.getMax(), 0.0001, "Max mismatch");
    }

    @Test
    void parallelCombine_matchesSinglePass() {
        // Generate random data
        Random random = new Random(123);
        float[] data = new float[10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) (random.nextGaussian() * 3.0 + 10.0);
        }

        // Single-pass computation
        StreamingDimensionAccumulator single = new StreamingDimensionAccumulator(0);
        single.addAll(data);

        // Parallel computation (4 chunks)
        StreamingDimensionAccumulator[] chunks = new StreamingDimensionAccumulator[4];
        int chunkSize = data.length / 4;
        for (int c = 0; c < 4; c++) {
            chunks[c] = new StreamingDimensionAccumulator(0);
            int start = c * chunkSize;
            int end = (c == 3) ? data.length : start + chunkSize;
            for (int i = start; i < end; i++) {
                chunks[c].add(data[i]);
            }
        }

        // Combine chunks
        StreamingDimensionAccumulator combined = new StreamingDimensionAccumulator(0);
        for (StreamingDimensionAccumulator chunk : chunks) {
            combined.combine(chunk);
        }

        // Compare results
        assertEquals(single.getCount(), combined.getCount(), "Count mismatch");
        assertEquals(single.getMean(), combined.getMean(), 0.0001, "Mean mismatch");
        assertEquals(single.getVariance(), combined.getVariance(), 0.01, "Variance mismatch");
        assertEquals(single.getMin(), combined.getMin(), 0.0001, "Min mismatch");
        assertEquals(single.getMax(), combined.getMax(), 0.0001, "Max mismatch");
    }

    @Test
    void toString_containsRelevantInfo() {
        StreamingDimensionAccumulator acc = new StreamingDimensionAccumulator(5);
        acc.addAll(new float[] {1.0f, 2.0f, 3.0f});

        String str = acc.toString();
        assertTrue(str.contains("dim=5"));
        assertTrue(str.contains("n=3"));
    }
}
