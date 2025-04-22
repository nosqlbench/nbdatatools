package io.nosqlbench.vectordata;

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


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import org.assertj.core.data.Offset;
import static org.assertj.core.data.Offset.offset;

/**
 * Test for generating a JSON dataset file with specified dimensions,
 * where each subarray has a mean around its index and variability proportional to its index.
 */
public class TestDatasetFileGenerationTest {

    @Test
    @Disabled
    void testGenerateDatasetFile(@TempDir Path tempDir) throws Exception {
        int n = 1000;
        int m = 100;
        Random rand = new Random(42);
        // base dithering noise with variance 0.001
        double baseNoiseStd = Math.sqrt(0.001);
        List<List<Double>> dataset = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double variability = (double) i / (n - 1);
            List<Double> subarray = new ArrayList<>(m);
            for (int j = 0; j < m; j++) {
                // mean at i, variability proportional to i, plus base dithering noise
                double value = i
                    + variability * rand.nextGaussian()
                    + baseNoiseStd * rand.nextGaussian();
                subarray.add(value);
            }
            dataset.add(subarray);
        }
        Path file = tempDir.resolve("test_dataset.json");
        System.out.println("Writing dataset to " + file);
        try (Writer writer = Files.newBufferedWriter(file)) {
            new Gson().toJson(dataset, writer);
        }
        assertThat(Files.exists(file)).isTrue();
        // Read back and verify dimensions
        List<List<Double>> loaded = new Gson().fromJson(
            Files.newBufferedReader(file),
            new TypeToken<List<List<Double>>>() {}.getType()
        );
        assertThat(loaded).hasSize(n);
        for (List<Double> sub : loaded) {
            assertThat(sub).hasSize(m);
        }
        // Verify first subarray (i=0) has only base dithering noise: mean≈0, variance≈0.001
        List<Double> first = loaded.get(0);
        // compute empirical mean
        double mean0 = first.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        // compute empirical variance
        double var0 = first.stream()
            .mapToDouble(v -> (v - mean0) * (v - mean0))
            .sum() / first.size();
        // mean should be near zero
        assertThat(mean0).isBetween(-0.01d, 0.01d);
        // variance should be near 0.001 (±0.0005)
        assertThat(var0).isBetween(0.0005d, 0.0015d);
    }

    @Test
    void testWriteFvec(@TempDir Path tempDir) throws Exception {
        int n = 1000;
        int m = 100;
        Random rand = new Random(42);
        double baseNoiseStd = Math.sqrt(0.001);
        List<List<Double>> dataset = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double variability = (double) i / (n - 1);
            List<Double> sub = new ArrayList<>(m);
            for (int j = 0; j < m; j++) {
                double value = i + variability * rand.nextGaussian() + baseNoiseStd * rand.nextGaussian();
                sub.add(value);
            }
            dataset.add(sub);
        }
        Path fvec = tempDir.resolve("dataset.fvec");
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(fvec))) {
            for (List<Double> vec : dataset) {
                dos.writeInt(m);
                for (Double v : vec) {
                    dos.writeFloat(v.floatValue());
                }
            }
        }
        assertThat(Files.exists(fvec)).isTrue();
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(fvec))) {
            int dim = dis.readInt();
            assertThat(dim).isEqualTo(m);
            for (int j = 0; j < m; j++) {
                float fv = dis.readFloat();
                float expected = dataset.get(0).get(j).floatValue();
                assertThat(fv).isCloseTo(expected, offset(1e-6f));
            }
        }
    }

    @Test
    void testWriteNormalizedFvec(@TempDir Path tempDir) throws Exception {
        int n = 1000;
        int m = 100;
        Random rand = new Random(42);
        double baseNoiseStd = Math.sqrt(0.001);
        List<List<Double>> dataset = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double variability = (double) i / (n - 1);
            List<Double> sub = new ArrayList<>(m);
            for (int j = 0; j < m; j++) {
                double value = i + variability * rand.nextGaussian() + baseNoiseStd * rand.nextGaussian();
                sub.add(value);
            }
            dataset.add(sub);
        }
        Path nfvec = tempDir.resolve("dataset_normalized.fvec");
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(nfvec))) {
            for (List<Double> vec : dataset) {
                double sum2 = 0.0;
                for (Double v : vec) {
                    double d = v;
                    sum2 += d * d;
                }
                double norm = Math.sqrt(sum2);
                dos.writeInt(m);
                for (Double v : vec) {
                    dos.writeFloat((float) (v / norm));
                }
            }
        }
        assertThat(Files.exists(nfvec)).isTrue();
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(nfvec))) {
            int dim = dis.readInt();
            assertThat(dim).isEqualTo(m);
            double sum2 = 0.0;
            for (int j = 0; j < m; j++) {
                float fv = dis.readFloat();
                sum2 += fv * fv;
            }
            double norm0 = Math.sqrt(sum2);
            assertThat(norm0).isBetween(0.999d, 1.001d);
        }
    }
}
