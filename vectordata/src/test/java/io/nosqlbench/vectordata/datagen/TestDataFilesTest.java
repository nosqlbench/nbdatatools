package io.nosqlbench.vectordata.datagen;

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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import com.google.gson.Gson;
import java.io.DataInputStream;
import java.io.BufferedReader;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;

/**
/// Unit tests for TestDataFiles utility: vector generation and file export.
*/
public class TestDataFilesTest {

  @Test
  void testGenVectorsAllZero() {
    int count = 5;
    int dims = 3;
    // variability=0 and scale=0 => all zeros, no zeros/duplicates overrides
    float[][] vecs = TestDataFiles.genVectors(count, dims, 42L, 0.0, 0.0, 0.0, 0.0);
    // outer array length
    assertThat(vecs.length).isEqualTo(count);
    for (float[] v : vecs) {
      // each vector dimension
      assertThat(v.length).isEqualTo(dims);
      for (float x : v) {
        assertThat(x).isEqualTo(0.0f);
      }
    }
  }

  @Test
  void testSaveToFileFvec(@TempDir Path tmp) throws Exception {
    float[][] tv = new float[][] { {1.1f, 2.2f}, {3.3f, 4.4f} };
    Path file = tmp.resolve("test.fvec");
    TestDataFiles.saveToFile(tv, file, TestDataFiles.Format.fvec);
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(file))) {
      for (int i = 0; i < tv.length; i++) {
        int len = dis.readInt();
        assertThat(len).isEqualTo(tv[i].length);
        for (int j = 0; j < len; j++) {
          float v = dis.readFloat();
          assertThat(v).isCloseTo(tv[i][j], offset(1e-6f));
        }
      }
    }
  }

  @Test
  void testSaveToFileIvec(@TempDir Path tmp) throws Exception {
    float[][] tv = new float[][] { {1.2f, 2.7f}, {-3.4f, 4.5f} };
    Path file = tmp.resolve("test.ivec");
    TestDataFiles.saveToFile(tv, file, TestDataFiles.Format.ivec);
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(file))) {
      for (int i = 0; i < tv.length; i++) {
        int len = dis.readInt();
        assertThat(len).isEqualTo(tv[i].length);
        for (int j = 0; j < len; j++) {
          int iv = dis.readInt();
          int expected = Math.round(tv[i][j]);
          assertThat(iv).isEqualTo(expected);
        }
      }
    }
  }

  @Test
  void testSaveToFileBvec(@TempDir Path tmp) throws Exception {
    float[][] tv = new float[][] { {1.4f, -2.3f}, {127.5f, -128.6f} };
    Path file = tmp.resolve("test.bvec");
    TestDataFiles.saveToFile(tv, file, TestDataFiles.Format.bvec);
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(file))) {
      for (int i = 0; i < tv.length; i++) {
        int len = dis.readInt();
        assertThat(len).isEqualTo(tv[i].length);
        for (int j = 0; j < len; j++) {
          byte bv = dis.readByte();
          byte expected = (byte) Math.round(tv[i][j]);
          assertThat(bv).isEqualTo(expected);
        }
      }
    }
  }

  @Test
  void testSaveToFileJson(@TempDir Path tmp) throws Exception {
    float[][] tv = new float[][] { {5.5f, 6.6f} };
    Path file = tmp.resolve("test.json");
    TestDataFiles.saveToFile(tv, file, TestDataFiles.Format.json);
    try (BufferedReader reader = Files.newBufferedReader(file)) {
      float[][] loaded = new Gson().fromJson(reader, float[][].class);
      // outer array length
      assertThat(loaded.length).isEqualTo(tv.length);
      for (int i = 0; i < tv.length; i++) {
        assertThat(loaded[i]).containsExactly(tv[i]);
      }
    }
  }

  @Test
  void testSaveToFileYaml(@TempDir Path tmp) throws Exception {
    float[][] tv = new float[][] { {7.7f, 8.8f}, {9.9f, 10.01f} };
    Path file = tmp.resolve("test.yaml");
    TestDataFiles.saveToFile(tv, file, TestDataFiles.Format.yaml);
    List<String> lines = Files.readAllLines(file);
    assertThat(lines).isNotEmpty();
    assertThat(lines.get(0)).isEqualTo("---");
    for (int i = 0; i < tv.length; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append("- [");
      for (int j = 0; j < tv[i].length; j++) {
        sb.append(Float.toString(tv[i][j]));
        if (j < tv[i].length - 1) sb.append(", ");
      }
      sb.append("]");
      assertThat(lines.get(i + 1)).isEqualTo(sb.toString());
    }
  }

  @Test
  void testGenVectorsVariabilityDistribution() {
    int count = 1000;
    int dims = 50;
    double variability = 2.0;
    // scale=0, zeroes=0, duplicates=0
    float[][] vecs = TestDataFiles.genVectors(count, dims, 123L, variability, 0.0, 0.0, 0.0);
    // flatten all values
    int total = count * dims;
    double sum = 0.0;
    double sum2 = 0.0;
    for (float[] row : vecs) {
      for (float v : row) {
        sum += v;
        sum2 += v * v;
      }
    }
    double mean = sum / total;
    double var = sum2 / total - mean * mean;
    // Expect mean ~ 0, var ~ variability^2
    assertThat(mean).isCloseTo(0.0, offset(0.1));
    assertThat(var).isCloseTo(variability * variability, offset(0.2 * variability * variability));
  }

  @Test
  void testGenVectorsScaleEffect() {
    int count = 1000;
    int dims = 100;
    double scale = 0.5;
    // variability=0, zeroes=0, duplicates=0
    float[][] vecs = TestDataFiles.genVectors(count, dims, 321L, 0.0, scale, 0.0, 0.0);
    // check row 0 is constant zero
    double sum0 = 0.0;
    for (float v : vecs[0]) sum0 += v;
    assertThat(sum0).isCloseTo(0.0, offset(1e-6));
    // check last row variance ~ (scale*(count-1))^2
    float[] last = vecs[count - 1];
    double meanL = 0.0;
    for (float v : last) meanL += v;
    meanL /= dims;
    double sumsq = 0.0;
    for (float v : last) sumsq += (v - meanL) * (v - meanL);
    double varL = sumsq / dims;
    double expectedVar = (scale * (count - 1)) * (scale * (count - 1));
    assertThat(varL).isCloseTo(expectedVar, offset(0.2 * expectedVar));
  }

  @Test
  void testGenVectorsZeroesProportion() {
    int count = 500;
    int dims = 5;
    double zeroesProp = 0.2;
    // use non-zero variability to avoid all rows being zero
    float[][] vecs = TestDataFiles.genVectors(count, dims, 111L, 1.0, 0.0, zeroesProp, 0.0);
    int zeros = 0;
    for (float[] row : vecs) {
      boolean allZero = true;
      for (float v : row) {
        if (v != 0.0f) { allZero = false; break; }
      }
      if (allZero) zeros++;
    }
    int expectedZeros = (int) Math.round(zeroesProp * count);
    assertThat(zeros).isEqualTo(expectedZeros);
  }

  @Test
  void testGenVectorsDuplicatesProportion() {
    int count = 1000;
    int dims = 1;
    double dupProp = 0.15;
    // variability=0, scale=1.0, zeroes=0
    float[][] vecs = TestDataFiles.genVectors(count, dims, 222L, 0.0, 1.0, 0.0, dupProp);
    // count unique values
    java.util.Set<Float> uniq = new java.util.HashSet<>();
    for (float[] row : vecs) {
      uniq.add(row[0]);
    }
    int duplicates = count - uniq.size();
    assertThat(duplicates / (double) count)
        .isCloseTo(dupProp, offset(0.05));
  }

  @Test
  void testGenVectorsCombinationAllParams1() {
    int count = 800;
    int dims = 10;
    double variability = 1.0;
    double scale = 0.3;
    double zeroesProp = 0.1;
    double dupProp = 0.1;
    float[][] vecs = TestDataFiles.genVectors(count, dims, 333L,
        variability, scale, zeroesProp, dupProp);
    // check zero proportion
    int zeros = 0;
    java.util.Map<String,Integer> freq = new java.util.HashMap<>();
    for (float[] row : vecs) {
      boolean allZero = true;
      for (float v : row) { if (v != 0.0f) { allZero = false; break; } }
      if (allZero) {
        zeros++;
      }
      String key = java.util.Arrays.toString(row);
      freq.put(key, freq.getOrDefault(key, 0) + 1);
    }
    // verify some zero vectors were introduced
    assertThat(zeros).isGreaterThan(0);
    // check duplicate proportion
    int dupCount = 0;
    for (int c : freq.values()) { if (c > 1) dupCount += c - 1; }
    // verify some duplicate vectors were introduced
    assertThat(dupCount).isGreaterThan(0);
    // basic sanity: values should exist
  }

  @Test
  void testGenVectorsCombinationAllParams2() {
    int count = 600;
    int dims = 20;
    double variability = 0.5;
    double scale = 0.7;
    double zeroesProp = 0.2;
    double dupProp = 0.05;
    float[][] vecs = TestDataFiles.genVectors(count, dims, 444L,
        variability, scale, zeroesProp, dupProp);
    // zero proportion
    int zeros = 0;
    java.util.Map<String,Integer> freq = new java.util.HashMap<>();
    for (float[] row : vecs) {
      boolean allZero = true;
      for (float v : row) { if (v != 0.0f) { allZero = false; break; } }
      if (allZero) zeros++;
      String key = java.util.Arrays.toString(row);
      freq.put(key, freq.getOrDefault(key, 0) + 1);
    }
    // verify some zero vectors were introduced
    assertThat(zeros).isGreaterThan(0);
    int dupCount = 0;
    for (int c : freq.values()) { if (c > 1) dupCount += c - 1; }
    // verify some duplicate vectors were introduced
    assertThat(dupCount).isGreaterThan(0);
    // basic sanity: values should exist beyond zero and duplicate rows
  }
}
