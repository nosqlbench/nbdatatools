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


// import java.nio.file.Path; // unused
import java.nio.file.Path;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/// This is a set of basic data generator utilities which is used
/// to build data which is conditioned in specific ways. The methods
/// here are not meant for larger-than-memory testing.
public class TestDataFiles {

  /// Generate a sequence of vectors at the given dimensionality,
  /// with the given statistical shaping. By default, the values of a given vector should average
  ///  to zero, with the range of values varying between -0.0 and 1.0.
  /// @param dimensions Each vector has this many dimensional components
  /// @param count Create this many vectors
  /// @param seed Seed for the random generator (for reproducibility)
  /// @param variability Baseline standard deviation for Gaussian noise
  /// @param scale vary the values by the magnitude of the ordinal position of each vector. 0.0
  /// means do not vary, 1.0 means vary by up to the magnitude of the ordinal.
  /// @param zeroesProportion There should be this many zero vectors in the dataset by choosing
  ///   random vectors to set all components to zero. Zeroes should not overlap with duplicates.
  /// @param duplicatesProportion There should be this many duplicate vectors in the dataset by
  ///   choosing other random source vectors. Duplicates should not overlap with zero vectors.
  /// @return an array of float vectors
  public static float[][] genVectors(int count, int dimensions, long seed, double variability,
      double scale, double zeroesProportion, double duplicatesProportion) {
    // Initialize random with provided seed
    Random rand = new Random(seed);
    // Base generation: each vector has Gaussian noise with baseline variability and ordinal scale
    float[][] vectors = new float[count][dimensions];
    for (int i = 0; i < count; i++) {
      double ordFactor = scale * i;
      for (int j = 0; j < dimensions; j++) {
        double baseNoise = rand.nextGaussian() * variability;
        double ordNoise = rand.nextGaussian() * ordFactor;
        vectors[i][j] = (float) (baseNoise + ordNoise);
      }
    }
    // Determine counts for override operations
    int zeroCount = (int) Math.round(zeroesProportion * count);
    int dupCount = (int) Math.round(duplicatesProportion * count);
    // Compute total override indices
    int overrideCount = Math.min(count, zeroCount + dupCount);
    // Shuffle indices for random selection
    List<Integer> idx = new ArrayList<>(count);
    for (int i = 0; i < count; i++) idx.add(i);
    Collections.shuffle(idx, rand);
    // Partition override indices: first zeros, then duplicate destinations
    List<Integer> overrideIdx = idx.subList(0, overrideCount);
    List<Integer> zeroIdx = overrideIdx.subList(0, Math.min(zeroCount, overrideCount));
    List<Integer> dupIdx = overrideIdx.subList(zeroIdx.size(), overrideIdx.size());
    // Remaining indices serve as source pool for duplicates
    List<Integer> poolIdx = idx.subList(overrideCount, count);
    // Apply zero vectors
    for (int zi : zeroIdx) {
      vectors[zi] = new float[dimensions];
    }
    // Apply duplicates from pool
    if (!poolIdx.isEmpty()) {
      for (int di : dupIdx) {
        int src = poolIdx.get(rand.nextInt(poolIdx.size()));
        float[] copy = new float[dimensions];
        System.arraycopy(vectors[src], 0, copy, 0, dimensions);
        vectors[di] = copy;
      }
    }
    return vectors;
  }

  public static enum Format {
    ivec,
    fvec,
    bvec,
    json,
    yaml
  }
  /// Write the given vectors to the specified path in one of the supported formats.
  /// @param vectors matrix of float vectors to save
  /// @param path output file path
  /// @param format output format (ivec: int32 vectors, fvec: float vectors,
  ///               bvec: byte vectors, json: JSON array, yaml: YAML sequence)
  /// @return the path to which the data was written
  public static Path saveToFile(float[][] vectors, Path path, Format format) {
    try {
      switch (format) {
        case fvec:
          try (java.io.DataOutputStream dos = new java.io.DataOutputStream(
              java.nio.file.Files.newOutputStream(path))) {
            for (float[] vec : vectors) {
              dos.writeInt(vec.length);
              for (float v : vec) {
                dos.writeFloat(v);
              }
            }
          }
          break;
        case ivec:
          try (java.io.DataOutputStream dos = new java.io.DataOutputStream(
              java.nio.file.Files.newOutputStream(path))) {
            for (float[] vec : vectors) {
              dos.writeInt(vec.length);
              for (float v : vec) {
                dos.writeInt(Math.round(v));
              }
            }
          }
          break;
        case bvec:
          try (java.io.DataOutputStream dos = new java.io.DataOutputStream(
              java.nio.file.Files.newOutputStream(path))) {
            for (float[] vec : vectors) {
              dos.writeInt(vec.length);
              for (float v : vec) {
                dos.writeByte((byte) Math.round(v));
              }
            }
          }
          break;
        case json:
          try (java.io.Writer writer = java.nio.file.Files.newBufferedWriter(path)) {
            new com.google.gson.Gson().toJson(vectors, writer);
          }
          break;
        case yaml:
          try (java.io.Writer writer = java.nio.file.Files.newBufferedWriter(path)) {
            // simple YAML: sequence of sequences
            writer.write("---\n");
            for (float[] vec : vectors) {
              writer.write("- [");
              for (int i = 0; i < vec.length; i++) {
                writer.write(Float.toString(vec[i]));
                if (i < vec.length - 1) writer.write(", ");
              }
              writer.write("]\n");
            }
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + format);
      }
      return path;
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to save vectors to file: " + path, e);
    }
  }
}
