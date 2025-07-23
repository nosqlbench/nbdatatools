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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/// This is a set of basic data generator utilities which is used
/// to build data which is conditioned in specific ways. The methods
/// here are not meant for larger-than-memory testing.
public class TestDataFiles {

  /// Construct a TestDataFiles instance.
  ///
  /// Private constructor to prevent instantiation of this utility class.
  private TestDataFiles() {}

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

  /// Supported file formats for vector data.
  ///
  /// This enum defines the different file formats that can be used to store vector data.
  public static enum Format {
    /// Integer vector format (32-bit integers)
    ivec,
    /// Float vector format (32-bit floats)
    fvec,
    /// Byte vector format (8-bit integers)
    bvec,
    /// JSON array format
    json,
    /// YAML sequence format
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
      // Ensure parent directory exists
      if (path.getParent() != null) {
        Files.createDirectories(path.getParent());
      }
      
      switch (format) {
        case fvec -> saveFvecFile(vectors, path);
        case ivec -> throw new UnsupportedOperationException("ivec format not yet implemented");
        case bvec -> throw new UnsupportedOperationException("bvec format not yet implemented");
        case json -> throw new UnsupportedOperationException("json format not yet implemented");
        case yaml -> throw new UnsupportedOperationException("yaml format not yet implemented");
        default -> throw new IllegalArgumentException("Unsupported format: " + format);
      }
      
      return path;
    } catch (IOException e) {
      throw new RuntimeException("Failed to write vectors to file: " + path, e);
    }
  }
  
  /// Save vectors in fvec format (binary float vector format).
  /// Format: 4-byte dimension count (little-endian) + vector data (little-endian floats)
  /// @param vectors matrix of float vectors to save
  /// @param path output file path
  private static void saveFvecFile(float[][] vectors, Path path) throws IOException {
    if (vectors.length == 0) {
      throw new IllegalArgumentException("Cannot save empty vector array");
    }
    
    int dimensions = vectors[0].length;
    int vectorCount = vectors.length;
    
    // Validate all vectors have same dimensions
    for (int i = 0; i < vectorCount; i++) {
      if (vectors[i].length != dimensions) {
        throw new IllegalArgumentException("All vectors must have same dimensions. Vector " + i + 
                                         " has " + vectors[i].length + " dimensions, expected " + dimensions);
      }
    }
    
    // Calculate total file size: 4 bytes for dimensions + vector data
    long totalSize = 4L + (long) vectorCount * dimensions * Float.BYTES;
    
    try (FileChannel channel = FileChannel.open(path, 
         StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
      
      // Write dimensions (4 bytes, little-endian)
      ByteBuffer dimBuffer = ByteBuffer.allocate(4);
      dimBuffer.order(ByteOrder.LITTLE_ENDIAN);
      dimBuffer.putInt(dimensions);
      dimBuffer.flip();
      channel.write(dimBuffer);
      
      // Write vector data
      int bufferSize = Math.min(8192, dimensions * Float.BYTES); // Use reasonable buffer size
      ByteBuffer dataBuffer = ByteBuffer.allocate(bufferSize);
      dataBuffer.order(ByteOrder.LITTLE_ENDIAN);
      
      for (int i = 0; i < vectorCount; i++) {
        float[] vector = vectors[i];
        
        // Write vector components in chunks if needed
        int componentsWritten = 0;
        while (componentsWritten < dimensions) {
          dataBuffer.clear();
          
          // Fill buffer with as many components as fit
          int componentsToWrite = Math.min(
            (dataBuffer.capacity() / Float.BYTES), 
            dimensions - componentsWritten
          );
          
          for (int j = 0; j < componentsToWrite; j++) {
            dataBuffer.putFloat(vector[componentsWritten + j]);
          }
          
          dataBuffer.flip();
          channel.write(dataBuffer);
          componentsWritten += componentsToWrite;
        }
      }
      
      // Verify file size
      long actualSize = channel.size();
      if (actualSize != totalSize) {
        throw new IOException("File size mismatch. Expected " + totalSize + " bytes, wrote " + actualSize + " bytes");
      }
    }
  }

}
