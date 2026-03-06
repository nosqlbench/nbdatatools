package io.nosqlbench.vectordata.spec.datasets.types;

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

import jdk.incubator.vector.*;

/// Primitive distance function for float arrays - eliminates enum switching overhead
@FunctionalInterface
interface FloatDistanceFunc {
  /// Compute distance between two float vectors (primitive types only)
  /// @param v1 first vector
  /// @param v2 second vector
  /// @return distance (primitive double)
  double distance(float[] v1, float[] v2);
}

/// Primitive distance function for double arrays - eliminates enum switching overhead
@FunctionalInterface
interface DoubleDistanceFunc {
  /// Compute distance between two double vectors (primitive types only)
  /// @param v1 first vector
  /// @param v2 second vector
  /// @return distance (primitive double)
  double distance(double[] v1, double[] v2);
}

/// Primitive distance function for half-precision (f16) arrays stored as short[].
/// Each short contains a raw IEEE 754 binary16 bit pattern.
@FunctionalInterface
interface HalfDistanceFunc {
  /// Compute distance between two half-precision vectors (binary16 bit patterns)
  /// @param v1 first vector (binary16 shorts)
  /// @param v2 second vector (binary16 shorts)
  /// @return distance (primitive double)
  double distance(short[] v1, short[] v2);
}

/// The distance function to use for computing distances between vectors
public enum DistanceFunction {

  /// The cosine distance function
  COSINE,
  /// The dot product distance function
  DOT_PRODUCT,
  /// The euclidean distance function
  EUCLIDEAN,
  /// Same as euclidean here
  L2,
  /// The Manhattan (L1) distance function
  L1;

  /**
   * Get a primitive distance function for float arrays.
   * Returns a cached function reference - NO enum switching overhead per call.
   * Use this for hot loops where distance is computed millions of times.
   *
   * @return primitive distance function (float[], float[]) -> double
   */
  public FloatDistanceFunc asFloatFunction() {
    return switch (this) {
      case COSINE -> this::floatCosineDistance;
      case EUCLIDEAN, L2 -> this::floatEuclideanDistance;
      case L1 -> this::floatManhattanDistance;
      case DOT_PRODUCT -> this::floatDotProduct;
    };
  }

  /**
   * Get a primitive distance function for double arrays.
   * Returns a cached function reference - NO enum switching overhead per call.
   * Use this for hot loops where distance is computed millions of times.
   *
   * @return primitive distance function (double[], double[]) -> double
   */
  public DoubleDistanceFunc asDoubleFunction() {
    return switch (this) {
      case COSINE -> this::doubleCosineDistance;
      case EUCLIDEAN, L2 -> this::doubleEuclideanDistance;
      case L1 -> this::doubleManhattanDistance;
      case DOT_PRODUCT -> this::doubleDotProduct;
    };
  }

  /**
   * Get a primitive distance function for half-precision (f16) arrays.
   * Returns a cached function reference - NO enum switching overhead per call.
   * Use this for hot loops where distance is computed millions of times.
   *
   * <p>Input arrays contain raw IEEE 754 binary16 bit patterns as shorts.
   * Elements are converted to float32 in SIMD-width batches and accumulated
   * using the Panama Vector API.
   *
   * @return primitive distance function (short[], short[]) -> double
   */
  public HalfDistanceFunc asHalfFunction() {
    return switch (this) {
      case COSINE -> this::halfCosineDistance;
      case EUCLIDEAN, L2 -> this::halfEuclideanDistance;
      case L1 -> this::halfManhattanDistance;
      case DOT_PRODUCT -> this::halfDotProduct;
    };
  }

  /// Compute the distance between two double vectors.
  /// @param v1 the first vector
  /// @param v2 the second vector
  /// @return the distance between the two vectors
  public double distance(double[] v1, double[] v2) {
    switch (this) {
      case COSINE:
        return doubleCosineDistance(v1, v2);
      case EUCLIDEAN:
      case L2:
        return doubleEuclideanDistance(v1, v2);
      case L1:
        return doubleManhattanDistance(v1, v2);
      case DOT_PRODUCT:
        return doubleDotProduct(v1, v2);
      default:
        throw new IllegalArgumentException("Unknown distance function: " + this);
    }
  }

  /// Compute the distance between two float vectors.
  /// @param v1 the first vector
  /// @param v2 the second vector
  /// @return the distance between the two vectors
  public double distance(float[] v1, float[] v2) {
    switch (this) {
      case COSINE:
        return floatCosineDistance(v1, v2);
      case EUCLIDEAN:
      case L2:
        return floatEuclideanDistance(v1, v2);
      case L1:
        return floatManhattanDistance(v1, v2);
      case DOT_PRODUCT:
        return floatDotProduct(v1, v2);
      default:
        throw new IllegalArgumentException("Unknown distance function: " + this);
    }
  }

  /// Compute the distance between two half-precision (f16) vectors.
  ///
  /// The input arrays contain raw IEEE 754 binary16 bit patterns as shorts.
  /// Elements are converted to float32 in SIMD-width batches and accumulated
  /// using the Panama Vector API for the distance computation.
  ///
  /// @param v1 the first vector (binary16 bit patterns)
  /// @param v2 the second vector (binary16 bit patterns)
  /// @return the distance between the two vectors
  public double distance(short[] v1, short[] v2) {
    switch (this) {
      case COSINE:
        return halfCosineDistance(v1, v2);
      case EUCLIDEAN:
      case L2:
        return halfEuclideanDistance(v1, v2);
      case L1:
        return halfManhattanDistance(v1, v2);
      case DOT_PRODUCT:
        return halfDotProduct(v1, v2);
      default:
        throw new IllegalArgumentException("Unknown distance function: " + this);
    }
  }

  // ==================== Half-precision (f16) SIMD distance methods ====================

  /// Convert a batch of f16 shorts to floats into a pre-allocated buffer.
  /// Uses {@link Float#float16ToFloat(short)} (available since Java 20).
  private static void f16ToF32Batch(short[] src, int srcOff, float[] dst, int count) {
    for (int j = 0; j < count; j++) {
      dst[j] = Float.float16ToFloat(src[srcOff + j]);
    }
  }

  private double halfDotProduct(short[] vectorA, short[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();
    var acc = FloatVector.zero(SPECIES);
    float[] bufA = new float[lanes];
    float[] bufB = new float[lanes];

    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    for (; i < upperBound; i += lanes) {
      f16ToF32Batch(vectorA, i, bufA, lanes);
      f16ToF32Batch(vectorB, i, bufB, lanes);
      var va = FloatVector.fromArray(SPECIES, bufA, 0);
      var vb = FloatVector.fromArray(SPECIES, bufB, 0);
      acc = va.fma(vb, acc);
    }

    double dot = acc.reduceLanes(VectorOperators.ADD);

    for (; i < vectorA.length; i++) {
      dot += (double) Float.float16ToFloat(vectorA[i]) * Float.float16ToFloat(vectorB[i]);
    }

    return -dot;
  }

  private double halfCosineDistance(short[] vectorA, short[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();
    var accDot = FloatVector.zero(SPECIES);
    var accNormA = FloatVector.zero(SPECIES);
    var accNormB = FloatVector.zero(SPECIES);
    float[] bufA = new float[lanes];
    float[] bufB = new float[lanes];

    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    for (; i < upperBound; i += lanes) {
      f16ToF32Batch(vectorA, i, bufA, lanes);
      f16ToF32Batch(vectorB, i, bufB, lanes);
      var va = FloatVector.fromArray(SPECIES, bufA, 0);
      var vb = FloatVector.fromArray(SPECIES, bufB, 0);
      accDot = va.fma(vb, accDot);
      accNormA = va.fma(va, accNormA);
      accNormB = vb.fma(vb, accNormB);
    }

    double dotProduct = accDot.reduceLanes(VectorOperators.ADD);
    double normA = accNormA.reduceLanes(VectorOperators.ADD);
    double normB = accNormB.reduceLanes(VectorOperators.ADD);

    for (; i < vectorA.length; i++) {
      float a = Float.float16ToFloat(vectorA[i]);
      float b = Float.float16ToFloat(vectorB[i]);
      dotProduct += a * b;
      normA += a * a;
      normB += b * b;
    }

    double magnitudeA = Math.sqrt(normA);
    double magnitudeB = Math.sqrt(normB);

    if (magnitudeA == 0 || magnitudeB == 0) {
      throw new IllegalArgumentException("One of the vectors has zero magnitude.");
    }

    return 1.0 - dotProduct / (magnitudeA * magnitudeB);
  }

  private double halfEuclideanDistance(short[] vectorA, short[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();
    var acc = FloatVector.zero(SPECIES);
    float[] bufA = new float[lanes];
    float[] bufB = new float[lanes];

    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    for (; i < upperBound; i += lanes) {
      f16ToF32Batch(vectorA, i, bufA, lanes);
      f16ToF32Batch(vectorB, i, bufB, lanes);
      var va = FloatVector.fromArray(SPECIES, bufA, 0);
      var vb = FloatVector.fromArray(SPECIES, bufB, 0);
      var diff = va.sub(vb);
      acc = diff.fma(diff, acc);
    }

    double sum = acc.reduceLanes(VectorOperators.ADD);

    for (; i < vectorA.length; i++) {
      double diff = Float.float16ToFloat(vectorA[i]) - Float.float16ToFloat(vectorB[i]);
      sum += diff * diff;
    }

    return Math.sqrt(sum);
  }

  private double halfManhattanDistance(short[] vectorA, short[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();
    var acc = FloatVector.zero(SPECIES);
    float[] bufA = new float[lanes];
    float[] bufB = new float[lanes];

    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    for (; i < upperBound; i += lanes) {
      f16ToF32Batch(vectorA, i, bufA, lanes);
      f16ToF32Batch(vectorB, i, bufB, lanes);
      var va = FloatVector.fromArray(SPECIES, bufA, 0);
      var vb = FloatVector.fromArray(SPECIES, bufB, 0);
      var diff = va.sub(vb);
      acc = acc.add(diff.abs());
    }

    double sum = acc.reduceLanes(VectorOperators.ADD);

    for (; i < vectorA.length; i++) {
      sum += Math.abs(Float.float16ToFloat(vectorA[i]) - Float.float16ToFloat(vectorB[i]));
    }

    return sum;
  }

  // ==================== Float distance methods ====================

  private double floatDotProduct(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();
    var acc = FloatVector.zero(SPECIES);
    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = FloatVector.fromArray(SPECIES, vectorA, offset);
        var vb = FloatVector.fromArray(SPECIES, vectorB, offset);
        acc = va.fma(vb, acc);
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = FloatVector.fromArray(SPECIES, vectorA, i);
      var vb = FloatVector.fromArray(SPECIES, vectorB, i);
      acc = va.fma(vb, acc);
    }

    // Single reduce at end
    double dot = acc.reduceLanes(VectorOperators.ADD);

    // Scalar tail
    for (; i < vectorA.length; i++) {
      dot += vectorA[i] * vectorB[i];
    }

    return -dot;
  }

  private double doubleDotProduct(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.doubleSpecies();
    int lanes = SPECIES.length();
    var acc = DoubleVector.zero(SPECIES);
    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = DoubleVector.fromArray(SPECIES, vectorA, offset);
        var vb = DoubleVector.fromArray(SPECIES, vectorB, offset);
        acc = va.fma(vb, acc);
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = DoubleVector.fromArray(SPECIES, vectorA, i);
      var vb = DoubleVector.fromArray(SPECIES, vectorB, i);
      acc = va.fma(vb, acc);
    }

    // Single reduce at end
    double dot = acc.reduceLanes(VectorOperators.ADD);

    // Scalar tail
    for (; i < vectorA.length; i++) {
      dot += vectorA[i] * vectorB[i];
    }

    return -dot;
  }

  private double floatCosineDistance(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();

    var accDot = FloatVector.zero(SPECIES);
    var accNormA = FloatVector.zero(SPECIES);
    var accNormB = FloatVector.zero(SPECIES);

    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop with vector accumulators
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = FloatVector.fromArray(SPECIES, vectorA, offset);
        var vb = FloatVector.fromArray(SPECIES, vectorB, offset);
        accDot = va.fma(vb, accDot);
        accNormA = va.fma(va, accNormA);
        accNormB = vb.fma(vb, accNormB);
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = FloatVector.fromArray(SPECIES, vectorA, i);
      var vb = FloatVector.fromArray(SPECIES, vectorB, i);
      accDot = va.fma(vb, accDot);
      accNormA = va.fma(va, accNormA);
      accNormB = vb.fma(vb, accNormB);
    }

    // Single reduce at end
    double dotProduct = accDot.reduceLanes(VectorOperators.ADD);
    double normA = accNormA.reduceLanes(VectorOperators.ADD);
    double normB = accNormB.reduceLanes(VectorOperators.ADD);

    // Scalar tail
    for (; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += vectorA[i] * vectorA[i];
      normB += vectorB[i] * vectorB[i];
    }

    double magnitudeA = Math.sqrt(normA);
    double magnitudeB = Math.sqrt(normB);

    if (magnitudeA == 0 || magnitudeB == 0) {
      throw new IllegalArgumentException("One of the vectors has zero magnitude.");
    }

    double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);
    return 1.0 - cosineSimilarity;
  }

  private double doubleCosineDistance(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.doubleSpecies();
    int lanes = SPECIES.length();

    var accDot = DoubleVector.zero(SPECIES);
    var accNormA = DoubleVector.zero(SPECIES);
    var accNormB = DoubleVector.zero(SPECIES);

    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop with vector accumulators
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = DoubleVector.fromArray(SPECIES, vectorA, offset);
        var vb = DoubleVector.fromArray(SPECIES, vectorB, offset);
        accDot = va.fma(vb, accDot);
        accNormA = va.fma(va, accNormA);
        accNormB = vb.fma(vb, accNormB);
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = DoubleVector.fromArray(SPECIES, vectorA, i);
      var vb = DoubleVector.fromArray(SPECIES, vectorB, i);
      accDot = va.fma(vb, accDot);
      accNormA = va.fma(va, accNormA);
      accNormB = vb.fma(vb, accNormB);
    }

    // Single reduce at end
    double dotProduct = accDot.reduceLanes(VectorOperators.ADD);
    double normA = accNormA.reduceLanes(VectorOperators.ADD);
    double normB = accNormB.reduceLanes(VectorOperators.ADD);

    // Scalar tail
    for (; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += vectorA[i] * vectorA[i];
      normB += vectorB[i] * vectorB[i];
    }

    double magnitudeA = Math.sqrt(normA);
    double magnitudeB = Math.sqrt(normB);

    if (magnitudeA == 0 || magnitudeB == 0) {
      throw new IllegalArgumentException("One of the vectors has zero magnitude.");
    }

    double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);
    return 1.0 - cosineSimilarity;
  }

  private double floatEuclideanDistance(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();

    var acc = FloatVector.zero(SPECIES);
    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop with vector accumulator
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = FloatVector.fromArray(SPECIES, vectorA, offset);
        var vb = FloatVector.fromArray(SPECIES, vectorB, offset);
        var diff = va.sub(vb);
        acc = diff.fma(diff, acc);
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = FloatVector.fromArray(SPECIES, vectorA, i);
      var vb = FloatVector.fromArray(SPECIES, vectorB, i);
      var diff = va.sub(vb);
      acc = diff.fma(diff, acc);
    }

    // Single reduce at end
    double sum = acc.reduceLanes(VectorOperators.ADD);

    // Scalar tail for remaining elements
    for (; i < vectorA.length; i++) {
      double diff = vectorA[i] - vectorB[i];
      sum += diff * diff;
    }

    return Math.sqrt(sum);
  }

  private double doubleEuclideanDistance(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.doubleSpecies();
    int lanes = SPECIES.length();

    var acc = DoubleVector.zero(SPECIES);
    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop with vector accumulator
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = DoubleVector.fromArray(SPECIES, vectorA, offset);
        var vb = DoubleVector.fromArray(SPECIES, vectorB, offset);
        var diff = va.sub(vb);
        acc = diff.fma(diff, acc);
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = DoubleVector.fromArray(SPECIES, vectorA, i);
      var vb = DoubleVector.fromArray(SPECIES, vectorB, i);
      var diff = va.sub(vb);
      acc = diff.fma(diff, acc);
    }

    // Single reduce at end
    double sum = acc.reduceLanes(VectorOperators.ADD);

    // Scalar tail for remaining elements
    for (; i < vectorA.length; i++) {
      double diff = vectorA[i] - vectorB[i];
      sum += diff * diff;
    }

    return Math.sqrt(sum);
  }

  private double floatManhattanDistance(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.floatSpecies();
    int lanes = SPECIES.length();

    var acc = FloatVector.zero(SPECIES);
    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop with vector accumulator
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = FloatVector.fromArray(SPECIES, vectorA, offset);
        var vb = FloatVector.fromArray(SPECIES, vectorB, offset);
        var diff = va.sub(vb);
        acc = acc.add(diff.abs());
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = FloatVector.fromArray(SPECIES, vectorA, i);
      var vb = FloatVector.fromArray(SPECIES, vectorB, i);
      var diff = va.sub(vb);
      acc = acc.add(diff.abs());
    }

    // Single reduce at end
    double sum = acc.reduceLanes(VectorOperators.ADD);

    // Scalar tail
    for (; i < vectorA.length; i++) {
      sum += Math.abs(vectorA[i] - vectorB[i]);
    }

    return sum;
  }

  private double doubleManhattanDistance(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    var SPECIES = LocalSpecies.doubleSpecies();
    int lanes = SPECIES.length();

    var acc = DoubleVector.zero(SPECIES);
    int i = 0;
    int upperBound = SPECIES.loopBound(vectorA.length);

    // 8-way unrolled loop with vector accumulator
    int unrollBound = upperBound - (7 * lanes);
    for (; i <= unrollBound; i += 8 * lanes) {
      for (int j = 0; j < 8; j++) {
        int offset = i + j * lanes;
        var va = DoubleVector.fromArray(SPECIES, vectorA, offset);
        var vb = DoubleVector.fromArray(SPECIES, vectorB, offset);
        var diff = va.sub(vb);
        acc = acc.add(diff.abs());
      }
    }

    // Handle remaining vectorized iterations
    for (; i < upperBound; i += lanes) {
      var va = DoubleVector.fromArray(SPECIES, vectorA, i);
      var vb = DoubleVector.fromArray(SPECIES, vectorB, i);
      var diff = va.sub(vb);
      acc = acc.add(diff.abs());
    }

    // Single reduce at end
    double sum = acc.reduceLanes(VectorOperators.ADD);

    // Scalar tail
    for (; i < vectorA.length; i++) {
      sum += Math.abs(vectorA[i] - vectorB[i]);
    }

    return sum;
  }
}
