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


  /// The distance function to use for computing distances between vectors
  public enum DistanceFunction {

    /// The cosine distance function
    COSINE,
    /// The dot product similarity as a distance (lower is better). Implemented as -dot(a,b).
    DOT_PRODUCT,
    /// The dot product distance function
    EUCLIDEAN,
    /// Same as euclidean here
    L2,
    /// The Manhattan (L1) distance function
    L1;

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
  /// Each element is converted to float inline and the distance is accumulated
  /// without allocating temporary float arrays.
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

  // ==================== IEEE 754 binary16 → float32 conversion ====================

  /// Convert an IEEE 754 binary16 (half-precision) bit pattern to a float.
  /// Pure-Java bit manipulation — works on Java 11+.
  private static float f16ToF32(short bits) {
    int h = bits & 0xFFFF;
    int sign = (h & 0x8000) << 16;
    int exp = (h >>> 10) & 0x1F;
    int mantissa = h & 0x03FF;

    if (exp == 0) {
      if (mantissa == 0) {
        return Float.intBitsToFloat(sign);
      }
      // Subnormal: normalize
      while ((mantissa & 0x0400) == 0) {
        mantissa <<= 1;
        exp--;
      }
      exp++;
      mantissa &= 0x03FF;
      return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mantissa << 13));
    }
    if (exp == 31) {
      // Inf or NaN
      return Float.intBitsToFloat(sign | 0x7F800000 | (mantissa << 13));
    }
    return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mantissa << 13));
  }

  // ==================== Half-precision (f16) distance methods ====================

  private double halfDotProduct(short[] a, short[] b) {
    if (a == null || b == null || a.length != b.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }
    double dot = 0.0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) f16ToF32(a[i]) * f16ToF32(b[i]);
    }
    return -dot;
  }

  private double halfCosineDistance(short[] vectorA, short[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;

    for (int i = 0; i < vectorA.length; i++) {
      float a = f16ToF32(vectorA[i]);
      float b = f16ToF32(vectorB[i]);
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

    double sum = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      double diff = f16ToF32(vectorA[i]) - f16ToF32(vectorB[i]);
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private double halfManhattanDistance(short[] vectorA, short[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double sum = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      sum += Math.abs(f16ToF32(vectorA[i]) - f16ToF32(vectorB[i]));
    }
    return sum;
  }

  // ==================== Float distance methods ====================

  private double floatDotProduct(float[] a, float[] b) {
    if (a == null || b == null || a.length != b.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }
    double dot = 0.0d;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
    }
    // distance-style: lower is better, so return negative similarity
    return -dot;
  }

  private double doubleDotProduct(double[] a, double[] b) {
    if (a == null || b == null || a.length != b.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }
    double dot = 0.0d;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
    }
    return -dot;
  }

  private double floatCosineDistance(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;

    for (int i = 0; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += vectorA[i] * vectorA[i];
      normB += vectorB[i] * vectorB[i];
    }

    // Calculate magnitudes
    double magnitudeA = Math.sqrt(normA);
    double magnitudeB = Math.sqrt(normB);

    if (magnitudeA == 0 || magnitudeB == 0) {
      throw new IllegalArgumentException("One of the vectors has zero magnitude.");
    }

    // Cosine similarity
    double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);
//    return cosineSimilarity;
//    // Cosine distance is 1 - cosine similarity
    return 1.0 - cosineSimilarity;

  }

  private double doubleCosineDistance(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;

    for (int i = 0; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += vectorA[i] * vectorA[i];
      normB += vectorB[i] * vectorB[i];
    }

    // Calculate magnitudes
    double magnitudeA = Math.sqrt(normA);
    double magnitudeB = Math.sqrt(normB);

    if (magnitudeA == 0 || magnitudeB == 0) {
      throw new IllegalArgumentException("One of the vectors has zero magnitude.");
    }

    // Cosine similarity
    double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);

    // Cosine distance is 1 - cosine similarity
    return 1.0 - cosineSimilarity;
  }

  private double floatEuclideanDistance(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double sum = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      double diff = vectorA[i] - vectorB[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private double doubleEuclideanDistance(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double sum = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      double diff = vectorA[i] - vectorB[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private double floatManhattanDistance(float[] vectorA, float[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double sum = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      sum += Math.abs(vectorA[i] - vectorB[i]);
    }
    return sum;
  }

  private double doubleManhattanDistance(double[] vectorA, double[] vectorB) {
    if (vectorA == null || vectorB == null || vectorA.length != vectorB.length) {
      throw new IllegalArgumentException("Vectors must be non-null and of the same dimension.");
    }

    double sum = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      sum += Math.abs(vectorA[i] - vectorB[i]);
    }
    return sum;
  }
}
