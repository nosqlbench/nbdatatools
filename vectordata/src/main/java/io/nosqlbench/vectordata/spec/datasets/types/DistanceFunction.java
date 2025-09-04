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
  /// The dot product distance function
  DOT_PRODUCT,
  /// The euclidean distance function
  EUCLIDEAN;

  /// compute the distance between two vectors
  /// @param v1 the first vector
  /// @param v2 the second vector
  /// @return the distance between the two vectors
  public double distance(double[] v1, double[] v2) {
    switch (this) {
      case COSINE:
        return doubleCosineDistance(v1, v2);
      case DOT_PRODUCT:
      case EUCLIDEAN:
        throw new RuntimeException("Not implemented");
      default:
        throw new IllegalArgumentException("Unknown distance function: " + this);
    }
  }

  /// compute the distance between two vectors
  /// @param v1 the first vector
  /// @param v2 the second vector
  /// @return the distance between the two vectors
  public double distance(float[] v1, float[] v2) {
    switch (this) {
      case COSINE:
        return floatCosineDistance(v1, v2);
      case DOT_PRODUCT:
      case EUCLIDEAN:
        throw new RuntimeException("Not implemented");
      default:
        throw new IllegalArgumentException("Unknown distance function: " + this);
    }
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
}
