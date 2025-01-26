package io.nosqlbench.nbvectors.options;

public enum DistanceFunction {
  COSINE;

  public double distance(double[] v1, double[] v2) {
    return switch (this) {
      case COSINE -> doubleCosineDistance(v1, v2);
    };
  }

  public double distance(float[] v1, float[] v2) {
    return switch (this) {
      case COSINE -> floatCosineDistance(v1, v2);
    };
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
