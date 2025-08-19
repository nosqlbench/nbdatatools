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

package io.nosqlbench.vshapes;

import java.util.Arrays;
import java.util.Comparator;

/// # VectorUtils
///
/// Low-level utility methods for vector space computations and k-NN operations.
///
/// ## Purpose
/// Provides optimized implementations of common vector operations:
/// - **Distance Calculations**: Euclidean distance variants
/// - **Neighbor Search**: k-nearest neighbor finding
/// - **Statistics**: Basic statistical measures for vectors
///
/// ## Features
/// - **Performance Optimized**: Avoids unnecessary sqrt operations where possible
/// - **Memory Efficient**: In-place operations and minimal allocations
/// - **Robust**: Input validation and error handling
///
/// ## Usage
/// ```java
/// // Distance calculation
/// double dist = VectorUtils.euclideanDistance(vector1, vector2);
///
/// // k-NN search
/// int[] nearest = VectorUtils.findKNearestNeighbors(query, vectors, k);
///
/// // Statistics
/// VectorUtils.Statistics stats = VectorUtils.computeStatistics(values);
/// ```
public final class VectorUtils {

    private VectorUtils() {} // Utility class

    /// Computes Euclidean distance between two vectors.
    /// Standard L2 distance: sqrt(sum((a_i - b_i)^2))
    /// 
    /// @param a first vector
    /// @param b second vector
    /// @return Euclidean distance
    /// @throws IllegalArgumentException if vectors have different dimensions
    public static double euclideanDistance(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must have same dimension");
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /// Computes squared Euclidean distance between two vectors.
    /// Avoids expensive sqrt operation for distance comparisons.
    /// 
    /// @param a first vector
    /// @param b second vector
    /// @return squared Euclidean distance
    /// @throws IllegalArgumentException if vectors have different dimensions
    public static double squaredEuclideanDistance(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must have same dimension");
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    /// Finds k nearest neighbors for a query vector within a vector space.
    /// Uses exhaustive search with Euclidean distance.
    /// 
    /// @param queryVector the query vector to find neighbors for
    /// @param vectorSpace the vector space to search within
    /// @param k number of nearest neighbors to return
    /// @param excludeIndex index to exclude from search (-1 for no exclusion)
    /// @return array of neighbor indices sorted by distance (closest first)
    public static int[] findKNearestNeighbors(float[] queryVector, VectorSpace vectorSpace, int k, int excludeIndex) {
        int n = vectorSpace.getVectorCount();
        
        // Create distance-index pairs
        DistanceIndexPair[] pairs = new DistanceIndexPair[excludeIndex >= 0 ? n - 1 : n];
        int pairIndex = 0;
        
        for (int i = 0; i < n; i++) {
            if (i == excludeIndex) continue;
            
            float[] vector = vectorSpace.getVector(i);
            double distance = euclideanDistance(queryVector, vector);
            pairs[pairIndex++] = new DistanceIndexPair(distance, i);
        }
        
        // Sort by distance and take first k
        Arrays.sort(pairs, Comparator.comparingDouble(pair -> pair.distance));
        
        int resultSize = Math.min(k, pairs.length);
        int[] result = new int[resultSize];
        for (int i = 0; i < resultSize; i++) {
            result[i] = pairs[i].index;
        }
        
        return result;
    }

    /// Computes k-nearest neighbors for all vectors in a vector space.
    /// This is a batch operation that finds neighbors for every vector.
    /// 
    /// @param vectorSpace the vector space to process
    /// @param k number of nearest neighbors per vector
    /// @return 2D array where each row contains k nearest neighbor indices for that vector
    public static int[][] computeAllKNN(VectorSpace vectorSpace, int k) {
        int n = vectorSpace.getVectorCount();
        int[][] knn = new int[n][];
        
        for (int i = 0; i < n; i++) {
            float[] queryVector = vectorSpace.getVector(i);
            knn[i] = findKNearestNeighbors(queryVector, vectorSpace, k, i); // exclude self
        }
        
        return knn;
    }

    /// Computes basic statistics (mean, std dev, min, max) for an array of values.
    /// 
    /// @param values the values to analyze
    /// @return statistics object with computed measures
    public static Statistics computeStatistics(double[] values) {
        if (values.length == 0) {
            return new Statistics(0, 0, 0, 0);
        }
        
        double sum = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        
        for (double value : values) {
            sum += value;
            if (value < min) min = value;
            if (value > max) max = value;
        }
        
        double mean = sum / values.length;
        
        double sumSquaredDeviations = 0;
        for (double value : values) {
            double deviation = value - mean;
            sumSquaredDeviations += deviation * deviation;
        }
        
        double stdDev = Math.sqrt(sumSquaredDeviations / values.length);
        
        return new Statistics(mean, stdDev, min, max);
    }

    /// Helper class for sorting vectors by distance during k-NN search.
    private static class DistanceIndexPair {
        final double distance;
        final int index;

        DistanceIndexPair(double distance, int index) {
            this.distance = distance;
            this.index = index;
        }
    }

    /// ## Statistics
    ///
    /// Container for basic statistical measures of a numeric array.
    /// 
    /// ### Fields
    /// - **mean**: Arithmetic mean of the values
    /// - **stdDev**: Population standard deviation  
    /// - **min**: Minimum value
    /// - **max**: Maximum value
    public static class Statistics {
        /// Arithmetic mean of the values
        public final double mean;
        /// Population standard deviation
        public final double stdDev;
        /// Minimum value in the dataset
        public final double min;
        /// Maximum value in the dataset
        public final double max;

        /// Creates a new statistics summary.
        /// 
        /// @param mean arithmetic mean
        /// @param stdDev standard deviation
        /// @param min minimum value
        /// @param max maximum value
        public Statistics(double mean, double stdDev, double min, double max) {
            this.mean = mean;
            this.stdDev = stdDev;
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return String.format("Statistics{mean=%.4f, stdDev=%.4f, min=%.4f, max=%.4f}", 
                               mean, stdDev, min, max);
        }
    }
}