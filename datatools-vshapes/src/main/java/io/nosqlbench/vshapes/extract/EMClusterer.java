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

package io.nosqlbench.vshapes.extract;

import java.util.Arrays;

/// Expectation-Maximization (EM) algorithm for Gaussian Mixture Model clustering.
///
/// ## Purpose
///
/// This class implements soft clustering for multi-modal distributions using EM.
/// Unlike hard clustering (nearest-mode assignment), EM computes probabilistic
/// responsibilities for each data point, allowing overlapping modes to be
/// properly modeled.
///
/// ## Algorithm
///
/// The EM algorithm iterates between two steps:
///
/// 1. **E-step (Expectation)**: Compute responsibilities - the probability that
///    each data point belongs to each component:
///    ```
///    r_{ik} = (w_k * N(x_i | μ_k, σ_k)) / Σ_j (w_j * N(x_i | μ_j, σ_j))
///    ```
///
/// 2. **M-step (Maximization)**: Update component parameters using weighted data:
///    ```
///    N_k = Σ_i r_{ik}
///    μ_k = (1/N_k) * Σ_i r_{ik} * x_i
///    σ_k² = (1/N_k) * Σ_i r_{ik} * (x_i - μ_k)²
///    w_k = N_k / N
///    ```
///
/// ## Convergence
///
/// The algorithm converges when the change in log-likelihood is less than a
/// threshold (default 1e-6) or after a maximum number of iterations (default 50).
///
/// ## Usage
///
/// ```java
/// // Initialize with data and initial mode locations
/// EMClusterer clusterer = new EMClusterer(data, initialPeaks);
///
/// // Run EM until convergence
/// EMResult result = clusterer.fit();
///
/// // Get soft assignments (responsibilities)
/// double[][] responsibilities = result.responsibilities();
///
/// // Get fitted parameters
/// double[] means = result.means();
/// double[] stdDevs = result.stdDevs();
/// double[] weights = result.weights();
/// ```
///
/// ## Thread Safety
///
/// This class is NOT thread-safe. Use separate instances for concurrent operations.
///
/// @see CompositeModelFitter
public final class EMClusterer {

    /// Default maximum iterations for EM convergence
    public static final int DEFAULT_MAX_ITERATIONS = 50;

    /// Default convergence threshold (change in log-likelihood)
    public static final double DEFAULT_CONVERGENCE_THRESHOLD = 1e-6;

    /// Minimum variance to prevent numerical instability
    private static final double MIN_VARIANCE = 1e-10;

    /// Small constant to avoid log(0)
    private static final double LOG_EPSILON = 1e-300;

    private final float[] data;
    private final int numComponents;
    private final int maxIterations;
    private final double convergenceThreshold;

    // Current parameter estimates
    private double[] means;
    private double[] stdDevs;
    private double[] weights;

    // Responsibilities: r[i][k] = P(z_i = k | x_i)
    private double[][] responsibilities;

    // Tracking
    private double lastLogLikelihood = Double.NEGATIVE_INFINITY;
    private int iterationsRun = 0;
    private boolean converged = false;

    /// Creates an EM clusterer with default parameters.
    ///
    /// @param data the data to cluster
    /// @param initialPeaks initial peak locations (determines number of components)
    public EMClusterer(float[] data, double[] initialPeaks) {
        this(data, initialPeaks, DEFAULT_MAX_ITERATIONS, DEFAULT_CONVERGENCE_THRESHOLD);
    }

    /// Creates an EM clusterer with custom parameters.
    ///
    /// @param data the data to cluster
    /// @param initialPeaks initial peak locations (determines number of components)
    /// @param maxIterations maximum EM iterations
    /// @param convergenceThreshold log-likelihood change threshold for convergence
    public EMClusterer(float[] data, double[] initialPeaks, int maxIterations, double convergenceThreshold) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("data cannot be null or empty");
        }
        if (initialPeaks == null || initialPeaks.length < 2) {
            throw new IllegalArgumentException("initialPeaks must have at least 2 components");
        }

        this.data = data;
        this.numComponents = initialPeaks.length;
        this.maxIterations = maxIterations;
        this.convergenceThreshold = convergenceThreshold;

        // Initialize parameters from initial peaks
        initializeParameters(initialPeaks);
    }

    /// Initializes parameters from initial peak locations.
    private void initializeParameters(double[] initialPeaks) {
        this.means = Arrays.copyOf(initialPeaks, numComponents);
        this.weights = new double[numComponents];
        this.stdDevs = new double[numComponents];
        this.responsibilities = new double[data.length][numComponents];

        // Initialize weights equally
        Arrays.fill(weights, 1.0 / numComponents);

        // Estimate initial standard deviations from data spread
        double dataMin = data[0], dataMax = data[0];
        for (float v : data) {
            if (v < dataMin) dataMin = v;
            if (v > dataMax) dataMax = v;
        }
        double spread = (dataMax - dataMin) / (2.0 * numComponents);
        Arrays.fill(stdDevs, Math.max(spread, Math.sqrt(MIN_VARIANCE)));
    }

    /// Runs the EM algorithm until convergence.
    ///
    /// @return the EM result with fitted parameters and responsibilities
    public EMResult fit() {
        for (iterationsRun = 0; iterationsRun < maxIterations; iterationsRun++) {
            // E-step: compute responsibilities
            double logLikelihood = estep();

            // Check convergence
            if (iterationsRun > 0 && Math.abs(logLikelihood - lastLogLikelihood) < convergenceThreshold) {
                converged = true;
                break;
            }
            lastLogLikelihood = logLikelihood;

            // M-step: update parameters
            mstep();
        }

        return new EMResult(
            Arrays.copyOf(means, numComponents),
            Arrays.copyOf(stdDevs, numComponents),
            Arrays.copyOf(weights, numComponents),
            copyResponsibilities(),
            lastLogLikelihood,
            iterationsRun,
            converged
        );
    }

    /// E-step: Compute responsibilities and return log-likelihood.
    private double estep() {
        double totalLogLikelihood = 0;

        for (int i = 0; i < data.length; i++) {
            double x = data[i];
            double[] densities = new double[numComponents];
            double sumDensity = 0;

            // Compute weighted density for each component
            for (int k = 0; k < numComponents; k++) {
                densities[k] = weights[k] * gaussianDensity(x, means[k], stdDevs[k]);
                sumDensity += densities[k];
            }

            // Avoid division by zero
            if (sumDensity < LOG_EPSILON) {
                sumDensity = LOG_EPSILON;
            }

            // Compute responsibilities
            for (int k = 0; k < numComponents; k++) {
                responsibilities[i][k] = densities[k] / sumDensity;
            }

            // Add to log-likelihood
            totalLogLikelihood += Math.log(Math.max(sumDensity, LOG_EPSILON));
        }

        return totalLogLikelihood;
    }

    /// M-step: Update parameters using responsibilities.
    private void mstep() {
        // Compute effective counts per component
        double[] Nk = new double[numComponents];
        for (int k = 0; k < numComponents; k++) {
            for (int i = 0; i < data.length; i++) {
                Nk[k] += responsibilities[i][k];
            }
        }

        // Update means
        for (int k = 0; k < numComponents; k++) {
            if (Nk[k] > 0) {
                double sum = 0;
                for (int i = 0; i < data.length; i++) {
                    sum += responsibilities[i][k] * data[i];
                }
                means[k] = sum / Nk[k];
            }
        }

        // Update standard deviations
        for (int k = 0; k < numComponents; k++) {
            if (Nk[k] > 0) {
                double sumSq = 0;
                for (int i = 0; i < data.length; i++) {
                    double diff = data[i] - means[k];
                    sumSq += responsibilities[i][k] * diff * diff;
                }
                double variance = sumSq / Nk[k];
                stdDevs[k] = Math.sqrt(Math.max(variance, MIN_VARIANCE));
            }
        }

        // Update weights
        for (int k = 0; k < numComponents; k++) {
            weights[k] = Nk[k] / data.length;
        }

        // Normalize weights to ensure they sum to 1
        double weightSum = 0;
        for (double w : weights) weightSum += w;
        if (weightSum > 0) {
            for (int k = 0; k < numComponents; k++) {
                weights[k] /= weightSum;
            }
        }
    }

    /// Computes Gaussian probability density.
    private double gaussianDensity(double x, double mean, double stdDev) {
        if (stdDev <= 0) stdDev = Math.sqrt(MIN_VARIANCE);
        double z = (x - mean) / stdDev;
        return Math.exp(-0.5 * z * z) / (stdDev * Math.sqrt(2 * Math.PI));
    }

    /// Creates a deep copy of responsibilities matrix.
    private double[][] copyResponsibilities() {
        double[][] copy = new double[data.length][numComponents];
        for (int i = 0; i < data.length; i++) {
            copy[i] = Arrays.copyOf(responsibilities[i], numComponents);
        }
        return copy;
    }

    /// Segments data based on maximum responsibility (hard assignment from soft clustering).
    ///
    /// This is useful for comparing EM results with hard clustering or for
    /// extracting per-component data arrays.
    ///
    /// @param responsibilities the responsibility matrix from EM
    /// @param data the original data
    /// @return array of data arrays, one per component
    public static float[][] segmentByMaxResponsibility(double[][] responsibilities, float[] data) {
        int n = data.length;
        int k = responsibilities[0].length;

        // Count points per component
        int[] counts = new int[k];
        for (int i = 0; i < n; i++) {
            int maxK = argmax(responsibilities[i]);
            counts[maxK]++;
        }

        // Allocate arrays
        float[][] result = new float[k][];
        int[] indices = new int[k];
        for (int c = 0; c < k; c++) {
            result[c] = new float[counts[c]];
        }

        // Assign points
        for (int i = 0; i < n; i++) {
            int maxK = argmax(responsibilities[i]);
            result[maxK][indices[maxK]++] = data[i];
        }

        return result;
    }

    /// Finds the index of the maximum value in an array.
    private static int argmax(double[] arr) {
        int maxIdx = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[maxIdx]) {
                maxIdx = i;
            }
        }
        return maxIdx;
    }

    /// Result of EM clustering.
    ///
    /// @param means fitted component means
    /// @param stdDevs fitted component standard deviations
    /// @param weights fitted component weights (sum to 1)
    /// @param responsibilities soft assignment matrix (n_samples × n_components)
    /// @param logLikelihood final log-likelihood
    /// @param iterations number of iterations run
    /// @param converged whether EM converged before max iterations
    public record EMResult(
        double[] means,
        double[] stdDevs,
        double[] weights,
        double[][] responsibilities,
        double logLikelihood,
        int iterations,
        boolean converged
    ) {
        /// Returns the number of components.
        public int numComponents() {
            return means.length;
        }

        /// Returns hard assignments by maximum responsibility.
        public int[] hardAssignments() {
            int[] assignments = new int[responsibilities.length];
            for (int i = 0; i < responsibilities.length; i++) {
                assignments[i] = argmax(responsibilities[i]);
            }
            return assignments;
        }

        /// Returns the effective sample size for a component.
        public double effectiveSampleSize(int component) {
            double sum = 0;
            for (double[] r : responsibilities) {
                sum += r[component];
            }
            return sum;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("EMResult[k=%d, LL=%.2f, iters=%d, converged=%s]%n",
                means.length, logLikelihood, iterations, converged));
            for (int k = 0; k < means.length; k++) {
                sb.append(String.format("  Component %d: mean=%.4f, std=%.4f, weight=%.2f%%%n",
                    k, means[k], stdDevs[k], weights[k] * 100));
            }
            return sb.toString();
        }
    }
}
