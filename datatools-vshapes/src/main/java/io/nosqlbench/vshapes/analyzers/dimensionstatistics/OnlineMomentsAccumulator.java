package io.nosqlbench.vshapes.analyzers.dimensionstatistics;

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

import io.nosqlbench.vshapes.extract.DimensionStatistics;

import java.util.concurrent.locks.ReentrantLock;

/// Thread-safe online accumulator for computing statistical moments.
///
/// # Overview
///
/// This class accumulates statistics from a stream of floating-point values
/// using Welford's online algorithm. It computes mean, variance, skewness,
/// and kurtosis in a single pass with O(1) memory.
///
/// # Algorithm
///
/// Uses the extended Welford algorithm for numerically stable computation
/// of higher moments:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    WELFORD'S ALGORITHM                          │
/// └─────────────────────────────────────────────────────────────────┘
///
///   For each new value x:
///
///     n = n + 1
///     delta = x - mean
///     deltaN = delta / n
///     deltaN2 = deltaN * deltaN
///     term1 = delta * deltaN * (n - 1)
///
///     mean = mean + deltaN
///
///     // Update higher moments (order matters!)
///     M4 += term1 * deltaN² * (n² - 3n + 3) + 6*deltaN²*M2 - 4*deltaN*M3
///     M3 += term1 * deltaN * (n - 2) - 3*deltaN*M2
///     M2 += term1
///
///   After all values:
///
///     variance = M2 / n
///     skewness = (M3 / n) / stdDev³
///     kurtosis = (M4 / n) / variance²
/// ```
///
/// # Thread Safety
///
/// All operations are protected by a [ReentrantLock], allowing safe
/// concurrent updates from multiple harness worker threads.
///
/// # Memory Usage
///
/// Constant memory: approximately 80 bytes per accumulator regardless
/// of data size (8 doubles + lock overhead).
///
/// @see DimensionStatisticsAnalyzer
/// @see DimensionStatistics
final class OnlineMomentsAccumulator {

    /// Lock is transient - not serialized. Re-created on first access after deserialization.
    private transient ReentrantLock lock;

    private long count = 0;
    private double mean = 0;
    private double m2 = 0;   // Second central moment (for variance)
    private double m3 = 0;   // Third central moment (for skewness)
    private double m4 = 0;   // Fourth central moment (for kurtosis)
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    /// Creates a new moments accumulator.
    OnlineMomentsAccumulator() {
        this.lock = new ReentrantLock();
    }

    /// Returns the lock, lazily initializing if necessary (e.g., after deserialization).
    private ReentrantLock getLock() {
        if (lock == null) {
            synchronized (this) {
                if (lock == null) {
                    lock = new ReentrantLock();
                }
            }
        }
        return lock;
    }

    /// Accepts a new value into the accumulator.
    ///
    /// Updates all running statistics using Welford's algorithm.
    ///
    /// @param value the value to accumulate
    void accept(float value) {
        getLock().lock();
        try {
            count++;
            double delta = value - mean;
            double deltaN = delta / count;
            double deltaN2 = deltaN * deltaN;
            double term1 = delta * deltaN * (count - 1);

            mean += deltaN;

            // Update higher moments (order matters - M4 before M3 before M2)
            m4 += term1 * deltaN2 * (count * count - 3 * count + 3) +
                  6 * deltaN2 * m2 - 4 * deltaN * m3;
            m3 += term1 * deltaN * (count - 2) - 3 * deltaN * m2;
            m2 += term1;

            if (value < min) min = value;
            if (value > max) max = value;
        } finally {
            getLock().unlock();
        }
    }

    /// Accepts multiple values in a batch.
    ///
    /// More efficient than calling accept() repeatedly when processing chunks.
    ///
    /// @param values the values to accumulate
    void acceptAll(float[] values) {
        getLock().lock();
        try {
            for (float value : values) {
                count++;
                double delta = value - mean;
                double deltaN = delta / count;
                double deltaN2 = deltaN * deltaN;
                double term1 = delta * deltaN * (count - 1);

                mean += deltaN;

                m4 += term1 * deltaN2 * (count * count - 3 * count + 3) +
                      6 * deltaN2 * m2 - 4 * deltaN * m3;
                m3 += term1 * deltaN * (count - 2) - 3 * deltaN * m2;
                m2 += term1;

                if (value < min) min = value;
                if (value > max) max = value;
            }
        } finally {
            getLock().unlock();
        }
    }

    /// Converts accumulated statistics to a [DimensionStatistics] record.
    ///
    /// @param dimension the dimension index for the statistics
    /// @return computed statistics for the dimension
    DimensionStatistics toStatistics(int dimension) {
        getLock().lock();
        try {
            if (count == 0) {
                return new DimensionStatistics(dimension, 0, 0, 0, 0, 0, 0, 3);
            }

            double variance = count > 1 ? m2 / count : 0;
            double stdDev = Math.sqrt(variance);

            double skewness = 0;
            double kurtosis = 3;  // Normal distribution baseline

            if (stdDev > 0 && count > 2) {
                skewness = (m3 / count) / (stdDev * stdDev * stdDev);
                kurtosis = (m4 / count) / (variance * variance);
            }

            return new DimensionStatistics(dimension, count, min, max, mean, variance, skewness, kurtosis);
        } finally {
            getLock().unlock();
        }
    }

    /// Returns the current count of accumulated values.
    long getCount() {
        getLock().lock();
        try {
            return count;
        } finally {
            getLock().unlock();
        }
    }

    /// Combines this accumulator with another using parallel Welford's algorithm.
    ///
    /// This operation is algebraically sound: combining accumulators produces
    /// numerically equivalent results to processing all values in a single accumulator.
    ///
    /// # Algorithm
    ///
    /// For accumulators A and B with counts nA and nB:
    ///
    /// ```text
    /// nAB = nA + nB
    /// δ = meanB - meanA
    /// meanAB = meanA + δ * nB / nAB
    ///
    /// M2AB = M2A + M2B + δ² * nA * nB / nAB
    /// M3AB = M3A + M3B + δ³ * nA * nB * (nA - nB) / nAB²
    ///        + 3 * δ * (nA * M2B - nB * M2A) / nAB
    /// M4AB = M4A + M4B + δ⁴ * nA * nB * (nA² - nA*nB + nB²) / nAB³
    ///        + 6 * δ² * (nA² * M2B + nB² * M2A) / nAB²
    ///        + 4 * δ * (nA * M3B - nB * M3A) / nAB
    /// ```
    ///
    /// # Algebraic Properties
    ///
    /// - **Associativity:** combine(A, combine(B, C)) == combine(combine(A, B), C)
    /// - **Commutativity:** combine(A, B) ≈ combine(B, A) (up to floating-point precision)
    ///
    /// @param other the accumulator to combine with
    /// @return a new accumulator with combined statistics
    OnlineMomentsAccumulator combine(OnlineMomentsAccumulator other) {
        getLock().lock();
        try {
            other.getLock().lock();
            try {
                OnlineMomentsAccumulator result = new OnlineMomentsAccumulator();

                if (this.count == 0) {
                    result.count = other.count;
                    result.mean = other.mean;
                    result.m2 = other.m2;
                    result.m3 = other.m3;
                    result.m4 = other.m4;
                    result.min = other.min;
                    result.max = other.max;
                    return result;
                }

                if (other.count == 0) {
                    result.count = this.count;
                    result.mean = this.mean;
                    result.m2 = this.m2;
                    result.m3 = this.m3;
                    result.m4 = this.m4;
                    result.min = this.min;
                    result.max = this.max;
                    return result;
                }

                long nA = this.count;
                long nB = other.count;
                long nAB = nA + nB;

                double delta = other.mean - this.mean;
                double delta2 = delta * delta;
                double delta3 = delta2 * delta;
                double delta4 = delta2 * delta2;

                double nA_d = (double) nA;
                double nB_d = (double) nB;
                double nAB_d = (double) nAB;

                // Combined mean
                result.mean = this.mean + delta * nB_d / nAB_d;
                result.count = nAB;

                // Combined M2 (variance numerator)
                result.m2 = this.m2 + other.m2 + delta2 * nA_d * nB_d / nAB_d;

                // Combined M3 (skewness numerator)
                result.m3 = this.m3 + other.m3
                    + delta3 * nA_d * nB_d * (nA_d - nB_d) / (nAB_d * nAB_d)
                    + 3.0 * delta * (nA_d * other.m2 - nB_d * this.m2) / nAB_d;

                // Combined M4 (kurtosis numerator)
                result.m4 = this.m4 + other.m4
                    + delta4 * nA_d * nB_d * (nA_d * nA_d - nA_d * nB_d + nB_d * nB_d) / (nAB_d * nAB_d * nAB_d)
                    + 6.0 * delta2 * (nA_d * nA_d * other.m2 + nB_d * nB_d * this.m2) / (nAB_d * nAB_d)
                    + 4.0 * delta * (nA_d * other.m3 - nB_d * this.m3) / nAB_d;

                // Combined min/max
                result.min = Math.min(this.min, other.min);
                result.max = Math.max(this.max, other.max);

                return result;
            } finally {
                other.getLock().unlock();
            }
        } finally {
            getLock().unlock();
        }
    }

    /// Combines this accumulator with another, modifying this accumulator in place.
    ///
    /// @param other the accumulator to merge into this one
    void combineInPlace(OnlineMomentsAccumulator other) {
        OnlineMomentsAccumulator combined = this.combine(other);
        getLock().lock();
        try {
            this.count = combined.count;
            this.mean = combined.mean;
            this.m2 = combined.m2;
            this.m3 = combined.m3;
            this.m4 = combined.m4;
            this.min = combined.min;
            this.max = combined.max;
        } finally {
            getLock().unlock();
        }
    }
}
