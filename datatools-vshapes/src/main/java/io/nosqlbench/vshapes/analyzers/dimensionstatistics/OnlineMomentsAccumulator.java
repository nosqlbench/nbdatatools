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

    private final ReentrantLock lock = new ReentrantLock();

    private long count = 0;
    private double mean = 0;
    private double m2 = 0;   // Second central moment (for variance)
    private double m3 = 0;   // Third central moment (for skewness)
    private double m4 = 0;   // Fourth central moment (for kurtosis)
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    /// Creates a new moments accumulator.
    OnlineMomentsAccumulator() {
    }

    /// Accepts a new value into the accumulator.
    ///
    /// Updates all running statistics using Welford's algorithm.
    ///
    /// @param value the value to accumulate
    void accept(float value) {
        lock.lock();
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
            lock.unlock();
        }
    }

    /// Accepts multiple values in a batch.
    ///
    /// More efficient than calling accept() repeatedly when processing chunks.
    ///
    /// @param values the values to accumulate
    void acceptAll(float[] values) {
        lock.lock();
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
            lock.unlock();
        }
    }

    /// Converts accumulated statistics to a [DimensionStatistics] record.
    ///
    /// @param dimension the dimension index for the statistics
    /// @return computed statistics for the dimension
    DimensionStatistics toStatistics(int dimension) {
        lock.lock();
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
            lock.unlock();
        }
    }

    /// Returns the current count of accumulated values.
    long getCount() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }
}
