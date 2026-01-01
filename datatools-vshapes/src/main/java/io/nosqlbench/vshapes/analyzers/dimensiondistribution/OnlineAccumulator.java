package io.nosqlbench.vshapes.analyzers.dimensiondistribution;

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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

/// Thread-safe online statistics accumulator with reservoir sampling.
///
/// # Overview
///
/// This class accumulates statistics from a stream of floating-point values
/// using online algorithms that require only O(1) memory regardless of stream size.
/// It computes:
///
/// - Mean and variance (Welford's algorithm)
/// - Skewness and kurtosis (extended Welford's)
/// - Min/max bounds
/// - Representative sample (reservoir sampling)
///
/// # Algorithm Details
///
/// ## Welford's Online Algorithm
///
/// Computes running mean and variance with numerical stability:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    WELFORD'S ALGORITHM                          │
/// └─────────────────────────────────────────────────────────────────┘
///
///   For each new value x:
///
///     count = count + 1
///     delta = x - mean
///     mean = mean + delta / count
///     delta2 = x - mean
///     M2 = M2 + delta * delta2
///
///   After all values:
///
///     variance = M2 / count
///     stddev = sqrt(variance)
/// ```
///
/// The key insight is that `delta * delta2` equals `(x - old_mean) * (x - new_mean)`,
/// which provides numerical stability even for values far from zero.
///
/// ## Higher Moments
///
/// Extended to compute skewness (M3) and kurtosis (M4):
///
/// ```text
///   M4 += term1 * deltaN² * (n² - 3n + 3) + 6 * deltaN² * M2 - 4 * deltaN * M3
///   M3 += term1 * deltaN * (n - 2) - 3 * deltaN * M2
///   M2 += term1
///
///   where:
///     term1 = delta * deltaN * (n - 1)
///     deltaN = delta / n
/// ```
///
/// ## Reservoir Sampling (Algorithm R)
///
/// Maintains k samples with uniform probability from a stream:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    RESERVOIR SAMPLING                           │
/// └─────────────────────────────────────────────────────────────────┘
///
///   reservoir[k]  <-  array of k samples
///   n = 0         <-  count of elements seen
///
///   For each element x:
///     n = n + 1
///     if n <= k:
///       reservoir[n-1] = x     // Fill initial reservoir
///     else:
///       j = random(1, n)       // Random index in [1, n]
///       if j <= k:
///         reservoir[j-1] = x   // Replace with probability k/n
/// ```
///
/// **Probability Analysis**: Each element has exactly k/n probability of being
/// in the final reservoir, ensuring uniform sampling.
///
/// # Thread Safety
///
/// All operations are protected by a [ReentrantLock]. Multiple threads can
/// safely call [#accept] concurrently from different harness worker threads.
///
/// ```text
/// ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
/// │  Thread 1   │     │  Thread 2   │     │  Thread 3   │
/// │  accept(x)  │     │  accept(y)  │     │  accept(z)  │
/// └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
///        │                   │                   │
///        └───────────────────┼───────────────────┘
///                            ▼
///                    ┌───────────────┐
///                    │ ReentrantLock │
///                    │   (serialize) │
///                    └───────────────┘
///                            │
///                            ▼
///                    ┌───────────────┐
///                    │ Accumulator   │
///                    │    State      │
///                    └───────────────┘
/// ```
///
/// # Memory Usage
///
/// Memory is bounded by the reservoir size:
///
/// | Component | Size |
/// |-----------|------|
/// | Reservoir array | `k * 4 bytes` (float[]) |
/// | Statistics state | ~64 bytes |
/// | Lock overhead | ~32 bytes |
///
/// Total: approximately `4k + 96` bytes per accumulator.
///
/// @see DimensionDistributionAnalyzer
/// @see DimensionStatistics
final class OnlineAccumulator {

    private final ReentrantLock lock = new ReentrantLock();
    private final float[] reservoir;
    private final int reservoirSize;

    private long count = 0;
    private double mean = 0;
    private double m2 = 0;   // Sum of squared deviations (for variance)
    private double m3 = 0;   // Third central moment (for skewness)
    private double m4 = 0;   // Fourth central moment (for kurtosis)
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    private long reservoirCount = 0;

    /// Creates an accumulator with the specified reservoir size.
    ///
    /// @param reservoirSize number of samples to retain for empirical fitting
    OnlineAccumulator(int reservoirSize) {
        this.reservoirSize = reservoirSize;
        this.reservoir = new float[reservoirSize];
    }

    /// Accepts a new value into the accumulator.
    ///
    /// Updates all running statistics and potentially adds the value
    /// to the reservoir sample.
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

            // Update higher moments (for skewness and kurtosis)
            m4 += term1 * deltaN2 * (count * count - 3 * count + 3) +
                  6 * deltaN2 * m2 - 4 * deltaN * m3;
            m3 += term1 * deltaN * (count - 2) - 3 * deltaN * m2;
            m2 += term1;

            if (value < min) min = value;
            if (value > max) max = value;

            // Reservoir sampling (Algorithm R)
            if (reservoirCount < reservoirSize) {
                reservoir[(int) reservoirCount] = value;
            } else {
                long r = ThreadLocalRandom.current().nextLong(reservoirCount + 1);
                if (r < reservoirSize) {
                    reservoir[(int) r] = value;
                }
            }
            reservoirCount++;
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

    /// Returns a copy of the reservoir samples.
    ///
    /// @return array of sampled values (may be smaller than reservoir size
    ///         if fewer values were seen)
    float[] getSamples() {
        lock.lock();
        try {
            int size = (int) Math.min(reservoirCount, reservoirSize);
            float[] result = new float[size];
            System.arraycopy(reservoir, 0, result, 0, size);
            return result;
        } finally {
            lock.unlock();
        }
    }
}
