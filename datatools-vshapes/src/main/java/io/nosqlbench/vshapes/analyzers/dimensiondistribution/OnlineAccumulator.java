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

    /// Lock is transient - not serialized. Re-created on first access after deserialization.
    private transient ReentrantLock lock;
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
    /// Updates all running statistics and potentially adds the value
    /// to the reservoir sample.
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

    /// Returns a copy of the reservoir samples.
    ///
    /// @return array of sampled values (may be smaller than reservoir size
    ///         if fewer values were seen)
    float[] getSamples() {
        getLock().lock();
        try {
            int size = (int) Math.min(reservoirCount, reservoirSize);
            float[] result = new float[size];
            System.arraycopy(reservoir, 0, result, 0, size);
            return result;
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
    /// For the reservoir, samples are drawn from both reservoirs with probability
    /// proportional to their original counts, maintaining approximately uniform sampling.
    ///
    /// # Algebraic Properties
    ///
    /// - **Associativity:** combine(A, combine(B, C)) == combine(combine(A, B), C)
    /// - **Mean/Variance:** Exact combination using Welford's parallel formulas
    /// - **Reservoir:** Approximately uniform sampling from combined population
    ///
    /// @param other the accumulator to combine with
    /// @return a new accumulator with combined statistics and merged reservoir
    OnlineAccumulator combine(OnlineAccumulator other) {
        getLock().lock();
        try {
            other.getLock().lock();
            try {
                OnlineAccumulator result = new OnlineAccumulator(this.reservoirSize);

                if (this.count == 0) {
                    copyTo(other, result);
                    return result;
                }

                if (other.count == 0) {
                    copyTo(this, result);
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

                // Combined statistics
                result.mean = this.mean + delta * nB_d / nAB_d;
                result.count = nAB;
                result.m2 = this.m2 + other.m2 + delta2 * nA_d * nB_d / nAB_d;
                result.m3 = this.m3 + other.m3
                    + delta3 * nA_d * nB_d * (nA_d - nB_d) / (nAB_d * nAB_d)
                    + 3.0 * delta * (nA_d * other.m2 - nB_d * this.m2) / nAB_d;
                result.m4 = this.m4 + other.m4
                    + delta4 * nA_d * nB_d * (nA_d * nA_d - nA_d * nB_d + nB_d * nB_d) / (nAB_d * nAB_d * nAB_d)
                    + 6.0 * delta2 * (nA_d * nA_d * other.m2 + nB_d * nB_d * this.m2) / (nAB_d * nAB_d)
                    + 4.0 * delta * (nA_d * other.m3 - nB_d * this.m3) / nAB_d;
                result.min = Math.min(this.min, other.min);
                result.max = Math.max(this.max, other.max);

                // Merge reservoirs with weighted sampling
                int sizeA = (int) Math.min(this.reservoirCount, this.reservoirSize);
                int sizeB = (int) Math.min(other.reservoirCount, other.reservoirSize);
                double pA = (double) nA / nAB;

                int resultSize = Math.min(this.reservoirSize, sizeA + sizeB);
                result.reservoirCount = this.reservoirCount + other.reservoirCount;

                for (int i = 0; i < resultSize; i++) {
                    if (sizeA > 0 && (sizeB == 0 || ThreadLocalRandom.current().nextDouble() < pA)) {
                        int idx = ThreadLocalRandom.current().nextInt(sizeA);
                        result.reservoir[i] = this.reservoir[idx];
                    } else if (sizeB > 0) {
                        int idx = ThreadLocalRandom.current().nextInt(sizeB);
                        result.reservoir[i] = other.reservoir[idx];
                    }
                }

                return result;
            } finally {
                other.getLock().unlock();
            }
        } finally {
            getLock().unlock();
        }
    }

    private void copyTo(OnlineAccumulator source, OnlineAccumulator dest) {
        dest.count = source.count;
        dest.mean = source.mean;
        dest.m2 = source.m2;
        dest.m3 = source.m3;
        dest.m4 = source.m4;
        dest.min = source.min;
        dest.max = source.max;
        dest.reservoirCount = source.reservoirCount;
        int size = (int) Math.min(source.reservoirCount, source.reservoirSize);
        System.arraycopy(source.reservoir, 0, dest.reservoir, 0, size);
    }
}
