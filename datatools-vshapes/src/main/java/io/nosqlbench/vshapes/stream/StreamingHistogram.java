package io.nosqlbench.vshapes.stream;

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

import java.util.ArrayList;
import java.util.List;

/// Streaming histogram for incremental distribution shape analysis.
///
/// ## Purpose
///
/// This class maintains a fixed-bin histogram that updates incrementally as values
/// arrive. It supports multi-modal detection by finding peaks in the distribution,
/// which can indicate the need for mixture models or empirical distributions.
///
/// ## Adaptive Binning
///
/// The histogram uses adaptive bounds that expand as new min/max values are seen.
/// When bounds expand, existing counts are redistributed to maintain accuracy.
///
/// ## Multi-Modal Detection
///
/// The [findModes] method uses a simple peak-finding algorithm:
/// 1. Smooth the histogram using a moving average
/// 2. Find local maxima (bins higher than neighbors)
/// 3. Filter peaks by minimum prominence (height relative to surrounding valleys)
///
/// ## Thread Safety
///
/// This class is NOT thread-safe. Use external synchronization for concurrent access.
///
/// ## Usage
///
/// ```java
/// StreamingHistogram hist = new StreamingHistogram(100);  // 100 bins
///
/// // Add values incrementally
/// for (float[] chunk : dataSource) {
///     hist.addAll(chunk);
/// }
///
/// // Check for multi-modality
/// List<Mode> modes = hist.findModes(0.1);  // 10% prominence threshold
/// if (modes.size() > 1) {
///     System.out.println("Multi-modal distribution detected!");
/// }
/// ```
public final class StreamingHistogram {

    /// Default number of bins
    public static final int DEFAULT_BINS = 100;

    /// Minimum prominence for a peak to be considered a mode (as fraction of max count)
    public static final double DEFAULT_PROMINENCE_THRESHOLD = 0.1;

    private final int numBins;
    private final long[] counts;
    private long totalCount = 0;

    // Adaptive bounds
    private double minValue = Double.POSITIVE_INFINITY;
    private double maxValue = Double.NEGATIVE_INFINITY;
    private boolean boundsInitialized = false;

    // Cached bin width (recomputed when bounds change)
    private double binWidth = 1.0;

    /// Creates a histogram with the default number of bins.
    public StreamingHistogram() {
        this(DEFAULT_BINS);
    }

    /// Creates a histogram with the specified number of bins.
    ///
    /// @param numBins number of bins (must be >= 10)
    public StreamingHistogram(int numBins) {
        if (numBins < 10) {
            throw new IllegalArgumentException("numBins must be at least 10, got: " + numBins);
        }
        this.numBins = numBins;
        this.counts = new long[numBins];
    }

    /// Creates a histogram with fixed bounds.
    ///
    /// @param numBins number of bins
    /// @param minValue minimum value (inclusive)
    /// @param maxValue maximum value (inclusive)
    public StreamingHistogram(int numBins, double minValue, double maxValue) {
        this(numBins);
        if (minValue >= maxValue) {
            throw new IllegalArgumentException("minValue must be less than maxValue");
        }
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.binWidth = (maxValue - minValue) / numBins;
        this.boundsInitialized = true;
    }

    /// Adds a single value to the histogram.
    ///
    /// @param value the value to add
    public void add(float value) {
        addValue(value);
    }

    /// Adds a single value to the histogram.
    ///
    /// @param value the value to add
    public void add(double value) {
        addValue(value);
    }

    /// Adds multiple values to the histogram.
    ///
    /// @param values array of values to add
    public void addAll(float[] values) {
        if (values == null || values.length == 0) return;

        // First pass: find min/max
        double newMin = minValue;
        double newMax = maxValue;
        for (float v : values) {
            if (v < newMin) newMin = v;
            if (v > newMax) newMax = v;
        }

        // Check if bounds need to expand
        if (newMin < minValue || newMax > maxValue) {
            expandBounds(newMin, newMax);
        }

        // Second pass: add to bins
        for (float v : values) {
            int bin = getBin(v);
            counts[bin]++;
            totalCount++;
        }
    }

    /// Adds multiple values to the histogram.
    ///
    /// @param values array of values to add
    public void addAll(double[] values) {
        if (values == null || values.length == 0) return;

        double newMin = minValue;
        double newMax = maxValue;
        for (double v : values) {
            if (v < newMin) newMin = v;
            if (v > newMax) newMax = v;
        }

        if (newMin < minValue || newMax > maxValue) {
            expandBounds(newMin, newMax);
        }

        for (double v : values) {
            int bin = getBin(v);
            counts[bin]++;
            totalCount++;
        }
    }

    private void addValue(double value) {
        if (!boundsInitialized) {
            // First value - initialize bounds with small range
            minValue = value - 0.5;
            maxValue = value + 0.5;
            binWidth = (maxValue - minValue) / numBins;
            boundsInitialized = true;
        } else if (value < minValue || value > maxValue) {
            expandBounds(Math.min(value, minValue), Math.max(value, maxValue));
        }

        int bin = getBin(value);
        counts[bin]++;
        totalCount++;
    }

    /// Expands the histogram bounds and redistributes existing counts.
    private void expandBounds(double newMin, double newMax) {
        if (!boundsInitialized || totalCount == 0) {
            minValue = newMin;
            maxValue = newMax;
            binWidth = (maxValue - minValue) / numBins;
            boundsInitialized = true;
            return;
        }

        // Add margin to avoid immediate re-expansion
        double range = newMax - newMin;
        double margin = range * 0.1;
        newMin -= margin;
        newMax += margin;

        double oldMin = minValue;
        double oldMax = maxValue;
        double oldBinWidth = binWidth;

        // Update bounds
        minValue = newMin;
        maxValue = newMax;
        binWidth = (maxValue - minValue) / numBins;

        // Redistribute counts from old bins to new bins
        long[] oldCounts = counts.clone();
        java.util.Arrays.fill(counts, 0);

        for (int oldBin = 0; oldBin < numBins; oldBin++) {
            if (oldCounts[oldBin] == 0) continue;

            // Find center of old bin
            double oldBinCenter = oldMin + (oldBin + 0.5) * oldBinWidth;

            // Map to new bin
            int newBin = getBin(oldBinCenter);
            counts[newBin] += oldCounts[oldBin];
        }
    }

    /// Returns the bin index for a value.
    private int getBin(double value) {
        if (value <= minValue) return 0;
        if (value >= maxValue) return numBins - 1;
        int bin = (int) ((value - minValue) / binWidth);
        return Math.min(bin, numBins - 1);
    }

    /// Finds modes (peaks) in the histogram.
    ///
    /// @param prominenceThreshold minimum prominence as fraction of max count (0.0 to 1.0)
    /// @return list of detected modes, sorted by count (highest first)
    public List<Mode> findModes(double prominenceThreshold) {
        if (totalCount == 0) {
            return List.of();
        }

        // Smooth the histogram
        double[] smoothed = smooth(3);  // Window size 3

        // Find max count for prominence calculation
        double maxCount = 0;
        for (double c : smoothed) {
            if (c > maxCount) maxCount = c;
        }

        double minProminence = maxCount * prominenceThreshold;

        // Find local maxima
        List<Mode> modes = new ArrayList<>();
        for (int i = 1; i < numBins - 1; i++) {
            if (smoothed[i] > smoothed[i - 1] && smoothed[i] > smoothed[i + 1]) {
                // Found a local maximum - calculate prominence
                double prominence = calculateProminence(smoothed, i);
                if (prominence >= minProminence) {
                    double binCenter = minValue + (i + 0.5) * binWidth;
                    modes.add(new Mode(i, binCenter, counts[i], prominence));
                }
            }
        }

        // Also check edges
        if (smoothed[0] > smoothed[1]) {
            double prominence = calculateProminence(smoothed, 0);
            if (prominence >= minProminence) {
                double binCenter = minValue + 0.5 * binWidth;
                modes.add(new Mode(0, binCenter, counts[0], prominence));
            }
        }
        if (smoothed[numBins - 1] > smoothed[numBins - 2]) {
            double prominence = calculateProminence(smoothed, numBins - 1);
            if (prominence >= minProminence) {
                double binCenter = minValue + (numBins - 0.5) * binWidth;
                modes.add(new Mode(numBins - 1, binCenter, counts[numBins - 1], prominence));
            }
        }

        // Sort by count (highest first)
        modes.sort((a, b) -> Long.compare(b.count(), a.count()));

        return modes;
    }

    /// Smooths the histogram using a moving average.
    private double[] smooth(int windowSize) {
        double[] result = new double[numBins];
        int halfWindow = windowSize / 2;

        for (int i = 0; i < numBins; i++) {
            double sum = 0;
            int count = 0;
            for (int j = Math.max(0, i - halfWindow); j <= Math.min(numBins - 1, i + halfWindow); j++) {
                sum += counts[j];
                count++;
            }
            result[i] = sum / count;
        }
        return result;
    }

    /// Calculates the prominence of a peak.
    /// Prominence is the height of the peak above the highest valley on either side.
    private double calculateProminence(double[] smoothed, int peakIndex) {
        double peakHeight = smoothed[peakIndex];

        // Find lowest point to the left before a higher peak
        double leftValley = peakHeight;
        for (int i = peakIndex - 1; i >= 0; i--) {
            if (smoothed[i] < leftValley) {
                leftValley = smoothed[i];
            }
            if (smoothed[i] > peakHeight) {
                break;  // Found a higher peak
            }
        }

        // Find lowest point to the right before a higher peak
        double rightValley = peakHeight;
        for (int i = peakIndex + 1; i < numBins; i++) {
            if (smoothed[i] < rightValley) {
                rightValley = smoothed[i];
            }
            if (smoothed[i] > peakHeight) {
                break;  // Found a higher peak
            }
        }

        // Prominence is height above the higher of the two valleys
        double higherValley = Math.max(leftValley, rightValley);
        return peakHeight - higherValley;
    }

    /// Returns whether the distribution appears to be multi-modal.
    ///
    /// @param prominenceThreshold minimum prominence as fraction of max count
    /// @return true if more than one significant mode is detected
    public boolean isMultiModal(double prominenceThreshold) {
        return findModes(prominenceThreshold).size() > 1;
    }

    /// Returns the number of bins.
    public int getNumBins() {
        return numBins;
    }

    /// Returns the total count of values added.
    public long getTotalCount() {
        return totalCount;
    }

    /// Returns the minimum value seen.
    public double getMinValue() {
        return minValue;
    }

    /// Returns the maximum value seen.
    public double getMaxValue() {
        return maxValue;
    }

    /// Returns the bin counts.
    public long[] getCounts() {
        return counts.clone();
    }

    /// Returns the count for a specific bin.
    public long getCount(int bin) {
        if (bin < 0 || bin >= numBins) {
            throw new IndexOutOfBoundsException("bin: " + bin);
        }
        return counts[bin];
    }

    /// Returns the bin width.
    public double getBinWidth() {
        return binWidth;
    }

    /// Returns the center value of a bin.
    public double getBinCenter(int bin) {
        return minValue + (bin + 0.5) * binWidth;
    }

    /// Resets the histogram to its initial state.
    public void reset() {
        java.util.Arrays.fill(counts, 0);
        totalCount = 0;
        minValue = Double.POSITIVE_INFINITY;
        maxValue = Double.NEGATIVE_INFINITY;
        boundsInitialized = false;
        binWidth = 1.0;
    }

    /// Returns a normalized density estimate.
    ///
    /// @return array of probability densities (sums to approximately 1.0)
    public double[] getDensity() {
        double[] density = new double[numBins];
        if (totalCount == 0) return density;

        double normFactor = 1.0 / (totalCount * binWidth);
        for (int i = 0; i < numBins; i++) {
            density[i] = counts[i] * normFactor;
        }
        return density;
    }

    @Override
    public String toString() {
        List<Mode> modes = findModes(DEFAULT_PROMINENCE_THRESHOLD);
        return String.format(
            "StreamingHistogram[bins=%d, n=%d, range=[%.4f, %.4f], modes=%d]",
            numBins, totalCount, minValue, maxValue, modes.size());
    }

    /// Represents a detected mode (peak) in the histogram.
    ///
    /// @param binIndex the bin index of the mode
    /// @param value the center value of the mode
    /// @param count the count at the mode
    /// @param prominence the prominence of the peak
    public record Mode(int binIndex, double value, long count, double prominence) {
        @Override
        public String toString() {
            return String.format("Mode[value=%.4f, count=%d, prominence=%.1f]", value, count, prominence);
        }
    }
}
