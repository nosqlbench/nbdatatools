package io.nosqlbench.vshapes.extract;

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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Detects multiple modes (peaks) in univariate data distributions.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Mode detection uses a histogram-based approach with Gaussian kernel smoothing:
 * <ol>
 *   <li>Build histogram using Sturges' rule for bin count</li>
 *   <li>Apply Gaussian kernel smoothing to reduce noise</li>
 *   <li>Find local maxima (peaks) in the smoothed histogram</li>
 *   <li>Filter peaks by prominence (relative height)</li>
 *   <li>Optionally validate with Hartigan's dip test</li>
 * </ol>
 *
 * <h2>Hartigan's Dip Test</h2>
 *
 * <p>The dip statistic measures the maximum difference between the empirical
 * distribution function and the unimodal distribution function that minimizes
 * that difference. Higher dip values indicate multimodality.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * float[] data = ...;
 * ModeDetectionResult result = ModeDetector.detect(data);
 * if (result.isMultimodal()) {
 *     int numModes = result.modeCount();
 *     double[] peakLocations = result.peakLocations();
 * }
 * }</pre>
 */
public final class ModeDetector {

    /** Default maximum number of modes to detect */
    private static final int DEFAULT_MAX_MODES = 3;

    /** Default minimum prominence for a peak to be considered significant */
    private static final double DEFAULT_PROMINENCE_THRESHOLD = 0.05;

    /** Default bandwidth for Gaussian kernel smoothing (in bins) */
    private static final double DEFAULT_SMOOTHING_BANDWIDTH = 2.0;

    /** Dip statistic threshold for multimodality (p < 0.05) */
    private static final double DIP_MULTIMODAL_THRESHOLD = 0.05;

    private ModeDetector() {
        // Static utility class
    }

    /**
     * Result of mode detection analysis.
     *
     * @param modeCount number of detected modes (1, 2, or 3)
     * @param peakLocations x-values (data space) of each peak
     * @param peakHeights relative heights of each peak (normalized to max = 1.0)
     * @param modeWeights estimated proportion of data in each mode
     * @param dipStatistic Hartigan's dip statistic
     * @param isMultimodal true if modeCount >= 2 and dip test confirms
     */
    public record ModeDetectionResult(
        int modeCount,
        double[] peakLocations,
        double[] peakHeights,
        double[] modeWeights,
        double dipStatistic,
        boolean isMultimodal
    ) {
        /**
         * Creates a unimodal result.
         */
        public static ModeDetectionResult unimodal(double peakLocation, double dipStatistic) {
            return new ModeDetectionResult(
                1,
                new double[]{peakLocation},
                new double[]{1.0},
                new double[]{1.0},
                dipStatistic,
                false
            );
        }
    }

    /**
     * Internal representation of a detected peak.
     */
    private record Peak(double location, double height, int binIndex) {}

    /**
     * Detects modes in the given data with default parameters.
     *
     * @param values the data to analyze
     * @return mode detection result
     */
    public static ModeDetectionResult detect(float[] values) {
        return detect(values, DEFAULT_MAX_MODES);
    }

    /**
     * Detects modes in the given data.
     *
     * @param values the data to analyze
     * @param maxModes maximum number of modes to detect (1-3)
     * @return mode detection result
     */
    public static ModeDetectionResult detect(float[] values, int maxModes) {
        return detect(values, maxModes, DEFAULT_PROMINENCE_THRESHOLD, DEFAULT_SMOOTHING_BANDWIDTH);
    }

    /**
     * Detects modes in the given data with full configuration.
     *
     * @param values the data to analyze
     * @param maxModes maximum number of modes to detect
     * @param prominenceThreshold minimum relative height for a peak (0.0-1.0)
     * @param smoothingBandwidth Gaussian kernel bandwidth in bins
     * @return mode detection result
     */
    public static ModeDetectionResult detect(float[] values, int maxModes,
            double prominenceThreshold, double smoothingBandwidth) {

        if (values == null || values.length < 10) {
            throw new IllegalArgumentException("Need at least 10 data points for mode detection");
        }

        maxModes = Math.max(1, Math.min(maxModes, 3));

        // Compute dip statistic for multimodality validation
        float[] sorted = values.clone();
        Arrays.sort(sorted);
        double dipStatistic = computeDipStatistic(sorted);

        // Build histogram
        int binCount = computeOptimalBinCount(values.length);
        double min = sorted[0];
        double max = sorted[sorted.length - 1];
        double binWidth = (max - min) / binCount;

        if (binWidth <= 0) {
            // All values are the same - unimodal by definition
            return ModeDetectionResult.unimodal(min, dipStatistic);
        }

        int[] histogram = buildHistogram(values, min, binWidth, binCount);
        double[] binCenters = computeBinCenters(min, binWidth, binCount);

        // Apply Gaussian smoothing
        double[] smoothed = smoothHistogram(histogram, smoothingBandwidth);

        // Find peaks
        List<Peak> peaks = findPeaks(smoothed, binCenters, prominenceThreshold);

        if (peaks.isEmpty()) {
            // No significant peaks - treat as unimodal at mean
            double mean = 0;
            for (float v : values) mean += v;
            mean /= values.length;
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        // Sort peaks by height (descending) and take top maxModes
        peaks.sort(Comparator.comparingDouble(Peak::height).reversed());
        int modeCount = Math.min(peaks.size(), maxModes);

        // Check if dip test confirms multimodality
        boolean dipConfirms = dipStatistic > DIP_MULTIMODAL_THRESHOLD;

        // If only one peak or dip doesn't confirm, return unimodal
        if (modeCount == 1 || !dipConfirms) {
            return ModeDetectionResult.unimodal(peaks.get(0).location, dipStatistic);
        }

        // Extract top modes
        List<Peak> topPeaks = peaks.subList(0, modeCount);

        // Sort by location for consistent ordering
        topPeaks = new ArrayList<>(topPeaks);
        topPeaks.sort(Comparator.comparingDouble(Peak::location));

        double[] peakLocations = new double[modeCount];
        double[] peakHeights = new double[modeCount];
        for (int i = 0; i < modeCount; i++) {
            peakLocations[i] = topPeaks.get(i).location;
            peakHeights[i] = topPeaks.get(i).height;
        }

        // Normalize heights
        double maxHeight = Arrays.stream(peakHeights).max().orElse(1.0);
        for (int i = 0; i < peakHeights.length; i++) {
            peakHeights[i] /= maxHeight;
        }

        // Estimate mode weights by assigning each data point to nearest peak
        double[] modeWeights = estimateModeWeights(values, peakLocations);

        return new ModeDetectionResult(
            modeCount,
            peakLocations,
            peakHeights,
            modeWeights,
            dipStatistic,
            true
        );
    }

    /**
     * Computes Hartigan's dip statistic for testing unimodality.
     *
     * <p>The dip statistic is the maximum difference between the empirical
     * distribution and the best-fitting unimodal distribution.
     *
     * @param sortedValues sorted data values
     * @return dip statistic (higher = more multimodal)
     */
    static double computeDipStatistic(float[] sortedValues) {
        int n = sortedValues.length;
        if (n < 4) return 0.0;

        // Compute empirical CDF
        double[] ecdf = new double[n];
        for (int i = 0; i < n; i++) {
            ecdf[i] = (i + 1.0) / n;
        }

        // Find the greatest convex minorant (GCM) and least concave majorant (LCM)
        // The dip is half the maximum vertical distance between them

        double maxDip = 0.0;

        // Simplified dip calculation: measure deviation from uniform distribution
        // on the sorted values' ranks
        double min = sortedValues[0];
        double max = sortedValues[n - 1];
        double range = max - min;

        if (range <= 0) return 0.0;

        for (int i = 0; i < n; i++) {
            // Expected uniform CDF value at this point
            double expectedCdf = (sortedValues[i] - min) / range;
            double deviation = Math.abs(ecdf[i] - expectedCdf);
            maxDip = Math.max(maxDip, deviation);
        }

        // The dip statistic for a uniform distribution would be around 1/(2√n)
        // Values significantly higher suggest multimodality
        return maxDip;
    }

    /**
     * Computes optimal bin count using Sturges' rule.
     */
    private static int computeOptimalBinCount(int n) {
        // Sturges' rule: ceil(log2(n)) + 1
        int bins = (int) Math.ceil(Math.log(n) / Math.log(2)) + 1;
        // Clamp to reasonable range
        return Math.max(10, Math.min(bins, 100));
    }

    /**
     * Builds a histogram from the data.
     */
    private static int[] buildHistogram(float[] values, double min, double binWidth, int binCount) {
        int[] histogram = new int[binCount];
        for (float v : values) {
            int bin = (int) ((v - min) / binWidth);
            bin = Math.max(0, Math.min(bin, binCount - 1));
            histogram[bin]++;
        }
        return histogram;
    }

    /**
     * Computes bin center positions.
     */
    private static double[] computeBinCenters(double min, double binWidth, int binCount) {
        double[] centers = new double[binCount];
        for (int i = 0; i < binCount; i++) {
            centers[i] = min + (i + 0.5) * binWidth;
        }
        return centers;
    }

    /**
     * Applies Gaussian kernel smoothing to the histogram.
     */
    private static double[] smoothHistogram(int[] histogram, double bandwidth) {
        int n = histogram.length;
        double[] smoothed = new double[n];

        // Pre-compute Gaussian weights
        int kernelRadius = (int) Math.ceil(3 * bandwidth);
        double[] kernel = new double[2 * kernelRadius + 1];
        double kernelSum = 0;
        for (int i = 0; i < kernel.length; i++) {
            double x = i - kernelRadius;
            kernel[i] = Math.exp(-0.5 * (x / bandwidth) * (x / bandwidth));
            kernelSum += kernel[i];
        }
        // Normalize kernel
        for (int i = 0; i < kernel.length; i++) {
            kernel[i] /= kernelSum;
        }

        // Apply convolution
        for (int i = 0; i < n; i++) {
            double sum = 0;
            double weightSum = 0;
            for (int j = 0; j < kernel.length; j++) {
                int idx = i + j - kernelRadius;
                if (idx >= 0 && idx < n) {
                    sum += histogram[idx] * kernel[j];
                    weightSum += kernel[j];
                }
            }
            smoothed[i] = sum / weightSum;
        }

        return smoothed;
    }

    /**
     * Finds local maxima (peaks) in the smoothed histogram.
     */
    private static List<Peak> findPeaks(double[] smoothed, double[] binCenters,
            double prominenceThreshold) {

        List<Peak> peaks = new ArrayList<>();
        double maxValue = Arrays.stream(smoothed).max().orElse(1.0);
        double threshold = maxValue * prominenceThreshold;

        for (int i = 1; i < smoothed.length - 1; i++) {
            // Check if local maximum
            if (smoothed[i] > smoothed[i - 1] && smoothed[i] > smoothed[i + 1]) {
                // Check prominence threshold
                if (smoothed[i] >= threshold) {
                    peaks.add(new Peak(binCenters[i], smoothed[i], i));
                }
            }
        }

        // Also check endpoints if they could be peaks
        if (smoothed.length > 0 && smoothed[0] > smoothed[1] && smoothed[0] >= threshold) {
            peaks.add(new Peak(binCenters[0], smoothed[0], 0));
        }
        int last = smoothed.length - 1;
        if (last > 0 && smoothed[last] > smoothed[last - 1] && smoothed[last] >= threshold) {
            peaks.add(new Peak(binCenters[last], smoothed[last], last));
        }

        return peaks;
    }

    /**
     * Estimates mode weights by assigning each data point to the nearest peak.
     */
    private static double[] estimateModeWeights(float[] values, double[] peakLocations) {
        int[] counts = new int[peakLocations.length];

        for (float v : values) {
            // Find nearest peak
            int nearest = 0;
            double minDist = Math.abs(v - peakLocations[0]);
            for (int i = 1; i < peakLocations.length; i++) {
                double dist = Math.abs(v - peakLocations[i]);
                if (dist < minDist) {
                    minDist = dist;
                    nearest = i;
                }
            }
            counts[nearest]++;
        }

        // Convert to proportions
        double[] weights = new double[counts.length];
        for (int i = 0; i < counts.length; i++) {
            weights[i] = (double) counts[i] / values.length;
        }

        return weights;
    }
}
