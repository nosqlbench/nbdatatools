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
    private static final int DEFAULT_MAX_MODES = 10;

    /** Default minimum prominence for a peak to be considered significant */
    private static final double DEFAULT_PROMINENCE_THRESHOLD = 0.05;

    /** Default bandwidth for Gaussian kernel smoothing (in bins) */
    private static final double DEFAULT_SMOOTHING_BANDWIDTH = 2.0;

    /** Dip statistic threshold for multimodality (p < 0.05) */
    private static final double DIP_MULTIMODAL_THRESHOLD = 0.05;

    /** Minimum bins per mode for adequate resolution */
    private static final int MIN_BINS_PER_MODE = 5;

    private ModeDetector() {
        // Static utility class
    }

    /**
     * Result of mode detection analysis.
     *
     * @param modeCount number of detected modes (1 to 10)
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
     * @param maxModes maximum number of modes to detect (1-10)
     * @return mode detection result
     */
    public static ModeDetectionResult detect(float[] values, int maxModes) {
        return detect(values, maxModes, DEFAULT_PROMINENCE_THRESHOLD, DEFAULT_SMOOTHING_BANDWIDTH);
    }

    /**
     * Detects modes with adaptive resolution refinement.
     *
     * <p>This method iteratively increases histogram resolution when it detects
     * that peaks might be merged (too wide, asymmetric, or have shoulders).
     * This is particularly useful for high mode counts where closely-spaced
     * modes may appear merged at lower resolutions.
     *
     * <p>The algorithm:
     * <ol>
     *   <li>Start with base resolution from {@link #computeAdaptiveBinCount}</li>
     *   <li>Detect modes at current resolution</li>
     *   <li>Analyze detected peaks for potential merged modes</li>
     *   <li>If merged modes suspected, increase resolution by 50% and repeat</li>
     *   <li>Stop when no improvement or max resolution reached</li>
     * </ol>
     *
     * @param values the data to analyze
     * @param maxModes maximum number of modes to detect (1-10)
     * @return mode detection result with optimal resolution
     */
    public static ModeDetectionResult detectAdaptive(float[] values, int maxModes) {
        if (values == null || values.length < 10) {
            throw new IllegalArgumentException("Need at least 10 data points for mode detection");
        }

        maxModes = Math.max(1, Math.min(maxModes, DEFAULT_MAX_MODES));

        float[] sorted = values.clone();
        Arrays.sort(sorted);

        // Compute base and max bin counts
        int baseBins = computeAdaptiveBinCount(values.length, maxModes, sorted);
        int maxBins = Math.min(500, Math.max(baseBins * 4, values.length / 10));

        ModeDetectionResult bestResult = null;
        int bestModeCount = 0;
        int currentBins = baseBins;
        int iterationsWithoutImprovement = 0;

        // Track mode count stability across iterations
        int previousModeCount = -1;
        int stableIterations = 0;

        while (currentBins <= maxBins && iterationsWithoutImprovement < 3) {
            ModeDetectionResult result = detectWithBinCount(values, sorted, maxModes, currentBins);

            // Check mode count stability - if unchanged for 2 iterations, accept it
            if (result.modeCount() == previousModeCount) {
                stableIterations++;
                if (stableIterations >= 2 && result.modeCount() >= 2) {
                    // Mode count is stable and multimodal - accept this result
                    bestResult = result;
                    bestModeCount = result.modeCount();
                    break;
                }
            } else {
                stableIterations = 0;
                previousModeCount = result.modeCount();
            }

            // Check if we found more modes than before
            if (result.modeCount() > bestModeCount) {
                bestResult = result;
                bestModeCount = result.modeCount();
                iterationsWithoutImprovement = 0;
            } else {
                iterationsWithoutImprovement++;
            }

            // If we've found maxModes, no need to continue
            if (bestModeCount >= maxModes) {
                break;
            }

            // Check if peaks might contain merged modes
            if (!mightHaveMergedPeaks(result, currentBins, maxModes)) {
                break;
            }

            // Increase resolution by 50%
            currentBins = (int) (currentBins * 1.5);
        }

        return bestResult != null ? bestResult :
            ModeDetectionResult.unimodal(computeMean(sorted), computeDipStatistic(sorted));
    }

    /**
     * Detects if the current result might have merged peaks that would benefit
     * from higher resolution.
     *
     * <p>Indicators of merged peaks:
     * <ul>
     *   <li>Fewer modes detected than expected given maxModes</li>
     *   <li>High dip statistic but few modes found (suggests hidden structure)</li>
     *   <li>Detected peak count significantly less than what data variance suggests</li>
     * </ul>
     *
     * @param result the current detection result
     * @param currentBins the current bin count used
     * @param maxModes the expected maximum modes
     * @return true if higher resolution might reveal more modes
     */
    private static boolean mightHaveMergedPeaks(ModeDetectionResult result, int currentBins, int maxModes) {
        // If we found >= 2 modes and dip is reasonable, data is adequately explained
        // Don't keep searching for more modes just because maxModes is high
        if (result.modeCount() >= 2 && result.dipStatistic() < 0.12) {
            return false;
        }

        // More conservative threshold for triggering resolution increase
        // For maxModes=10, only trigger if modeCount < 3 (not < 5)
        int splitThreshold = Math.min(3, Math.max(2, maxModes / 4));

        // If only 1 mode found but moderate dip, there may be hidden modes
        // Use lower threshold (0.06) for unimodal detection since it's more likely wrong
        if (result.modeCount() == 1 && result.dipStatistic() > 0.06) {
            return true;
        }

        // If strong multimodality signal but very few modes found, try higher resolution
        if (result.modeCount() < splitThreshold && result.dipStatistic() > 0.10) {
            return true;
        }

        // Removed: aggressive binsPerMode > 20 check that led to over-detection
        return false;
    }

    /**
     * Detects modes with a specific bin count.
     *
     * @param values the data to analyze
     * @param sorted pre-sorted values
     * @param maxModes maximum modes to detect
     * @param binCount specific bin count to use
     * @return mode detection result
     */
    private static ModeDetectionResult detectWithBinCount(float[] values, float[] sorted,
            int maxModes, int binCount) {

        double dipStatistic = computeDipStatistic(sorted);
        double min = sorted[0];
        double max = sorted[sorted.length - 1];
        double range = max - min;
        double binWidth = range / binCount;

        if (binWidth <= 0) {
            return ModeDetectionResult.unimodal(min, dipStatistic);
        }

        double mean = computeMean(sorted);

        // Check for nearly-constant data
        double coefficientOfVariation = (mean != 0) ? range / Math.abs(mean) : range;
        if (coefficientOfVariation < 0.01) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        int[] histogram = buildHistogram(values, min, binWidth, binCount);
        double[] binCenters = computeBinCenters(min, binWidth, binCount);

        // Check for gaps in the raw histogram BEFORE smoothing
        // Gaps indicate multimodality that smoothing would hide
        GapAnalysis gapAnalysis = analyzeGaps(histogram, values.length, binCount);

        // Use more aggressive (lower) smoothing for higher bin counts
        double baseBandwidth = DEFAULT_SMOOTHING_BANDWIDTH;
        double adaptiveBandwidth = computeAdaptiveBandwidth(baseBandwidth, maxModes, binCount);
        // Further reduce for high resolution
        if (binCount > 100) {
            adaptiveBandwidth *= 0.7;
        }
        // If gaps detected, use minimal smoothing to preserve structure
        if (gapAnalysis.hasSignificantGaps) {
            adaptiveBandwidth = Math.min(adaptiveBandwidth, 0.5);
        }
        double[] smoothed = smoothHistogram(histogram, Math.max(0.3, adaptiveBandwidth));

        double adaptiveProminence = computeAdaptiveProminence(DEFAULT_PROMINENCE_THRESHOLD, maxModes);
        List<Peak> peaks = findPeaks(smoothed, binCenters, adaptiveProminence);

        if (peaks.isEmpty()) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        double dataRange = max - min;
        double minSeparation = dataRange / (maxModes * 3.0);
        peaks = filterPeaksBySeparation(peaks, smoothed, minSeparation, binWidth, maxModes);

        if (peaks.isEmpty()) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        peaks.sort(Comparator.comparingDouble(Peak::height).reversed());
        int modeCount = Math.min(peaks.size(), maxModes);

        boolean dipConfirms = dipStatistic > DIP_MULTIMODAL_THRESHOLD;
        double secondaryPeakThreshold = computeSecondaryPeakThreshold(maxModes);
        boolean peaksAreProminent = peaks.size() >= 2 &&
            peaks.get(1).height >= peaks.get(0).height * secondaryPeakThreshold;

        // Gap analysis provides evidence of multimodality, but requires additional confirmation
        // to avoid false positives on uniform data with random bin variation
        boolean gapsConfirm = gapAnalysis.hasSignificantGaps && gapAnalysis.gapCount >= 2;

        // Multimodality is confirmed if:
        // 1. Dip test confirms (strong statistical evidence), OR
        // 2. Both peaks are prominent AND gaps confirm AND dip is moderate (structural evidence)
        // The dip > 0.03 requirement prevents uniform data from being classified as multimodal
        // (uniform data has very low dip even if peaks/gaps appear due to random variation)
        boolean structuralEvidence = peaksAreProminent && gapsConfirm && dipStatistic > 0.03;
        boolean multimodalConfirmed = dipConfirms || structuralEvidence;

        if (modeCount == 1 || !multimodalConfirmed) {
            // If gaps detected with strong evidence (many gaps + dip above baseline),
            // try to estimate modes from gaps even if only 1 peak found
            boolean strongGapEvidence = gapsConfirm && gapAnalysis.gapCount >= 3 &&
                                         dipStatistic > 0.03;
            if (strongGapEvidence && gapAnalysis.estimatedModes > 1) {
                // Return gap-based mode estimate with elevated dip statistic
                return estimateModesFromGaps(histogram, binCenters, gapAnalysis, values,
                    Math.max(dipStatistic, 0.1));
            }
            return ModeDetectionResult.unimodal(peaks.get(0).location, dipStatistic);
        }

        List<Peak> topPeaks = new ArrayList<>(peaks.subList(0, modeCount));
        topPeaks.sort(Comparator.comparingDouble(Peak::location));

        double[] peakLocations = new double[modeCount];
        double[] peakHeights = new double[modeCount];
        for (int i = 0; i < modeCount; i++) {
            peakLocations[i] = topPeaks.get(i).location;
            peakHeights[i] = topPeaks.get(i).height;
        }

        double maxHeight = Arrays.stream(peakHeights).max().orElse(1.0);
        for (int i = 0; i < peakHeights.length; i++) {
            peakHeights[i] /= maxHeight;
        }

        double[] modeWeights = estimateModeWeights(values, peakLocations);

        return new ModeDetectionResult(modeCount, peakLocations, peakHeights, modeWeights,
            dipStatistic, true);
    }

    /**
     * Computes mean of sorted values.
     */
    private static double computeMean(float[] sorted) {
        double sum = 0;
        for (float v : sorted) sum += v;
        return sum / sorted.length;
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

        maxModes = Math.max(1, Math.min(maxModes, DEFAULT_MAX_MODES));

        // Compute dip statistic for multimodality validation
        float[] sorted = values.clone();
        Arrays.sort(sorted);
        double dipStatistic = computeDipStatistic(sorted);

        // Build histogram with adaptive bin count for expected modes
        int binCount = computeAdaptiveBinCount(values.length, maxModes, sorted);
        double min = sorted[0];
        double max = sorted[sorted.length - 1];
        double range = max - min;
        double binWidth = range / binCount;

        if (binWidth <= 0) {
            // All values are the same - unimodal by definition
            return ModeDetectionResult.unimodal(min, dipStatistic);
        }

        // Check for nearly-constant data: if range is tiny relative to values, treat as unimodal
        // Use coefficient of variation: range / mean should be significant
        double mean = 0;
        for (float v : sorted) mean += v;
        mean /= values.length;
        double coefficientOfVariation = (mean != 0) ? range / Math.abs(mean) : range;
        if (coefficientOfVariation < 0.01) {
            // Data is effectively constant - less than 1% variation
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        int[] histogram = buildHistogram(values, min, binWidth, binCount);
        double[] binCenters = computeBinCenters(min, binWidth, binCount);

        // Compute adaptive smoothing bandwidth - reduce for more modes
        // For high mode counts, we need less smoothing to preserve peaks
        double adaptiveBandwidth = computeAdaptiveBandwidth(smoothingBandwidth, maxModes, binCount);
        double[] smoothed = smoothHistogram(histogram, adaptiveBandwidth);

        // Compute adaptive prominence threshold - lower for more modes
        // For 10 equal-weight modes, each peak is ~10% of max, so threshold must be lower
        double adaptiveProminence = computeAdaptiveProminence(prominenceThreshold, maxModes);

        // Find peaks with adaptive thresholds
        List<Peak> peaks = findPeaks(smoothed, binCenters, adaptiveProminence);

        if (peaks.isEmpty()) {
            // No significant peaks - treat as unimodal at mean (already computed above)
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        // Filter peaks to remove noise: require minimum separation and local prominence
        // For high mode counts, use a smaller minimum separation
        double dataRange = max - min;
        double minSeparation = dataRange / (maxModes * 3.0);  // Allow 1/3 mode width spacing
        peaks = filterPeaksBySeparation(peaks, smoothed, minSeparation, binWidth, maxModes);

        if (peaks.isEmpty()) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        // Sort peaks by height (descending) and take top maxModes
        peaks.sort(Comparator.comparingDouble(Peak::height).reversed());
        int modeCount = Math.min(peaks.size(), maxModes);

        // Check if dip test confirms multimodality
        boolean dipConfirms = dipStatistic > DIP_MULTIMODAL_THRESHOLD;

        // If only one peak, return unimodal
        if (modeCount == 1) {
            return ModeDetectionResult.unimodal(peaks.get(0).location, dipStatistic);
        }

        // For multiple peaks: trust histogram if peaks are prominent enough,
        // even if dip test doesn't confirm. This handles cases where L2 normalization
        // distorts the mode separation but histogram peaks are still visible.
        // Use adaptive threshold - for many modes, secondary peaks may be smaller
        double secondaryPeakThreshold = computeSecondaryPeakThreshold(maxModes);
        boolean peaksAreProminent = peaks.size() >= 2 &&
            peaks.get(1).height >= peaks.get(0).height * secondaryPeakThreshold;

        // Multimodality is confirmed if:
        // 1. Dip test confirms (strong statistical evidence), OR
        // 2. Peaks are prominent AND dip is at least moderate (> 0.03)
        // The dip > 0.03 requirement prevents uniform data from being classified as multimodal
        boolean multimodalConfirmed = dipConfirms || (peaksAreProminent && dipStatistic > 0.03);

        if (!multimodalConfirmed) {
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
     * Computes adaptive bin count based on data size and expected mode count.
     *
     * <p>Uses Scott's rule as a base, then ensures sufficient bins per mode.
     * For high mode counts (4-10), we need more bins to resolve closely-spaced peaks.
     *
     * @param n the number of data points
     * @param maxModes the maximum expected number of modes
     * @param sortedValues the sorted data values
     * @return the adaptive bin count
     */
    private static int computeAdaptiveBinCount(int n, int maxModes, float[] sortedValues) {
        // Scott's rule: h = 3.49 * σ * n^(-1/3)
        // Gives bins = (max - min) / h
        double mean = 0;
        for (float v : sortedValues) mean += v;
        mean /= n;

        double variance = 0;
        for (float v : sortedValues) {
            double diff = v - mean;
            variance += diff * diff;
        }
        variance /= n;
        double stdDev = Math.sqrt(variance);

        double range = sortedValues[n - 1] - sortedValues[0];
        if (range <= 0 || stdDev <= 0) {
            return 20;  // Fallback for constant data
        }

        // Scott's rule
        double binWidth = 3.49 * stdDev * Math.pow(n, -1.0 / 3.0);
        int scottBins = (int) Math.ceil(range / binWidth);

        // Freedman-Diaconis rule for comparison (uses IQR)
        int q1Idx = n / 4;
        int q3Idx = 3 * n / 4;
        double iqr = sortedValues[q3Idx] - sortedValues[q1Idx];
        int fdBins = scottBins;
        if (iqr > 0) {
            double fdWidth = 2 * iqr * Math.pow(n, -1.0 / 3.0);
            fdBins = (int) Math.ceil(range / fdWidth);
        }

        // Take the larger of the two rules
        int baseBins = Math.max(scottBins, fdBins);

        // Ensure minimum bins per mode for adequate resolution
        int minBinsForModes = maxModes * MIN_BINS_PER_MODE;

        // Take the maximum, but cap at a reasonable limit
        int adaptiveBins = Math.max(baseBins, minBinsForModes);
        return Math.max(15, Math.min(adaptiveBins, 200));
    }

    /**
     * Computes adaptive smoothing bandwidth based on mode count.
     *
     * <p>For higher mode counts, we reduce smoothing to preserve closely-spaced peaks.
     *
     * @param baseBandwidth the default bandwidth
     * @param maxModes the expected number of modes
     * @param binCount the number of histogram bins
     * @return the adaptive bandwidth
     */
    private static double computeAdaptiveBandwidth(double baseBandwidth, int maxModes, int binCount) {
        // For 2-3 modes, use standard bandwidth
        // For 4-10 modes, reduce bandwidth aggressively to preserve peaks
        if (maxModes <= 3) {
            return baseBandwidth;
        }

        // Reduce bandwidth as mode count increases, but not too aggressively
        // For 10 modes: use ~30% of base bandwidth (balanced approach)
        // Too aggressive reduction leads to over-detection of noise as modes
        double reduction = 1.0 - (maxModes - 3) * 0.10;  // 10% reduction per mode
        reduction = Math.max(0.30, reduction);  // Floor at 30%

        // Also consider bin density - fewer bins means less smoothing needed
        double binsPerMode = (double) binCount / maxModes;
        if (binsPerMode < 5) {
            // Very few bins per mode - minimal smoothing
            reduction *= 0.5;
        }

        return Math.max(0.3, baseBandwidth * reduction);  // Minimum 0.3 (was 0.5)
    }

    /**
     * Computes adaptive prominence threshold based on mode count.
     *
     * <p>For higher mode counts, we lower the threshold since each peak
     * is expected to have less relative height.
     *
     * @param baseProminence the default prominence threshold
     * @param maxModes the expected number of modes
     * @return the adaptive prominence threshold
     */
    private static double computeAdaptiveProminence(double baseProminence, int maxModes) {
        // For 2-3 modes, use standard prominence
        // For 10 equal-weight modes, each peak is ~10% of max, so threshold
        // should be around 3% to allow for variation
        if (maxModes <= 3) {
            return baseProminence;
        }

        // Scale down prominence as mode count increases
        // minProminence = 1 / (maxModes * 3) approximately
        double scaleFactor = 3.0 / maxModes;
        double adaptiveProminence = baseProminence * scaleFactor;

        // Never go below 3% to avoid noise (was 1%)
        // Too low a threshold leads to over-detection of noise as modes
        return Math.max(0.03, adaptiveProminence);
    }

    /**
     * Computes secondary peak threshold based on mode count.
     *
     * <p>For higher mode counts, we accept smaller secondary peaks since
     * modes may have varying weights.
     *
     * @param maxModes the expected number of modes
     * @return the threshold ratio for secondary peaks (relative to max peak)
     */
    private static double computeSecondaryPeakThreshold(int maxModes) {
        // Base: 20% for 2 modes
        // For 10 modes: 5% (any visible peak is significant)
        if (maxModes <= 2) {
            return 0.20;
        }
        if (maxModes <= 4) {
            return 0.15;
        }
        if (maxModes <= 6) {
            return 0.10;
        }
        return 0.05;  // For 7-10 modes
    }

    /**
     * Filters peaks to remove noise by requiring minimum separation and local prominence.
     *
     * <p>Peaks that are too close together are merged, keeping the higher peak.
     * Peaks must also have a significant valley (drop) between them and their neighbors.
     *
     * @param peaks the initial list of peaks
     * @param smoothed the smoothed histogram
     * @param minSeparation minimum distance between peaks in data units
     * @param binWidth the histogram bin width
     * @param maxModes the maximum number of modes expected (affects valley threshold)
     * @return filtered list of peaks
     */
    private static List<Peak> filterPeaksBySeparation(List<Peak> peaks, double[] smoothed,
            double minSeparation, double binWidth, int maxModes) {

        if (peaks.size() <= 1) {
            return peaks;
        }

        // Sort by location for proximity analysis
        List<Peak> sorted = new ArrayList<>(peaks);
        sorted.sort(Comparator.comparingDouble(Peak::location));

        List<Peak> filtered = new ArrayList<>();
        Peak current = sorted.get(0);

        for (int i = 1; i < sorted.size(); i++) {
            Peak next = sorted.get(i);
            double separation = next.location - current.location;

            if (separation < minSeparation) {
                // Too close - keep the higher peak
                if (next.height > current.height) {
                    current = next;
                }
                // else keep current
            } else {
                // Check for significant valley between current and next
                if (hasSignificantValley(current, next, smoothed, binWidth, maxModes)) {
                    filtered.add(current);
                    current = next;
                } else {
                    // No valley - merge by keeping higher peak
                    if (next.height > current.height) {
                        current = next;
                    }
                }
            }
        }
        filtered.add(current);

        return filtered;
    }

    /**
     * Checks if there's a significant valley (dip) between two peaks.
     *
     * <p>A valley is significant if it drops below a threshold of the lower peak height.
     * The threshold is adaptive based on the expected number of modes:
     * <ul>
     *   <li>For 2-3 modes: 90% threshold (require 10% drop)</li>
     *   <li>For 4-6 modes: 95% threshold (require 5% drop)</li>
     *   <li>For 7-10 modes: 98% threshold (require 2% drop)</li>
     * </ul>
     *
     * <p>Higher mode counts require less valley depth because overlapping modes
     * in bounded data often have shallow valleys between them.
     *
     * @param p1 the first peak
     * @param p2 the second peak (must have higher bin index)
     * @param smoothed the smoothed histogram
     * @param binWidth the histogram bin width
     * @param maxModes the maximum expected number of modes
     * @return true if there's a significant valley between the peaks
     */
    private static boolean hasSignificantValley(Peak p1, Peak p2, double[] smoothed,
            double binWidth, int maxModes) {
        int startBin = p1.binIndex;
        int endBin = p2.binIndex;

        if (endBin <= startBin + 1) {
            return false;  // Adjacent bins - no room for a valley
        }

        double minValley = Double.MAX_VALUE;

        // Find the minimum value between the two peaks
        for (int i = startBin + 1; i < endBin && i < smoothed.length; i++) {
            if (smoothed[i] < minValley) {
                minValley = smoothed[i];
            }
        }

        // Compute adaptive valley threshold based on mode count
        // For high mode counts, accept shallower valleys (higher threshold)
        double valleyRatio = computeAdaptiveValleyThreshold(maxModes);
        double lowerPeakHeight = Math.min(p1.height, p2.height);
        double valleyThreshold = lowerPeakHeight * valleyRatio;

        return minValley < valleyThreshold;
    }

    /**
     * Computes adaptive valley threshold based on expected mode count.
     *
     * <p>Higher mode counts use more lenient thresholds (higher values)
     * because overlapping modes have shallower valleys.
     *
     * @param maxModes the expected number of modes
     * @return the valley threshold as a fraction of peak height (0.90-0.98)
     */
    private static double computeAdaptiveValleyThreshold(int maxModes) {
        if (maxModes <= 3) {
            return 0.90;  // Require 10% drop for low mode counts
        }
        if (maxModes <= 6) {
            return 0.92;  // Require 8% drop for medium mode counts (was 5%)
        }
        // Require 8% drop even for high mode counts (was 2%)
        // Too lenient threshold leads to noise valleys being counted as mode separators
        return 0.92;
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

    /**
     * Estimates mode locations from gap analysis when peak detection fails.
     *
     * <p>This is used when the histogram has clear gaps (empty regions) but
     * the smoothed histogram doesn't show distinct peaks. This can happen
     * when modes are spread across a wide range with gaps between them.
     *
     * @param histogram the raw histogram
     * @param binCenters the bin center locations
     * @param gapAnalysis the gap analysis result
     * @param values the original data values
     * @param dipStatistic the computed dip statistic
     * @return mode detection result based on gap analysis
     */
    private static ModeDetectionResult estimateModesFromGaps(int[] histogram, double[] binCenters,
            GapAnalysis gapAnalysis, float[] values, double dipStatistic) {

        int binCount = histogram.length;
        List<double[]> modeRegions = new ArrayList<>();  // [start, end, peakLocation, weight]

        // Find contiguous non-gap regions
        int regionStart = -1;
        int regionMaxBin = -1;
        int regionMaxCount = 0;
        int regionTotal = 0;

        for (int i = 0; i < binCount; i++) {
            boolean isGap = histogram[i] == 0 || contains(gapAnalysis.gapIndices, i);

            if (!isGap) {
                if (regionStart < 0) {
                    regionStart = i;
                    regionMaxBin = i;
                    regionMaxCount = histogram[i];
                    regionTotal = histogram[i];
                } else {
                    regionTotal += histogram[i];
                    if (histogram[i] > regionMaxCount) {
                        regionMaxCount = histogram[i];
                        regionMaxBin = i;
                    }
                }
            } else if (regionStart >= 0) {
                // End of a region
                modeRegions.add(new double[]{
                    binCenters[regionStart],
                    binCenters[i - 1],
                    binCenters[regionMaxBin],
                    regionTotal
                });
                regionStart = -1;
                regionMaxBin = -1;
                regionMaxCount = 0;
                regionTotal = 0;
            }
        }

        // Handle final region
        if (regionStart >= 0) {
            modeRegions.add(new double[]{
                binCenters[regionStart],
                binCenters[binCount - 1],
                binCenters[regionMaxBin],
                regionTotal
            });
        }

        if (modeRegions.isEmpty()) {
            // No distinct regions found
            double mean = 0;
            for (float v : values) mean += v;
            mean /= values.length;
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        int modeCount = modeRegions.size();
        double[] peakLocations = new double[modeCount];
        double[] peakHeights = new double[modeCount];
        double[] modeWeights = new double[modeCount];

        double totalWeight = 0;
        double maxWeight = 0;
        for (int i = 0; i < modeCount; i++) {
            double[] region = modeRegions.get(i);
            peakLocations[i] = region[2];  // Peak location within region
            modeWeights[i] = region[3];    // Count in region
            totalWeight += modeWeights[i];
            maxWeight = Math.max(maxWeight, modeWeights[i]);
        }

        // Normalize weights
        for (int i = 0; i < modeCount; i++) {
            peakHeights[i] = modeWeights[i] / maxWeight;
            modeWeights[i] /= totalWeight;
        }

        return new ModeDetectionResult(modeCount, peakLocations, peakHeights, modeWeights,
            dipStatistic, true);
    }

    /**
     * Checks if an array contains a value.
     */
    private static boolean contains(int[] array, int value) {
        for (int i : array) {
            if (i == value) return true;
        }
        return false;
    }

    /**
     * Result of gap analysis in the histogram.
     *
     * @param hasSignificantGaps true if there are bins with very low counts
     * @param gapCount number of significant gaps detected
     * @param estimatedModes estimated mode count based on gaps (gaps + 1)
     * @param gapIndices indices of bins that are gaps
     */
    private record GapAnalysis(
        boolean hasSignificantGaps,
        int gapCount,
        int estimatedModes,
        int[] gapIndices
    ) {}

    /**
     * Analyzes the raw histogram for gaps (low-density regions).
     *
     * <p>Gaps in the histogram indicate multimodality that smoothing would hide.
     * A gap is a bin (or sequence of bins) with significantly fewer samples than
     * expected for a uniform distribution.
     *
     * <p>This is particularly important for wide-span multimodal distributions
     * where modes are spread across the range with empty regions between them.
     *
     * @param histogram the raw histogram counts
     * @param totalSamples total number of data points
     * @param binCount number of bins
     * @return gap analysis result
     */
    private static GapAnalysis analyzeGaps(int[] histogram, int totalSamples, int binCount) {
        // Expected count per bin for uniform distribution
        double expectedPerBin = (double) totalSamples / binCount;

        // A bin is a "gap" if it has less than 10% of expected count
        // (or zero for smaller expected counts)
        double gapThreshold = Math.max(1, expectedPerBin * 0.10);

        // Also check for "sparse" bins - less than 30% of expected
        double sparseThreshold = expectedPerBin * 0.30;

        List<Integer> gapBins = new ArrayList<>();
        int consecutiveGapCount = 0;
        int significantGapRegions = 0;

        for (int i = 0; i < binCount; i++) {
            if (histogram[i] < gapThreshold) {
                gapBins.add(i);
                consecutiveGapCount++;
            } else {
                if (consecutiveGapCount >= 2) {
                    // A gap region of 2+ bins is significant
                    significantGapRegions++;
                }
                consecutiveGapCount = 0;
            }
        }
        // Check final run
        if (consecutiveGapCount >= 2) {
            significantGapRegions++;
        }

        // Also count isolated sparse bins as potential mode boundaries
        int sparseBinCount = 0;
        for (int i = 1; i < binCount - 1; i++) {
            if (histogram[i] < sparseThreshold &&
                histogram[i] < histogram[i-1] * 0.5 &&
                histogram[i] < histogram[i+1] * 0.5) {
                // This bin is sparse and lower than both neighbors - valley
                sparseBinCount++;
            }
        }

        boolean hasGaps = significantGapRegions > 0 || sparseBinCount >= 2;
        int estimatedModes = Math.max(1, significantGapRegions + sparseBinCount / 2 + 1);

        return new GapAnalysis(
            hasGaps,
            significantGapRegions + sparseBinCount,
            estimatedModes,
            gapBins.stream().mapToInt(Integer::intValue).toArray()
        );
    }
}
