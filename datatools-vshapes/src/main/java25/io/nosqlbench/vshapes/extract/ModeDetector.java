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

import io.nosqlbench.vshapes.ComputeModeSpecies;
import jdk.incubator.vector.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Panama Vector API optimized mode detector for univariate distributions.
 *
 * <h2>Purpose</h2>
 *
 * <p>This Java 25+ implementation uses SIMD operations for computing the
 * Hartigan dip statistic and histogram smoothing, providing speedups
 * on CPUs with AVX2 or AVX-512 support.
 *
 * <h2>SIMD Optimizations</h2>
 *
 * <ul>
 *   <li>Vectorized dip statistic computation (max deviation finding)</li>
 *   <li>SIMD histogram smoothing convolution</li>
 *   <li>Vectorized mean/variance for adaptive bin count</li>
 * </ul>
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
 */
public final class ModeDetector {

    private static final VectorSpecies<Double> DOUBLE_SPECIES = ComputeModeSpecies.doubleSpecies();
    private static final VectorSpecies<Float> FLOAT_SPECIES = ComputeModeSpecies.floatSpecies();
    private static final int DOUBLE_LANES = DOUBLE_SPECIES.length();
    private static final int FLOAT_LANES = FLOAT_SPECIES.length();

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
     * that peaks might be merged.
     *
     * @param values the data to analyze
     * @param maxModes maximum number of modes to detect
     * @return mode detection result
     */
    public static ModeDetectionResult detectAdaptive(float[] values, int maxModes) {
        if (values == null || values.length < 4) {
            return ModeDetectionResult.unimodal(values != null && values.length > 0 ? values[0] : 0, 0);
        }

        float[] sorted = values.clone();
        Arrays.sort(sorted);

        // Start with optimal bin count and iterate if needed
        int baseBins = computeAdaptiveBinCount(values.length, maxModes, sorted);
        int currentBins = baseBins;
        int maxBins = Math.min(baseBins * 4, 400);  // Cap at 4x initial or 400

        ModeDetectionResult bestResult = null;
        int bestModeCount = 0;
        int previousModeCount = -1;
        int stableIterations = 0;

        while (currentBins <= maxBins) {
            ModeDetectionResult result = detectWithBinCount(values, maxModes,
                DEFAULT_PROMINENCE_THRESHOLD, DEFAULT_SMOOTHING_BANDWIDTH, currentBins, sorted);

            // Track the result with highest confirmed mode count
            if (result.isMultimodal() && result.modeCount() > bestModeCount) {
                bestResult = result;
                bestModeCount = result.modeCount();
            } else if (bestResult == null) {
                bestResult = result;
            }

            // Check if mode count has stabilized
            if (result.modeCount() == previousModeCount) {
                stableIterations++;
                if (stableIterations >= 2 && result.modeCount() >= 2) {
                    bestResult = result;
                    bestModeCount = result.modeCount();
                    break;  // Mode count is stable
                }
            } else {
                stableIterations = 0;
                previousModeCount = result.modeCount();
            }

            // Check if we might have merged peaks
            if (!mightHaveMergedPeaks(result, currentBins, maxModes)) {
                break;  // No need for higher resolution
            }

            // Increase resolution by 50%
            currentBins = (int) (currentBins * 1.5);
        }

        return bestResult != null ? bestResult :
            ModeDetectionResult.unimodal(computeMeanSIMD(sorted), computeDipStatisticSIMD(sorted));
    }

    /**
     * Detects if the current result might have merged peaks.
     */
    private static boolean mightHaveMergedPeaks(ModeDetectionResult result, int currentBins, int maxModes) {
        // If we found >= 2 modes and dip is reasonable, accept it
        if (result.modeCount() >= 2 && result.dipStatistic() < 0.12) {
            return false;  // Data is adequately explained
        }

        // More conservative threshold for triggering resolution increase
        int splitThreshold = Math.min(3, Math.max(2, maxModes / 4));

        // Unimodal result with elevated dip suggests possible multimodality
        if (result.modeCount() == 1 && result.dipStatistic() > 0.06) {
            return true;
        }

        // If strong multimodality signal but very few modes found
        if (result.modeCount() < splitThreshold && result.dipStatistic() > 0.10) {
            return true;
        }

        return false;
    }

    /**
     * Detects modes with full control over parameters.
     */
    public static ModeDetectionResult detect(float[] values, int maxModes,
            double prominenceThreshold, double smoothingBandwidth) {

        if (values == null || values.length < 4) {
            return ModeDetectionResult.unimodal(values != null && values.length > 0 ? values[0] : 0, 0);
        }

        float[] sorted = values.clone();
        Arrays.sort(sorted);

        int binCount = computeAdaptiveBinCount(values.length, maxModes, sorted);
        return detectWithBinCount(values, maxModes, prominenceThreshold, smoothingBandwidth, binCount, sorted);
    }

    /**
     * Internal detection with specified bin count.
     */
    private static ModeDetectionResult detectWithBinCount(float[] values, int maxModes,
            double prominenceThreshold, double smoothingBandwidth, int binCount, float[] sorted) {

        double dipStatistic = computeDipStatisticSIMD(sorted);

        double min = sorted[0];
        double max = sorted[sorted.length - 1];
        double range = max - min;
        double binWidth = range / binCount;

        if (range <= 0 || binWidth <= 0) {
            return ModeDetectionResult.unimodal(min, dipStatistic);
        }

        double mean = computeMeanSIMD(sorted);

        // Check for nearly-constant data
        if (range / (Math.abs(mean) + 1e-10) < 1e-6) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        int[] histogram = buildHistogram(values, min, binWidth, binCount);
        double[] binCenters = computeBinCenters(min, binWidth, binCount);

        // Compute adaptive smoothing bandwidth
        double adaptiveBandwidth = computeAdaptiveBandwidth(smoothingBandwidth, maxModes, binCount);
        double[] smoothed = smoothHistogramSIMD(histogram, adaptiveBandwidth);

        // Find peaks with adaptive prominence threshold
        double adaptiveProminence = computeAdaptiveProminence(prominenceThreshold, maxModes);
        List<Peak> peaks = findPeaks(smoothed, binCenters, adaptiveProminence);

        if (peaks.isEmpty()) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        // Filter peaks
        double dataRange = max - min;
        double minSeparation = dataRange / (maxModes * 3.0);
        peaks = filterPeaksBySeparation(peaks, smoothed, minSeparation, binWidth, maxModes);

        if (peaks.isEmpty()) {
            return ModeDetectionResult.unimodal(mean, dipStatistic);
        }

        peaks.sort(Comparator.comparingDouble(Peak::height).reversed());
        int modeCount = Math.min(peaks.size(), maxModes);

        boolean dipConfirms = dipStatistic > DIP_MULTIMODAL_THRESHOLD;

        if (modeCount == 1) {
            return ModeDetectionResult.unimodal(peaks.get(0).location, dipStatistic);
        }

        // Check if peaks are prominent enough
        double secondaryPeakThreshold = computeSecondaryPeakThreshold(maxModes);
        boolean peaksAreProminent = peaks.size() >= 2 &&
            peaks.get(1).height >= peaks.get(0).height * secondaryPeakThreshold;

        boolean multimodalConfirmed = dipConfirms || (peaksAreProminent && dipStatistic > 0.03);

        if (!multimodalConfirmed) {
            return ModeDetectionResult.unimodal(peaks.get(0).location, dipStatistic);
        }

        // Extract top modes
        List<Peak> topPeaks = peaks.subList(0, modeCount);

        // Compute mode weights from histogram
        double[] weights = computeModeWeights(topPeaks, histogram, binWidth);

        // Build result arrays
        double[] peakLocations = new double[modeCount];
        double[] peakHeights = new double[modeCount];
        double maxHeight = topPeaks.get(0).height;
        for (int i = 0; i < modeCount; i++) {
            Peak p = topPeaks.get(i);
            peakLocations[i] = p.location;
            peakHeights[i] = p.height / maxHeight;
        }

        return new ModeDetectionResult(modeCount, peakLocations, peakHeights, weights, dipStatistic, true);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SIMD-OPTIMIZED METHODS
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * SIMD-optimized dip statistic computation.
     *
     * <p>Vectorizes the deviation computation loop for ~4-8x throughput.
     */
    static double computeDipStatisticSIMD(float[] sortedValues) {
        int n = sortedValues.length;
        if (n < 4) return 0.0;

        double min = sortedValues[0];
        double max = sortedValues[n - 1];
        double range = max - min;
        if (range <= 0) return 0.0;

        // Convert to double for precision
        double[] sortedD = new double[n];
        for (int i = 0; i < n; i++) {
            sortedD[i] = sortedValues[i];
        }

        int vectorizedLength = DOUBLE_SPECIES.loopBound(n);

        // Broadcast constants
        DoubleVector vMin = DoubleVector.broadcast(DOUBLE_SPECIES, min);
        DoubleVector vRange = DoubleVector.broadcast(DOUBLE_SPECIES, range);
        DoubleVector vN = DoubleVector.broadcast(DOUBLE_SPECIES, (double) n);
        DoubleVector vMaxDip = DoubleVector.broadcast(DOUBLE_SPECIES, 0.0);

        // Vectorized loop
        int i = 0;
        for (; i < vectorizedLength; i += DOUBLE_LANES) {
            // Load sorted values
            DoubleVector vX = DoubleVector.fromArray(DOUBLE_SPECIES, sortedD, i);

            // Compute expected uniform CDF: (x - min) / range
            DoubleVector vExpectedCdf = vX.sub(vMin).div(vRange);

            // Compute empirical CDF: (i + j + 1) / n
            double[] ecdf = new double[DOUBLE_LANES];
            for (int j = 0; j < DOUBLE_LANES; j++) {
                ecdf[j] = (i + j + 1.0) / n;
            }
            DoubleVector vEcdf = DoubleVector.fromArray(DOUBLE_SPECIES, ecdf, 0);

            // Deviation = |ecdf - expectedCdf|
            DoubleVector vDeviation = vEcdf.sub(vExpectedCdf).abs();
            vMaxDip = vMaxDip.max(vDeviation);
        }

        // Reduce vector max to scalar
        double maxDip = vMaxDip.reduceLanes(VectorOperators.MAX);

        // Handle tail elements
        for (; i < n; i++) {
            double expectedCdf = (sortedValues[i] - min) / range;
            double ecdf = (i + 1.0) / n;
            double deviation = Math.abs(ecdf - expectedCdf);
            maxDip = Math.max(maxDip, deviation);
        }

        return maxDip;
    }

    /**
     * SIMD-optimized mean computation for sorted float array.
     */
    private static double computeMeanSIMD(float[] values) {
        int n = values.length;
        int vectorizedLength = FLOAT_SPECIES.loopBound(n);

        FloatVector vSum = FloatVector.broadcast(FLOAT_SPECIES, 0.0f);

        int i = 0;
        for (; i < vectorizedLength; i += FLOAT_LANES) {
            FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, values, i);
            vSum = vSum.add(v);
        }

        float sum = vSum.reduceLanes(VectorOperators.ADD);

        // Tail
        for (; i < n; i++) {
            sum += values[i];
        }

        return sum / n;
    }

    /**
     * SIMD-optimized histogram smoothing using Gaussian kernel convolution.
     *
     * <p>For small kernels (typical case), optimizes the inner convolution loop.
     */
    private static double[] smoothHistogramSIMD(int[] histogram, double bandwidth) {
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

        // Convert histogram to double for SIMD
        double[] histD = new double[n];
        for (int i = 0; i < n; i++) {
            histD[i] = histogram[i];
        }

        // For small kernels, convolution is memory-bound; SIMD helps with accumulation
        // For each output position, compute convolution sum
        for (int i = 0; i < n; i++) {
            double sum = 0;
            double weightSum = 0;

            int jStart = Math.max(0, i - kernelRadius);
            int jEnd = Math.min(n - 1, i + kernelRadius);

            // SIMD accumulation where possible
            int convLen = jEnd - jStart + 1;
            int vectorizedConv = DOUBLE_SPECIES.loopBound(convLen);

            int j = 0;
            DoubleVector vSum = DoubleVector.broadcast(DOUBLE_SPECIES, 0.0);
            DoubleVector vWeightSum = DoubleVector.broadcast(DOUBLE_SPECIES, 0.0);

            for (; j < vectorizedConv; j += DOUBLE_LANES) {
                int histIdx = jStart + j;
                int kernelIdx = histIdx - i + kernelRadius;

                // Manual gather since indices may be out of bounds for simple load
                double[] histVals = new double[DOUBLE_LANES];
                double[] kernelVals = new double[DOUBLE_LANES];
                for (int k = 0; k < DOUBLE_LANES; k++) {
                    histVals[k] = histD[histIdx + k];
                    kernelVals[k] = kernel[kernelIdx + k];
                }

                DoubleVector vHist = DoubleVector.fromArray(DOUBLE_SPECIES, histVals, 0);
                DoubleVector vKernel = DoubleVector.fromArray(DOUBLE_SPECIES, kernelVals, 0);

                vSum = vHist.fma(vKernel, vSum);
                vWeightSum = vWeightSum.add(vKernel);
            }

            sum = vSum.reduceLanes(VectorOperators.ADD);
            weightSum = vWeightSum.reduceLanes(VectorOperators.ADD);

            // Tail
            for (; j < convLen; j++) {
                int histIdx = jStart + j;
                int kernelIdx = histIdx - i + kernelRadius;
                sum += histD[histIdx] * kernel[kernelIdx];
                weightSum += kernel[kernelIdx];
            }

            smoothed[i] = sum / weightSum;
        }

        return smoothed;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // HELPER METHODS (non-SIMD, delegated from base implementation)
    // ═══════════════════════════════════════════════════════════════════════════

    private static int[] buildHistogram(float[] values, double min, double binWidth, int binCount) {
        int[] histogram = new int[binCount];
        for (float v : values) {
            int bin = (int) ((v - min) / binWidth);
            bin = Math.max(0, Math.min(bin, binCount - 1));
            histogram[bin]++;
        }
        return histogram;
    }

    private static double[] computeBinCenters(double min, double binWidth, int binCount) {
        double[] centers = new double[binCount];
        for (int i = 0; i < binCount; i++) {
            centers[i] = min + (i + 0.5) * binWidth;
        }
        return centers;
    }

    private static List<Peak> findPeaks(double[] smoothed, double[] binCenters, double prominenceThreshold) {
        List<Peak> peaks = new ArrayList<>();
        double maxHeight = 0;
        for (double v : smoothed) maxHeight = Math.max(maxHeight, v);
        if (maxHeight == 0) return peaks;

        double threshold = prominenceThreshold * maxHeight;

        for (int i = 1; i < smoothed.length - 1; i++) {
            if (smoothed[i] > smoothed[i-1] && smoothed[i] > smoothed[i+1] && smoothed[i] >= threshold) {
                peaks.add(new Peak(binCenters[i], smoothed[i], i));
            }
        }
        return peaks;
    }

    private static List<Peak> filterPeaksBySeparation(List<Peak> peaks, double[] smoothed,
            double minSeparation, double binWidth, int maxModes) {

        if (peaks.size() <= 1) return peaks;

        peaks.sort(Comparator.comparingDouble(Peak::height).reversed());
        List<Peak> filtered = new ArrayList<>();
        filtered.add(peaks.get(0));

        for (int i = 1; i < peaks.size() && filtered.size() < maxModes; i++) {
            Peak candidate = peaks.get(i);
            boolean tooClose = false;

            for (Peak existing : filtered) {
                if (Math.abs(candidate.location - existing.location) < minSeparation) {
                    tooClose = true;
                    break;
                }
            }

            if (!tooClose) {
                filtered.add(candidate);
            }
        }

        return filtered;
    }

    private static double[] computeModeWeights(List<Peak> peaks, int[] histogram, double binWidth) {
        int modeCount = peaks.size();
        double[] weights = new double[modeCount];
        int totalCount = 0;
        for (int count : histogram) totalCount += count;

        // Assign each bin to nearest peak
        int[] binAssignments = new int[histogram.length];
        for (int i = 0; i < histogram.length; i++) {
            double binCenter = i * binWidth;  // Approximate
            double minDist = Double.MAX_VALUE;
            int nearest = 0;
            for (int p = 0; p < modeCount; p++) {
                double dist = Math.abs(binCenter - peaks.get(p).location);
                if (dist < minDist) {
                    minDist = dist;
                    nearest = p;
                }
            }
            binAssignments[i] = nearest;
        }

        // Sum counts per mode
        int[] modeCounts = new int[modeCount];
        for (int i = 0; i < histogram.length; i++) {
            modeCounts[binAssignments[i]] += histogram[i];
        }

        // Normalize to weights
        for (int p = 0; p < modeCount; p++) {
            weights[p] = (double) modeCounts[p] / totalCount;
        }

        return weights;
    }

    private static int computeAdaptiveBinCount(int n, int maxModes, float[] sortedValues) {
        // Use SIMD for mean/variance
        double mean = computeMeanSIMD(sortedValues);

        double variance = 0;
        for (float v : sortedValues) {
            double diff = v - mean;
            variance += diff * diff;
        }
        variance /= n;
        double stdDev = Math.sqrt(variance);

        double range = sortedValues[n - 1] - sortedValues[0];
        if (range <= 0 || stdDev <= 0) {
            return 20;
        }

        // Scott's rule
        double binWidth = 3.49 * stdDev * Math.pow(n, -1.0 / 3.0);
        int scottBins = (int) Math.ceil(range / binWidth);

        // Freedman-Diaconis rule
        int q1Idx = n / 4;
        int q3Idx = 3 * n / 4;
        double iqr = sortedValues[q3Idx] - sortedValues[q1Idx];
        int fdBins = scottBins;
        if (iqr > 0) {
            double fdWidth = 2 * iqr * Math.pow(n, -1.0 / 3.0);
            fdBins = (int) Math.ceil(range / fdWidth);
        }

        int baseBins = Math.max(scottBins, fdBins);
        int minBinsForModes = maxModes * MIN_BINS_PER_MODE;
        int adaptiveBins = Math.max(baseBins, minBinsForModes);
        return Math.max(15, Math.min(adaptiveBins, 200));
    }

    private static double computeAdaptiveBandwidth(double baseBandwidth, int maxModes, int binCount) {
        if (maxModes <= 3) return baseBandwidth;

        double reduction = 1.0 - (maxModes - 3) * 0.10;
        reduction = Math.max(0.30, reduction);
        double adaptiveBandwidth = baseBandwidth * reduction;

        double minBandwidth = Math.max(0.5, binCount / 80.0);
        return Math.max(minBandwidth, adaptiveBandwidth);
    }

    private static double computeAdaptiveProminence(double baseProminence, int maxModes) {
        if (maxModes <= 3) return baseProminence;

        double reduction = 1.0 - (maxModes - 3) * 0.12;
        double adaptiveProminence = baseProminence * Math.max(0.3, reduction);
        return Math.max(0.03, adaptiveProminence);
    }

    private static double computeSecondaryPeakThreshold(int maxModes) {
        if (maxModes <= 3) return 0.35;
        if (maxModes <= 6) return 0.25;
        return 0.15;
    }
}
