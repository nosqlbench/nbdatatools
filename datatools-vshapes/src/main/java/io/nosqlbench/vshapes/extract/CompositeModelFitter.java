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

import io.nosqlbench.vshapes.extract.ModeDetector.ModeDetectionResult;
import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Fits a composite (mixture) model to multi-modal data.
 *
 * <h2>Algorithm</h2>
 *
 * <p>This fitter detects multi-modal distributions and fits a mixture of
 * simpler distributions:
 * <ol>
 *   <li>Detect modes using histogram peak analysis</li>
 *   <li>Segment data points by nearest mode</li>
 *   <li>Fit best distribution to each mode's data</li>
 *   <li>Combine into CompositeScalarModel with weights</li>
 * </ol>
 *
 * <h2>When Used</h2>
 *
 * <p>This fitter only produces a result when data is genuinely multi-modal.
 * For unimodal data, it throws an exception so BestFitSelector skips it.
 *
 * <h2>Component Selection</h2>
 *
 * <p>Each mode can be fit with any distribution supported by the component
 * selector (Normal, Beta, Gamma, etc.). This provides flexibility to model
 * heterogeneous mixtures.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Use with bounded data selector for components
 * BestFitSelector componentSelector = BestFitSelector.boundedDataSelector();
 * CompositeModelFitter fitter = new CompositeModelFitter(componentSelector);
 *
 * // Fit bimodal data
 * FitResult result = fitter.fit(bimodalData);
 * CompositeScalarModel model = (CompositeScalarModel) result.model();
 * }</pre>
 *
 * @see ModeDetector
 * @see CompositeScalarModel
 * @see BestFitSelector
 */
public final class CompositeModelFitter implements ComponentModelFitter {

    /** Model type identifier */
    public static final String MODEL_TYPE = "composite";

    /** Default maximum number of components */
    private static final int DEFAULT_MAX_COMPONENTS = 3;

    /** Minimum data points per mode to fit a distribution */
    private static final int MIN_POINTS_PER_MODE = 50;

    /** Number of points to use for CDF validation */
    private static final int CDF_VALIDATION_POINTS = 100;

    /** Maximum K-S distance to accept composite model */
    private static final double MAX_CDF_DEVIATION = 0.05;

    private final BestFitSelector componentSelector;
    private final int maxComponents;
    private final double maxCdfDeviation;

    /**
     * Creates a composite fitter using the default bounded data selector for components.
     */
    public CompositeModelFitter() {
        this(BestFitSelector.boundedDataSelector(), DEFAULT_MAX_COMPONENTS, MAX_CDF_DEVIATION);
    }

    /**
     * Creates a composite fitter with a custom component selector.
     *
     * @param componentSelector selector for fitting each mode's distribution
     */
    public CompositeModelFitter(BestFitSelector componentSelector) {
        this(componentSelector, DEFAULT_MAX_COMPONENTS, MAX_CDF_DEVIATION);
    }

    /**
     * Creates a composite fitter with full configuration.
     *
     * @param componentSelector selector for fitting each mode's distribution
     * @param maxComponents maximum number of components (1-3)
     */
    public CompositeModelFitter(BestFitSelector componentSelector, int maxComponents) {
        this(componentSelector, maxComponents, MAX_CDF_DEVIATION);
    }

    /**
     * Creates a composite fitter with full configuration including CDF validation threshold.
     *
     * @param componentSelector selector for fitting each mode's distribution
     * @param maxComponents maximum number of components (1-3)
     * @param maxCdfDeviation maximum K-S distance between composite and empirical CDF
     */
    public CompositeModelFitter(BestFitSelector componentSelector, int maxComponents, double maxCdfDeviation) {
        this.componentSelector = componentSelector;
        this.maxComponents = Math.max(2, Math.min(maxComponents, 3));
        this.maxCdfDeviation = maxCdfDeviation;
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;
    }

    @Override
    public boolean requiresRawData() {
        return true;
    }

    @Override
    public FitResult fit(float[] values) {
        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return fit(stats, values);
    }

    @Override
    public FitResult fit(DimensionStatistics stats, float[] values) {
        // Step 1: Detect modes
        ModeDetectionResult modeResult = ModeDetector.detect(values, maxComponents);

        // Only proceed if data is multimodal
        if (!modeResult.isMultimodal()) {
            throw new IllegalStateException("Data is not multimodal - composite model not appropriate");
        }

        // Step 2: Segment data by mode
        float[][] modeData = segmentByMode(values, modeResult.peakLocations());

        // Check that each mode has enough data
        for (int i = 0; i < modeData.length; i++) {
            if (modeData[i].length < MIN_POINTS_PER_MODE) {
                throw new IllegalStateException(
                    "Mode " + i + " has insufficient data points: " + modeData[i].length);
            }
        }

        // Step 3: Fit best distribution to each mode
        List<ScalarModel> components = new ArrayList<>();
        double totalLogLikelihood = 0;
        int totalParams = 0;

        for (int i = 0; i < modeData.length; i++) {
            FitResult modeFit = componentSelector.selectBestResult(modeData[i]);
            components.add(modeFit.model());
            totalLogLikelihood += estimateLogLikelihood(modeData[i], modeFit.goodnessOfFit());
            totalParams += estimateParameterCount(modeFit.modelType());
        }

        // Add weights as parameters (k-1 for k components since they sum to 1)
        totalParams += components.size() - 1;

        // Step 4: Create composite model
        CompositeScalarModel composite = new CompositeScalarModel(
            components,
            modeResult.modeWeights()
        );

        // Step 5: CDF sanity check - validate composite matches empirical
        CdfValidationResult cdfValidation = validateCdf(values, composite);
        lastValidationResult = cdfValidation;

        if (!cdfValidation.isValid()) {
            throw new IllegalStateException(String.format(
                "Composite model CDF deviation (%.4f) exceeds threshold (%.4f). %s",
                cdfValidation.maxDeviation(), maxCdfDeviation,
                cdfValidation.formatSummary()));
        }

        // Step 6: Compute goodness-of-fit using BIC
        double bic = computeBIC(values.length, totalLogLikelihood, totalParams);
        double normalizedScore = bic / values.length;

        return new FitResult(composite, normalizedScore, MODEL_TYPE);
    }

    /**
     * Result of CDF validation comparing composite model to empirical distribution.
     *
     * @param isValid true if deviation is within acceptable threshold
     * @param maxDeviation maximum K-S distance between composite and empirical CDF
     * @param avgDeviation average absolute deviation across sample points
     * @param samplePoints number of points used for validation
     * @param threshold the threshold used for validation
     */
    public record CdfValidationResult(
        boolean isValid,
        double maxDeviation,
        double avgDeviation,
        int samplePoints,
        double threshold
    ) {
        /**
         * Returns a formatted summary for user display.
         */
        public String formatSummary() {
            return String.format(
                "CDF Validation: max_dev=%.4f, avg_dev=%.4f, threshold=%.4f, points=%d, %s",
                maxDeviation, avgDeviation, threshold, samplePoints,
                isValid ? "PASSED" : "FAILED");
        }
    }

    /**
     * Returns the last CDF validation result for reporting purposes.
     * This is set after each call to fit().
     */
    private CdfValidationResult lastValidationResult;

    /**
     * Gets the last CDF validation result.
     * @return the validation result, or null if fit() hasn't been called
     */
    public CdfValidationResult getLastValidationResult() {
        return lastValidationResult;
    }

    /**
     * Validates the composite model by comparing its CDF to the empirical CDF.
     *
     * <p>Samples the data range at regular intervals and computes the K-S statistic
     * between the composite model's theoretical CDF and the empirical CDF.
     *
     * @param values original data values
     * @param composite the fitted composite model
     * @return validation result with deviation metrics
     */
    CdfValidationResult validateCdf(float[] values, CompositeScalarModel composite) {
        // Sort values for empirical CDF computation
        float[] sorted = values.clone();
        Arrays.sort(sorted);

        double min = sorted[0];
        double max = sorted[sorted.length - 1];
        double range = max - min;

        if (range <= 0) {
            // All values identical - trivial case
            return new CdfValidationResult(true, 0.0, 0.0, 1, maxCdfDeviation);
        }

        double maxDeviation = 0;
        double totalDeviation = 0;
        int validPoints = 0;

        // Sample CDF at regular intervals across the data range
        for (int i = 0; i <= CDF_VALIDATION_POINTS; i++) {
            double x = min + (range * i) / CDF_VALIDATION_POINTS;

            // Compute empirical CDF at x
            double empiricalCdf = computeEmpiricalCdf(sorted, x);

            // Compute composite model CDF at x
            double compositeCdf = computeCompositeCdf(composite, x, min, max);

            double deviation = Math.abs(empiricalCdf - compositeCdf);
            maxDeviation = Math.max(maxDeviation, deviation);
            totalDeviation += deviation;
            validPoints++;
        }

        double avgDeviation = validPoints > 0 ? totalDeviation / validPoints : 0;
        boolean isValid = maxDeviation <= maxCdfDeviation;

        return new CdfValidationResult(isValid, maxDeviation, avgDeviation, validPoints, maxCdfDeviation);
    }

    /**
     * Computes the empirical CDF at a given point.
     *
     * @param sortedValues sorted data values
     * @param x the point at which to evaluate CDF
     * @return proportion of values <= x
     */
    private double computeEmpiricalCdf(float[] sortedValues, double x) {
        // Binary search for position
        int low = 0, high = sortedValues.length;
        while (low < high) {
            int mid = (low + high) / 2;
            if (sortedValues[mid] <= x) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return (double) low / sortedValues.length;
    }

    /**
     * Computes the CDF of the composite model at a given point.
     *
     * <p>For a mixture: CDF(x) = sum_k w_k * CDF_k(x)
     *
     * @param composite the composite model
     * @param x the point at which to evaluate CDF
     * @param min data minimum (for normalization)
     * @param max data maximum (for normalization)
     * @return the composite CDF value at x
     */
    private double computeCompositeCdf(CompositeScalarModel composite, double x, double min, double max) {
        ScalarModel[] components = composite.getScalarModels();
        double[] weights = composite.getWeights();

        double cdf = 0;
        for (int i = 0; i < components.length; i++) {
            cdf += weights[i] * computeComponentCdf(components[i], x, min, max);
        }

        return cdf;
    }

    /**
     * Computes the CDF for a single scalar model component.
     */
    private double computeComponentCdf(ScalarModel model, double x, double dataMin, double dataMax) {
        if (model instanceof NormalScalarModel normal) {
            return normalCdf(x, normal.getMean(), normal.getStdDev());
        } else if (model instanceof UniformScalarModel uniform) {
            return uniformCdf(x, uniform.getLower(), uniform.getUpper());
        } else if (model instanceof BetaScalarModel beta) {
            return betaCdf(x, beta.getAlpha(), beta.getBeta(), beta.getLower(), beta.getUpper());
        } else {
            // Fallback: approximate with uniform over data range
            return uniformCdf(x, dataMin, dataMax);
        }
    }

    /**
     * Standard normal CDF using error function approximation.
     */
    private double normalCdf(double x, double mean, double stdDev) {
        if (stdDev <= 0) return x >= mean ? 1.0 : 0.0;
        double z = (x - mean) / (stdDev * Math.sqrt(2));
        return 0.5 * (1 + erf(z));
    }

    /**
     * Error function approximation (Abramowitz and Stegun).
     */
    private double erf(double x) {
        double sign = x < 0 ? -1 : 1;
        x = Math.abs(x);

        double a1 = 0.254829592;
        double a2 = -0.284496736;
        double a3 = 1.421413741;
        double a4 = -1.453152027;
        double a5 = 1.061405429;
        double p = 0.3275911;

        double t = 1.0 / (1.0 + p * x);
        double y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);

        return sign * y;
    }

    /**
     * Uniform distribution CDF.
     */
    private double uniformCdf(double x, double lower, double upper) {
        if (x <= lower) return 0.0;
        if (x >= upper) return 1.0;
        return (x - lower) / (upper - lower);
    }

    /**
     * Beta distribution CDF approximation using incomplete beta function.
     */
    private double betaCdf(double x, double alpha, double beta, double lower, double upper) {
        if (x <= lower) return 0.0;
        if (x >= upper) return 1.0;

        // Transform to standard [0,1] interval
        double z = (x - lower) / (upper - lower);

        // Incomplete beta function approximation
        return incompleteBeta(z, alpha, beta);
    }

    /**
     * Regularized incomplete beta function approximation.
     */
    private double incompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0.0;
        if (x >= 1) return 1.0;

        // Use continued fraction expansion for accuracy
        // Simplified approximation using numerical integration
        int steps = 100;
        double sum = 0;
        double dx = x / steps;

        for (int i = 0; i < steps; i++) {
            double t = (i + 0.5) * dx;
            sum += Math.pow(t, a - 1) * Math.pow(1 - t, b - 1) * dx;
        }

        // Normalize by beta function B(a,b)
        double betaFunc = Math.exp(lgamma(a) + lgamma(b) - lgamma(a + b));
        return sum / betaFunc;
    }

    /**
     * Log-gamma function approximation (Stirling).
     */
    private double lgamma(double x) {
        if (x <= 0) return 0;
        return 0.5 * Math.log(2 * Math.PI / x) + x * (Math.log(x + 1.0 / (12.0 * x - 1.0 / (10.0 * x))) - 1);
    }

    /**
     * Segments data points into groups based on nearest peak location.
     *
     * @param values all data points
     * @param peakLocations locations of detected modes
     * @return array of data arrays, one per mode
     */
    private float[][] segmentByMode(float[] values, double[] peakLocations) {
        int k = peakLocations.length;
        List<List<Float>> segments = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            segments.add(new ArrayList<>());
        }

        // Assign each point to nearest mode
        for (float v : values) {
            int nearest = 0;
            double minDist = Math.abs(v - peakLocations[0]);
            for (int i = 1; i < k; i++) {
                double dist = Math.abs(v - peakLocations[i]);
                if (dist < minDist) {
                    minDist = dist;
                    nearest = i;
                }
            }
            segments.get(nearest).add(v);
        }

        // Convert to arrays
        float[][] result = new float[k][];
        for (int i = 0; i < k; i++) {
            List<Float> seg = segments.get(i);
            result[i] = new float[seg.size()];
            for (int j = 0; j < seg.size(); j++) {
                result[i][j] = seg.get(j);
            }
        }

        return result;
    }

    /**
     * Estimates log-likelihood from goodness-of-fit score.
     *
     * <p>Since different fitters use different scoring methods (A-D, K-S, etc.),
     * we approximate the log-likelihood from the goodness-of-fit score.
     */
    private double estimateLogLikelihood(float[] data, double goodnessOfFit) {
        // Approximate: lower GoF score = better fit = higher log-likelihood
        // Use a simple transformation: LL ≈ -n * goodnessOfFit
        return -data.length * goodnessOfFit;
    }

    /**
     * Estimates the number of parameters for a model type.
     */
    private int estimateParameterCount(String modelType) {
        return switch (modelType) {
            case "normal" -> 2;  // mean, stdDev
            case "uniform" -> 2;  // lower, upper
            case "beta" -> 4;  // alpha, beta, lower, upper
            case "gamma" -> 3;  // shape, scale, location
            case "student_t" -> 3;  // df, location, scale
            case "empirical" -> 50;  // approximate: number of bins
            default -> 2;  // default assumption
        };
    }

    /**
     * Computes the Bayesian Information Criterion (BIC).
     *
     * <p>BIC = -2 * log-likelihood + k * log(n)
     * where k = number of parameters, n = sample size
     *
     * @param n sample size
     * @param logLikelihood sum of log-likelihood from all components
     * @param numParams total number of parameters
     * @return BIC value (lower is better)
     */
    private double computeBIC(int n, double logLikelihood, int numParams) {
        return -2 * logLikelihood + numParams * Math.log(n);
    }
}
