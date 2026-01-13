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

import io.nosqlbench.vshapes.model.ScalarModel;

import java.util.Arrays;
import java.util.Objects;

/**
 * Abstract base class for parametric distribution fitters.
 *
 * <p>This class provides the common framework for fitting parametric
 * distribution models to observed data. Subclasses implement the
 * parameter estimation logic for their specific distribution type.
 *
 * <h2>Uniform Scoring</h2>
 *
 * <p>All parametric fitters use the same goodness-of-fit scoring:
 * the raw Kolmogorov-Smirnov D-statistic computed via the model's CDF.
 * This ensures fair comparison across distribution types.
 *
 * <h2>Subclass Contract</h2>
 *
 * <p>Subclasses must implement:
 * <ul>
 *   <li>{@link #estimateParameters} - estimate distribution parameters</li>
 *   <li>{@link #getModelType} - return the model type identifier</li>
 * </ul>
 *
 * @see ComponentModelFitter
 * @see BestFitSelector
 */
public abstract class AbstractParametricFitter implements ComponentModelFitter {

    /** Protected constructor for subclasses. */
    protected AbstractParametricFitter() {}

    /**
     * Estimates distribution parameters from observed data.
     *
     * <p>Implementations should use the pre-computed statistics where
     * possible for efficiency, falling back to raw values when needed.
     *
     * @param stats pre-computed dimension statistics
     * @param values the observed values (may be null if fitter doesn't need them)
     * @return the fitted ScalarModel with estimated parameters
     */
    protected abstract ScalarModel estimateParameters(DimensionStatistics stats, float[] values);

    @Override
    public FitResult fit(float[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return fit(stats, values);
    }

    @Override
    public FitResult fit(DimensionStatistics stats, float[] values) {
        Objects.requireNonNull(stats, "stats cannot be null");

        ScalarModel model = estimateParameters(stats, values);
        double goodnessOfFit = computeKSStatistic(model, values);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    /**
     * Computes the Kolmogorov-Smirnov D-statistic for goodness-of-fit.
     *
     * <p>The K-S D-statistic is the maximum absolute difference between
     * the empirical CDF of the observed data and the theoretical CDF
     * of the fitted model:
     *
     * <pre>
     * D = max|F_n(x) - F(x)|
     * </pre>
     *
     * <p>Where F_n is the empirical CDF and F is the model's CDF.
     * Lower values indicate better fit (0 = perfect fit, 1 = worst fit).
     *
     * @param model the fitted model
     * @param values the observed values
     * @return the K-S D-statistic in range [0, 1]
     */
    protected double computeKSStatistic(ScalarModel model, float[] values) {
        if (values == null || values.length == 0) {
            return 0.0;
        }

        // Sort values for empirical CDF computation
        float[] sorted = values.clone();
        Arrays.sort(sorted);

        int n = sorted.length;
        double maxD = 0.0;

        for (int i = 0; i < n; i++) {
            // Empirical CDF at this point: F_n(x) = (i + 1) / n
            double empiricalCdf = (double) (i + 1) / n;

            // Model CDF at this point
            double modelCdf = model.cdf(sorted[i]);

            // Also check the step just before this point
            double empiricalCdfBefore = (double) i / n;

            // Maximum difference at this point
            double d1 = Math.abs(empiricalCdf - modelCdf);
            double d2 = Math.abs(empiricalCdfBefore - modelCdf);

            maxD = Math.max(maxD, Math.max(d1, d2));
        }

        return maxD;
    }
}
