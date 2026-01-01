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

import io.nosqlbench.vshapes.model.UniformComponentModel;

import java.util.Objects;

/**
 * Fits a uniform distribution to observed data.
 *
 * <h2>Algorithm</h2>
 *
 * <p>For a uniform distribution U(a, b), the parameters are estimated as:
 * <ul>
 *   <li>a = min(data) - epsilon (slight extension to avoid boundary issues)</li>
 *   <li>b = max(data) + epsilon</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Uses the Kolmogorov-Smirnov (K-S) test statistic comparing the empirical
 * CDF to the uniform CDF. The K-S statistic measures the maximum deviation
 * between the two CDFs.
 *
 * <h2>When to Use</h2>
 *
 * <p>Uniform distributions are appropriate when:
 * <ul>
 *   <li>Data is bounded within a known range</li>
 *   <li>Values are spread evenly across the range</li>
 *   <li>There's no central tendency (no mean-seeking behavior)</li>
 * </ul>
 *
 * @see UniformComponentModel
 * @see ComponentModelFitter
 */
public final class UniformModelFitter implements ComponentModelFitter {

    private final double boundaryExtension;

    /**
     * Creates a uniform model fitter with default boundary extension.
     */
    public UniformModelFitter() {
        this(0.0);  // No extension by default
    }

    /**
     * Creates a uniform model fitter with specified boundary extension.
     *
     * @param boundaryExtension fraction of range to extend beyond observed min/max
     */
    public UniformModelFitter(double boundaryExtension) {
        if (boundaryExtension < 0 || boundaryExtension > 0.5) {
            throw new IllegalArgumentException("boundaryExtension must be in [0, 0.5]");
        }
        this.boundaryExtension = boundaryExtension;
    }

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

        double min = stats.min();
        double max = stats.max();

        // Apply boundary extension
        double range = max - min;
        if (range <= 0) {
            range = 1.0;  // Handle constant data
        }

        double extension = range * boundaryExtension;
        double lower = min - extension;
        double upper = max + extension;

        UniformComponentModel model = new UniformComponentModel(lower, upper);

        // Compute goodness-of-fit using K-S statistic
        double goodnessOfFit = computeKolmogorovSmirnov(values, lower, upper);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return UniformComponentModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;
    }

    /**
     * Computes the Kolmogorov-Smirnov test statistic for uniform distribution.
     *
     * <p>The K-S statistic is the maximum absolute difference between the
     * empirical CDF and the theoretical uniform CDF.
     *
     * <p>Lower values indicate better fit. Critical values (approximate):
     * <ul>
     *   <li>&lt; 1.36/sqrt(n): acceptable fit (p &gt; 0.05)</li>
     *   <li>&lt; 1.63/sqrt(n): marginal fit (p &gt; 0.01)</li>
     * </ul>
     */
    private double computeKolmogorovSmirnov(float[] values, double lower, double upper) {
        int n = values.length;

        // Sort a copy of the values
        float[] sorted = values.clone();
        java.util.Arrays.sort(sorted);

        double range = upper - lower;
        if (range <= 0) {
            return Double.MAX_VALUE;
        }

        double maxDiff = 0;

        for (int i = 0; i < n; i++) {
            // Empirical CDF at this point
            double empiricalCDF = (i + 1.0) / n;

            // Theoretical uniform CDF
            double theoreticalCDF = (sorted[i] - lower) / range;
            theoreticalCDF = Math.max(0, Math.min(1, theoreticalCDF));

            // Also check just before this point
            double empiricalCDFBefore = (double) i / n;

            double diff1 = Math.abs(empiricalCDF - theoreticalCDF);
            double diff2 = Math.abs(empiricalCDFBefore - theoreticalCDF);

            maxDiff = Math.max(maxDiff, Math.max(diff1, diff2));
        }

        // Return the D statistic scaled by sqrt(n) for comparison
        // This makes it comparable across different sample sizes
        return maxDiff * Math.sqrt(n);
    }
}
