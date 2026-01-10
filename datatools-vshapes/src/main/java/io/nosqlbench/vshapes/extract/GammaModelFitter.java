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

import io.nosqlbench.vshapes.model.GammaScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;

/**
 * Fits a Gamma distribution to observed data - Pearson Type III.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Detect location shift if data has a minimum above 0</li>
 *   <li>Compute sample mean and variance</li>
 *   <li>Estimate shape k = (mean/stdDev)²</li>
 *   <li>Estimate scale θ = variance/mean</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Gamma distribution requires:
 * <ul>
 *   <li>Positive skewness (right-skewed data)</li>
 *   <li>Semi-bounded support [location, +∞)</li>
 *   <li>Positive values (after location adjustment)</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF.
 *
 * @see GammaScalarModel
 * @see AbstractParametricFitter
 */
public final class GammaModelFitter extends AbstractParametricFitter {

    private final boolean detectLocation;

    /**
     * Creates a Gamma model fitter with default settings.
     */
    public GammaModelFitter() {
        this(true);
    }

    /**
     * Creates a Gamma model fitter.
     *
     * @param detectLocation whether to detect location shift from minimum
     */
    public GammaModelFitter(boolean detectLocation) {
        this.detectLocation = detectLocation;
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        // Detect location shift
        double location = 0.0;
        if (detectLocation && stats.min() > 0) {
            // Use a fraction of the minimum as the location parameter
            location = stats.min() * 0.9;
        } else if (detectLocation && stats.min() < 0) {
            // Shift so all values are positive
            location = stats.min() - 0.1 * Math.abs(stats.min());
        }

        // Compute adjusted statistics
        double adjustedMean = stats.mean() - location;
        double variance = stats.variance();

        // Ensure positive mean for gamma
        if (adjustedMean <= 0) {
            adjustedMean = Math.abs(stats.mean()) + 0.01;
        }

        // Ensure positive variance
        if (variance <= 0) {
            variance = adjustedMean * adjustedMean;
        }

        // Method of moments estimation
        // shape (k) = mean² / variance
        // scale (θ) = variance / mean
        double shape = (adjustedMean * adjustedMean) / variance;
        double scale = variance / adjustedMean;

        // Ensure valid parameters
        shape = Math.max(shape, 0.1);
        scale = Math.max(scale, 1e-10);

        return new GammaScalarModel(shape, scale, location);
    }

    @Override
    public String getModelType() {
        return GammaScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;  // Gamma is semi-bounded
    }
}
