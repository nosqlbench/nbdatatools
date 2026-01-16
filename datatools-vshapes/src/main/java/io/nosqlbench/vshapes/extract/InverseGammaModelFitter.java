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

import io.nosqlbench.vshapes.model.InverseGammaScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;

/**
 * Fits an Inverse Gamma distribution to observed data - Pearson Type V.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Compute sample mean and variance</li>
 *   <li>Estimate shape α from: α = 2 + mean²/variance</li>
 *   <li>Estimate scale β from: β = mean(α - 1)</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Inverse Gamma distribution requires:
 * <ul>
 *   <li>Positive values only</li>
 *   <li>Right-skewed data</li>
 *   <li>α &gt; 2 for finite variance</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF.
 *
 * @see InverseGammaScalarModel
 * @see AbstractParametricFitter
 */
public final class InverseGammaModelFitter extends AbstractParametricFitter {

    /**
     * Creates an Inverse Gamma model fitter.
     */
    public InverseGammaModelFitter() {
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        double mean = stats.mean();
        double variance = stats.variance();

        // Inverse gamma requires positive values - if mean <= 0, return default model
        // The CDF-based scoring will naturally produce a high (poor) score
        if (mean <= 0) {
            return new InverseGammaScalarModel(3.0, 2.0);
        }

        // Ensure positive variance
        if (variance <= 0) {
            variance = mean * mean * 0.1;
        }

        // Method of moments: α = 2 + mean²/variance, β = mean(α - 1)
        double shape = 2 + (mean * mean) / variance;

        // Ensure shape is valid (need α > 2 for finite variance)
        shape = Math.max(shape, 2.1);

        double scale = mean * (shape - 1);

        // Ensure positive scale
        scale = Math.max(scale, 1e-10);

        return new InverseGammaScalarModel(shape, scale);
    }

    @Override
    public String getModelType() {
        return InverseGammaScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;  // Semi-bounded (0, +∞)
    }
}
