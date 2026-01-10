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

import io.nosqlbench.vshapes.model.BetaPrimeScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;

/**
 * Fits a Beta Prime distribution to observed data - Pearson Type VI.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation:
 * <ol>
 *   <li>Compute sample mean and variance</li>
 *   <li>Estimate β from mean and variance relationship</li>
 *   <li>Estimate α from mean: α = mean(β - 1)</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Beta Prime distribution requires:
 * <ul>
 *   <li>Positive values only</li>
 *   <li>Right-skewed data</li>
 *   <li>β &gt; 2 for finite variance</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF.
 *
 * @see BetaPrimeScalarModel
 * @see AbstractParametricFitter
 */
public final class BetaPrimeModelFitter extends AbstractParametricFitter {

    /**
     * Creates a Beta Prime model fitter.
     */
    public BetaPrimeModelFitter() {
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        double mean = stats.mean();
        double variance = stats.variance();

        // Beta prime requires positive values - if mean <= 0, return default model
        // The CDF-based scoring will naturally produce a high (poor) score
        if (mean <= 0) {
            return new BetaPrimeScalarModel(2.0, 4.0);
        }

        // Ensure positive variance
        if (variance <= 0) {
            variance = mean * mean * 0.1;
        }

        // Method of moments for beta prime:
        // Mean = α / (β - 1) for β > 1
        // Variance = α(α + β - 1) / [(β - 1)²(β - 2)] for β > 2
        //
        // Let r = variance / mean² = (α + β - 1) / [(β - 1)(β - 2)]
        // This is a quadratic in β that we solve numerically

        double r = variance / (mean * mean);

        // Initial guess: β ≈ 2 + 1/r (rough approximation)
        double beta = Math.max(2.1, 2 + 1.0 / (r + 0.1));

        // Refine β using Newton-Raphson (a few iterations)
        for (int i = 0; i < 10; i++) {
            double bm1 = beta - 1;
            double bm2 = beta - 2;
            if (bm2 <= 0) {
                beta = 2.1;
                break;
            }

            // α from mean: α = mean * (β - 1)
            double alpha = mean * bm1;

            // Expected variance ratio
            double expectedR = (alpha + beta - 1) / (bm1 * bm2);

            // Derivative of expectedR with respect to beta
            double deriv = -(alpha + beta - 1) * (2 * beta - 3) / (bm1 * bm1 * bm2 * bm2)
                           + 1.0 / (bm1 * bm2);

            if (Math.abs(deriv) < 1e-10) break;

            double newBeta = beta - (expectedR - r) / deriv;
            if (newBeta <= 2) newBeta = 2.1;
            if (Math.abs(newBeta - beta) < 1e-6) break;
            beta = newBeta;
        }

        // Ensure valid β
        beta = Math.max(beta, 2.1);

        // Compute α from mean
        double alpha = mean * (beta - 1);
        alpha = Math.max(alpha, 0.1);

        return new BetaPrimeScalarModel(alpha, beta);
    }

    @Override
    public String getModelType() {
        return BetaPrimeScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;  // Semi-bounded (0, +∞)
    }
}
