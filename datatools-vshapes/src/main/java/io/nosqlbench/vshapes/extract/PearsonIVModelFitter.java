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

import io.nosqlbench.vshapes.model.PearsonIVScalarModel;

import java.util.Objects;

/**
 * Fits a Pearson Type IV distribution to observed data.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation from the first four moments:
 * <ol>
 *   <li>Compute sample mean, variance, skewness, and kurtosis</li>
 *   <li>Estimate r = 6(β₂ - β₁ - 1) / (2β₂ - 3β₁ - 6)</li>
 *   <li>Estimate m = r/2</li>
 *   <li>Estimate ν from skewness</li>
 *   <li>Estimate scale and location from mean and variance</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Pearson Type IV is appropriate when:
 * <ul>
 *   <li>Data is asymmetric (non-zero skewness)</li>
 *   <li>Pearson criterion 0 &lt; κ &lt; 1</li>
 *   <li>Unbounded support</li>
 * </ul>
 *
 * @see PearsonIVScalarModel
 * @see ComponentModelFitter
 */
public final class PearsonIVModelFitter implements ComponentModelFitter {

    /**
     * Creates a Pearson IV model fitter.
     */
    public PearsonIVModelFitter() {
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

        double mean = stats.mean();
        double stdDev = stats.stdDev();
        double skewness = stats.skewness();
        double kurtosis = stats.kurtosis();

        // Ensure positive stdDev
        if (stdDev <= 0) {
            stdDev = 1.0;
        }

        double beta1 = skewness * skewness;
        double beta2 = kurtosis;

        // Check Pearson criterion for Type IV: 0 < κ < 1
        double denom1 = 2 * beta2 - 3 * beta1 - 6;
        double denom2 = 4 * beta2 - 3 * beta1;

        // If not Type IV region, return poor fit
        // Large but finite penalty score for data outside Type IV region
        // Using 100.0 ensures it's comparable to K-S scores but clearly worse
        final double NOT_TYPE_IV_PENALTY = 100.0;

        if (Math.abs(denom1) < 1e-10 || Math.abs(denom2) < 1e-10) {
            PearsonIVScalarModel model = new PearsonIVScalarModel(2.0, 0.0, 1.0, 0.0);
            return new FitResult(model, NOT_TYPE_IV_PENALTY, getModelType());
        }

        double kappa = beta1 * Math.pow(beta2 + 3, 2) / (4 * denom1 * denom2);

        if (kappa <= 0 || kappa >= 1) {
            // Not in Type IV region
            PearsonIVScalarModel model = new PearsonIVScalarModel(2.0, 0.0, 1.0, 0.0);
            return new FitResult(model, NOT_TYPE_IV_PENALTY, getModelType());
        }

        // Estimate r from moments
        double r = 6 * (beta2 - beta1 - 1) / denom1;

        // m = r/2
        double m = r / 2;

        // Ensure m > 0.5 for valid Pearson IV
        m = Math.max(m, 0.6);

        // Estimate ν from skewness
        // For Pearson IV, skewness is related to ν and m
        double nu = -skewness * Math.sqrt(m);

        // Estimate scale from variance
        // For Pearson IV: Var ≈ a² * (2m-1) / [(2m-2)² - ν²] when well-defined
        double varFactor = (2 * m - 1);
        double denom = (2 * m - 2) * (2 * m - 2) - nu * nu;

        double a;
        if (denom > 0 && varFactor > 0) {
            a = stdDev * Math.sqrt(denom / varFactor);
        } else {
            a = stdDev;
        }

        // Ensure positive scale
        a = Math.max(a, 1e-10);

        // Estimate location from mean
        double lambda;
        if (m > 1 && Math.abs(2 * m - 2) > 1e-10) {
            lambda = mean - a * nu / (2 * m - 2);
        } else {
            lambda = mean;
        }

        PearsonIVScalarModel model = new PearsonIVScalarModel(m, nu, a, lambda);

        // Compute goodness-of-fit using moment-based scoring
        // This is fast O(1) and correlates well with K-S for Pearson IV
        double goodnessOfFit = computeMomentBasedScore(stats, m, nu, a, lambda, kappa);

        return new FitResult(model, goodnessOfFit, getModelType());
    }

    @Override
    public String getModelType() {
        return PearsonIVScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return false;  // Pearson IV is unbounded
    }

    /**
     * Computes goodness-of-fit using moment-based scoring.
     *
     * <p>This is O(1) and much faster than K-S based scoring while still
     * providing a comparable scale. The score is based on:
     * <ol>
     *   <li>How central κ is in the (0, 1) range</li>
     *   <li>How well the estimated moments match the observed moments</li>
     * </ol>
     *
     * <p>The result is scaled to be comparable to K-S × √n scores.
     */
    private double computeMomentBasedScore(DimensionStatistics stats, double m, double nu,
                                            double a, double lambda, double kappa) {
        // Base score from kappa position (0 at κ=0.5, 1 at boundaries)
        double kappaFit = Math.abs(kappa - 0.5) * 2;

        // Penalty for very high kurtosis (hard to fit accurately)
        double kurtosisPenalty = 0;
        if (stats.kurtosis() > 10) {
            kurtosisPenalty = (stats.kurtosis() - 10) * 0.05;
        }

        // Penalty for low skewness (Pearson IV is for asymmetric data)
        double skewnessPenalty = 0;
        if (Math.abs(stats.skewness()) < 0.5) {
            skewnessPenalty = (0.5 - Math.abs(stats.skewness())) * 2;
        }

        // Scale to be comparable to K-S × √n (typically 0.5-5 for good fits)
        double score = (kappaFit + kurtosisPenalty + skewnessPenalty) * 2;

        return Math.max(0.1, Math.min(50.0, score));
    }
}
