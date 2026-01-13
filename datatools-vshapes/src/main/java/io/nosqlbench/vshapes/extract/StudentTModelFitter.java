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
import io.nosqlbench.vshapes.model.StudentTScalarModel;

/**
 * Fits a Student's t-distribution to observed data - Pearson Type VII.
 *
 * <h2>Algorithm</h2>
 *
 * <p>Uses the method of moments estimation based on excess kurtosis:
 * <ol>
 *   <li>Compute sample mean and standard deviation</li>
 *   <li>Compute sample excess kurtosis</li>
 *   <li>Estimate degrees of freedom: ν = 4 + 6/excessKurtosis</li>
 *   <li>Estimate location μ = sample mean</li>
 *   <li>Estimate scale σ from sample variance: σ² = var * (ν-2)/ν</li>
 * </ol>
 *
 * <h2>Applicability</h2>
 *
 * <p>Student's t-distribution is appropriate when:
 * <ul>
 *   <li>Data is symmetric (low skewness)</li>
 *   <li>Data has heavier tails than normal (kurtosis &gt; 3)</li>
 *   <li>Unbounded support</li>
 * </ul>
 *
 * <h2>Constraints</h2>
 *
 * <p>Degrees of freedom must be &gt; 4 for finite kurtosis.
 * This fitter clamps ν to [4.01, 100].
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF, plus a small
 * penalty for asymmetric data (since Student's t is symmetric).
 *
 * @see StudentTScalarModel
 * @see AbstractParametricFitter
 */
public final class StudentTModelFitter extends AbstractParametricFitter {

    private static final double MIN_DF = 4.01;  // Must be > 4 for finite kurtosis
    private static final double MAX_DF = 100.0; // Above this, use normal

    private final double maxSkewness;

    /**
     * Creates a Student's t model fitter with default settings.
     */
    public StudentTModelFitter() {
        this(0.5);  // Allow some asymmetry tolerance
    }

    /**
     * Creates a Student's t model fitter.
     *
     * @param maxSkewness maximum allowed skewness magnitude (t is symmetric)
     */
    public StudentTModelFitter(double maxSkewness) {
        this.maxSkewness = maxSkewness;
    }

    /**
     * Creates a Student's t model fitter for normalized vectors.
     *
     * <p>Note: Student-t is inherently unbounded, so this returns the standard
     * fitter. For normalized data, the simplicity bias in BestFitSelector
     * should prefer Normal distributions over Student-t when fits are similar.
     *
     * @return a fitter for normalized vector data
     */
    public static StudentTModelFitter forNormalizedVectors() {
        return new StudentTModelFitter();
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        double mean = stats.mean();
        double variance = stats.variance();
        double excessKurtosis = stats.kurtosis() - 3.0;  // Convert to excess kurtosis

        // Estimate degrees of freedom from excess kurtosis
        // For t-distribution: excess kurtosis = 6/(ν-4) for ν > 4
        // Solving: ν = 4 + 6/excessKurtosis
        double degreesOfFreedom;
        if (excessKurtosis <= 0.06) {
            // Very close to normal, use high df
            degreesOfFreedom = MAX_DF;
        } else {
            degreesOfFreedom = 4 + 6.0 / excessKurtosis;
        }

        // Clamp to valid range
        degreesOfFreedom = Math.max(MIN_DF, Math.min(MAX_DF, degreesOfFreedom));

        // Estimate location (mean)
        double location = mean;

        // Estimate scale from variance
        // For t: Var = σ² * ν/(ν-2)
        // So: σ² = Var * (ν-2)/ν
        double scale;
        if (degreesOfFreedom > 2) {
            scale = Math.sqrt(variance * (degreesOfFreedom - 2) / degreesOfFreedom);
        } else {
            scale = Math.sqrt(variance);
        }

        // Ensure positive scale
        scale = Math.max(scale, 1e-10);

        return new StudentTScalarModel(degreesOfFreedom, location, scale);
    }

    @Override
    public FitResult fit(DimensionStatistics stats, float[] values) {
        ScalarModel model = estimateParameters(stats, values);
        double ksScore = computeKSStatistic(model, values);

        // Apply penalty for high df where Student-t approaches Normal.
        // Student-t with df >= 30 is nearly identical to Normal.
        // Prefer Normal (simpler model) when df is high.
        StudentTScalarModel tModel = (StudentTScalarModel) model;
        double df = tModel.getDegreesOfFreedom();

        double adjustment = 0;

        // Penalize high df - defer to Normal as the simpler model
        // The penalty grows as df increases. At high df, Student-t is
        // indistinguishable from Normal, so Normal should win via simplicity.
        // Use aggressive penalty to ensure Normal wins when df is high.
        if (df >= 20) {
            // Strong penalty: at df=20, 50% penalty; at df=100, 80% penalty
            double normalLikeness = (df - 20) / (MAX_DF - 20);  // 0 at df=20, 1 at df=100
            adjustment += (0.50 + 0.30 * normalLikeness) * ksScore;
        }

        // Give a bonus for low df (heavy tails) where Student-t is distinctive
        if (df < 10) {
            double heavyTailed = (10 - df) / (10 - MIN_DF);  // Higher for lower df
            adjustment -= 0.15 * ksScore * heavyTailed;
        }

        // Penalize asymmetric data - Student-t is symmetric
        double skewness = Math.abs(stats.skewness());
        if (skewness > 0.3) {
            adjustment += 0.20 * ksScore * Math.min(1.0, (skewness - 0.3) / 0.5);
        }

        double adjustedScore = Math.max(0, ksScore + adjustment);
        return new FitResult(model, adjustedScore, getModelType());
    }

    @Override
    public String getModelType() {
        return StudentTScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return false;  // t-distribution is unbounded
    }
}
