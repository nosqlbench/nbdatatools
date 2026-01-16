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

package io.nosqlbench.vshapes.extract;

import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.GammaScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.StudentTScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;

import java.util.Random;

/// Internal mini round-trip verifier for model fitting quality assessment.
///
/// ## Purpose
///
/// This class performs a quick verification of model fit quality by:
/// 1. Generating synthetic samples from the fitted model
/// 2. Re-fitting the same model type to the synthetic data
/// 3. Comparing parameters to detect parameter instability
///
/// This catches cases where a model appears to fit well (low KS statistic) but
/// has unstable parameters that don't round-trip reliably.
///
/// ## Verification Levels
///
/// Three levels are provided to balance speed vs accuracy:
///
/// | Level | Sample Size | Use Case |
/// |-------|-------------|----------|
/// | FAST | 500 | Quick sanity check |
/// | BALANCED | 1000 | Default - good tradeoff |
/// | THOROUGH | 5000 | High accuracy, production |
///
/// ## Drift Calculation
///
/// Parameter drift is calculated as the relative difference between original
/// and re-fitted parameters, normalized by the original value:
///
/// ```
/// drift = |original - refitted| / max(|original|, epsilon)
/// ```
///
/// For models with multiple parameters, the maximum drift across all parameters
/// is reported.
///
/// ## Usage
///
/// ```java
/// // Create verifier with balanced level (default)
/// InternalVerifier verifier = new InternalVerifier();
///
/// // Verify a fit result
/// VerificationResult result = verifier.verify(
///     fitResult.model(),
///     originalData,
///     fitter
/// );
///
/// if (!result.passed()) {
///     // Model is unstable - try composite or empirical
///     System.out.println("Drift: " + result.maxDrift() + "%");
/// }
/// ```
///
/// ## Thread Safety
///
/// This class is NOT thread-safe due to internal Random state.
/// Use separate instances for concurrent verification.
///
/// @see AdaptiveModelExtractor
public final class InternalVerifier {

    /// Verification thoroughness levels.
    public enum VerificationLevel {
        /// Quick sanity check (500 samples)
        FAST(500),
        /// Good balance of speed and accuracy (1000 samples)
        BALANCED(1000),
        /// High accuracy for production (5000 samples)
        THOROUGH(5000);

        /// Number of samples to generate for verification
        public final int sampleSize;

        VerificationLevel(int sampleSize) {
            this.sampleSize = sampleSize;
        }
    }

    /// Default drift threshold for pass/fail (0.5%)
    public static final double DEFAULT_DRIFT_THRESHOLD = 0.005;

    /// Epsilon for relative drift calculation to avoid division by zero
    private static final double EPSILON = 1e-10;

    private final VerificationLevel level;
    private final double driftThreshold;
    private final Random random;

    /// Creates an internal verifier with default settings (BALANCED level, 0.5% threshold).
    public InternalVerifier() {
        this(VerificationLevel.BALANCED, DEFAULT_DRIFT_THRESHOLD);
    }

    /// Creates an internal verifier with specified level.
    ///
    /// @param level the verification thoroughness level
    public InternalVerifier(VerificationLevel level) {
        this(level, DEFAULT_DRIFT_THRESHOLD);
    }

    /// Creates an internal verifier with full configuration.
    ///
    /// @param level the verification thoroughness level
    /// @param driftThreshold the maximum allowed parameter drift (0.0 to 1.0)
    public InternalVerifier(VerificationLevel level, double driftThreshold) {
        this.level = level;
        this.driftThreshold = driftThreshold;
        this.random = new Random(42); // Fixed seed for reproducibility
    }

    /// Returns the current verification level.
    public VerificationLevel getLevel() {
        return level;
    }

    /// Returns the drift threshold.
    public double getDriftThreshold() {
        return driftThreshold;
    }

    /// Verifies a fitted model by round-trip testing.
    ///
    /// @param model the fitted model to verify
    /// @param originalData the original data (used for bounds estimation)
    /// @param fitter the fitter to use for re-fitting (should match model type)
    /// @return verification result with pass/fail and drift metrics
    public VerificationResult verify(ScalarModel model, float[] originalData,
                                      ComponentModelFitter fitter) {
        try {
            // Step 1: Generate synthetic samples from the model
            float[] syntheticData = generateSamples(model, level.sampleSize, originalData);

            // Step 2: Re-fit using the same fitter
            ComponentModelFitter.FitResult refitResult = fitter.fit(syntheticData);

            // Step 3: Compare parameters
            ParameterComparison comparison = compareParameters(model, refitResult.model());

            // Step 4: Determine pass/fail
            boolean passed = comparison.maxDrift() <= driftThreshold;

            return new VerificationResult(
                passed,
                comparison.maxDrift(),
                comparison.avgDrift(),
                comparison.details(),
                level,
                refitResult.goodnessOfFit()
            );

        } catch (Exception e) {
            // Verification failed - model is likely problematic
            return new VerificationResult(
                false,
                1.0, // Max drift
                1.0,
                "Verification failed: " + e.getMessage(),
                level,
                Double.MAX_VALUE
            );
        }
    }

    /// Generates synthetic samples from a model.
    private float[] generateSamples(ScalarModel model, int count, float[] originalData) {
        float[] samples = new float[count];

        // Get data bounds for sampling range
        double min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        for (float v : originalData) {
            if (v < min) min = v;
            if (v > max) max = v;
        }

        // Generate samples using inverse CDF (quantile function)
        for (int i = 0; i < count; i++) {
            double u = random.nextDouble();
            samples[i] = (float) sampleFromModel(model, u, min, max);
        }

        return samples;
    }

    /// Samples from a model using inverse CDF.
    private double sampleFromModel(ScalarModel model, double u, double dataMin, double dataMax) {
        // Use bisection to invert CDF
        double lo = dataMin - (dataMax - dataMin);
        double hi = dataMax + (dataMax - dataMin);

        for (int iter = 0; iter < 100; iter++) {
            double mid = (lo + hi) / 2;
            double cdf = evaluateCdf(model, mid, dataMin, dataMax);

            if (Math.abs(cdf - u) < 1e-8) {
                return mid;
            }

            if (cdf < u) {
                lo = mid;
            } else {
                hi = mid;
            }
        }

        return (lo + hi) / 2;
    }

    /// Evaluates the CDF of a model at a given point.
    private double evaluateCdf(ScalarModel model, double x, double dataMin, double dataMax) {
        if (model instanceof NormalScalarModel normal) {
            return normalCdf(x, normal.getMean(), normal.getStdDev());
        } else if (model instanceof UniformScalarModel uniform) {
            return uniformCdf(x, uniform.getLower(), uniform.getUpper());
        } else if (model instanceof BetaScalarModel beta) {
            return betaCdf(x, beta.getAlpha(), beta.getBeta(), beta.getLower(), beta.getUpper());
        } else if (model instanceof GammaScalarModel gamma) {
            return gammaCdf(x, gamma.getShape(), gamma.getScale(), gamma.getLocation());
        } else if (model instanceof StudentTScalarModel studentT) {
            return studentTCdf(x, studentT.getDegreesOfFreedom(), studentT.getLocation(), studentT.getScale());
        } else if (model instanceof CompositeScalarModel composite) {
            return compositeCdf(composite, x, dataMin, dataMax);
        } else {
            // Fallback: uniform approximation
            return uniformCdf(x, dataMin, dataMax);
        }
    }

    /// Compares parameters between original and re-fitted models.
    private ParameterComparison compareParameters(ScalarModel original, ScalarModel refitted) {
        // Handle type mismatch
        if (!original.getClass().equals(refitted.getClass())) {
            return new ParameterComparison(1.0, 1.0, "Type mismatch: " +
                original.getModelType() + " vs " + refitted.getModelType());
        }

        if (original instanceof NormalScalarModel origNormal &&
            refitted instanceof NormalScalarModel refitNormal) {
            double meanDrift = relativeDrift(origNormal.getMean(), refitNormal.getMean());
            double stdDrift = relativeDrift(origNormal.getStdDev(), refitNormal.getStdDev());
            double maxDrift = Math.max(meanDrift, stdDrift);
            double avgDrift = (meanDrift + stdDrift) / 2;
            return new ParameterComparison(maxDrift, avgDrift,
                String.format("mean: %.4f→%.4f (%.2f%%), std: %.4f→%.4f (%.2f%%)",
                    origNormal.getMean(), refitNormal.getMean(), meanDrift * 100,
                    origNormal.getStdDev(), refitNormal.getStdDev(), stdDrift * 100));
        }

        if (original instanceof UniformScalarModel origUniform &&
            refitted instanceof UniformScalarModel refitUniform) {
            double lowerDrift = relativeDrift(origUniform.getLower(), refitUniform.getLower());
            double upperDrift = relativeDrift(origUniform.getUpper(), refitUniform.getUpper());
            double maxDrift = Math.max(lowerDrift, upperDrift);
            double avgDrift = (lowerDrift + upperDrift) / 2;
            return new ParameterComparison(maxDrift, avgDrift,
                String.format("lower: %.4f→%.4f (%.2f%%), upper: %.4f→%.4f (%.2f%%)",
                    origUniform.getLower(), refitUniform.getLower(), lowerDrift * 100,
                    origUniform.getUpper(), refitUniform.getUpper(), upperDrift * 100));
        }

        if (original instanceof BetaScalarModel origBeta &&
            refitted instanceof BetaScalarModel refitBeta) {
            double alphaDrift = relativeDrift(origBeta.getAlpha(), refitBeta.getAlpha());
            double betaDrift = relativeDrift(origBeta.getBeta(), refitBeta.getBeta());
            double maxDrift = Math.max(alphaDrift, betaDrift);
            double avgDrift = (alphaDrift + betaDrift) / 2;
            return new ParameterComparison(maxDrift, avgDrift,
                String.format("alpha: %.4f→%.4f (%.2f%%), beta: %.4f→%.4f (%.2f%%)",
                    origBeta.getAlpha(), refitBeta.getAlpha(), alphaDrift * 100,
                    origBeta.getBeta(), refitBeta.getBeta(), betaDrift * 100));
        }

        if (original instanceof GammaScalarModel origGamma &&
            refitted instanceof GammaScalarModel refitGamma) {
            double shapeDrift = relativeDrift(origGamma.getShape(), refitGamma.getShape());
            double scaleDrift = relativeDrift(origGamma.getScale(), refitGamma.getScale());
            double maxDrift = Math.max(shapeDrift, scaleDrift);
            double avgDrift = (shapeDrift + scaleDrift) / 2;
            return new ParameterComparison(maxDrift, avgDrift,
                String.format("shape: %.4f→%.4f (%.2f%%), scale: %.4f→%.4f (%.2f%%)",
                    origGamma.getShape(), refitGamma.getShape(), shapeDrift * 100,
                    origGamma.getScale(), refitGamma.getScale(), scaleDrift * 100));
        }

        if (original instanceof CompositeScalarModel origComposite &&
            refitted instanceof CompositeScalarModel refitComposite) {
            // For composite models, compare number of components and weights
            int origCount = origComposite.getComponentCount();
            int refitCount = refitComposite.getComponentCount();

            if (origCount != refitCount) {
                return new ParameterComparison(1.0, 1.0,
                    String.format("Component count mismatch: %d vs %d", origCount, refitCount));
            }

            // Compare weights (normalized)
            double[] origWeights = origComposite.getWeights();
            double[] refitWeights = refitComposite.getWeights();
            double maxWeightDrift = 0.0;
            double totalWeightDrift = 0.0;

            for (int i = 0; i < origCount; i++) {
                double drift = Math.abs(origWeights[i] - refitWeights[i]);
                maxWeightDrift = Math.max(maxWeightDrift, drift);
                totalWeightDrift += drift;
            }

            double avgWeightDrift = totalWeightDrift / origCount;
            return new ParameterComparison(maxWeightDrift, avgWeightDrift,
                String.format("Composite %d components, max weight drift: %.2f%%",
                    origCount, maxWeightDrift * 100));
        }

        // Fallback for other model types
        return new ParameterComparison(0.0, 0.0, "Comparison not implemented for " + original.getModelType());
    }

    /// Computes relative drift between two values.
    private double relativeDrift(double original, double refitted) {
        double absOriginal = Math.abs(original);
        if (absOriginal < EPSILON) {
            return Math.abs(refitted) < EPSILON ? 0.0 : 1.0;
        }
        return Math.abs(original - refitted) / absOriginal;
    }

    // CDF implementations

    private double normalCdf(double x, double mean, double stdDev) {
        if (stdDev <= 0) return x >= mean ? 1.0 : 0.0;
        double z = (x - mean) / (stdDev * Math.sqrt(2));
        return 0.5 * (1 + erf(z));
    }

    private double uniformCdf(double x, double lower, double upper) {
        if (x <= lower) return 0.0;
        if (x >= upper) return 1.0;
        return (x - lower) / (upper - lower);
    }

    private double betaCdf(double x, double alpha, double beta, double lower, double upper) {
        if (x <= lower) return 0.0;
        if (x >= upper) return 1.0;
        double z = (x - lower) / (upper - lower);
        return incompleteBeta(z, alpha, beta);
    }

    private double gammaCdf(double x, double shape, double scale, double location) {
        if (x <= location) return 0.0;
        double z = (x - location) / scale;
        return incompleteGamma(shape, z);
    }

    private double studentTCdf(double x, double degreesOfFreedom, double location, double scale) {
        if (scale <= 0) return x >= location ? 1.0 : 0.0;
        double t = (x - location) / scale;
        // Approximation using normal for large df
        if (degreesOfFreedom > 30) {
            return normalCdf(t, 0, 1);
        }
        // Use beta function approximation
        double v = degreesOfFreedom / (degreesOfFreedom + t * t);
        double p = 0.5 * incompleteBeta(v, degreesOfFreedom / 2, 0.5);
        return t >= 0 ? 1 - p : p;
    }

    private double compositeCdf(CompositeScalarModel composite, double x, double dataMin, double dataMax) {
        ScalarModel[] components = composite.getScalarModels();
        double[] weights = composite.getWeights();
        double cdf = 0;
        for (int i = 0; i < components.length; i++) {
            cdf += weights[i] * evaluateCdf(components[i], x, dataMin, dataMax);
        }
        return cdf;
    }

    // Special functions

    private double erf(double x) {
        double sign = x < 0 ? -1 : 1;
        x = Math.abs(x);
        double a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741;
        double a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
        double t = 1.0 / (1.0 + p * x);
        double y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
        return sign * y;
    }

    private double incompleteBeta(double x, double a, double b) {
        if (x <= 0) return 0.0;
        if (x >= 1) return 1.0;
        int steps = 100;
        double sum = 0, dx = x / steps;
        for (int i = 0; i < steps; i++) {
            double t = (i + 0.5) * dx;
            sum += Math.pow(t, a - 1) * Math.pow(1 - t, b - 1) * dx;
        }
        double betaFunc = Math.exp(lgamma(a) + lgamma(b) - lgamma(a + b));
        return sum / betaFunc;
    }

    private double incompleteGamma(double a, double x) {
        if (x <= 0) return 0.0;
        // Series expansion for small x
        double sum = 0, term = 1.0 / a;
        for (int n = 1; n < 100; n++) {
            sum += term;
            term *= x / (a + n);
            if (Math.abs(term) < 1e-10) break;
        }
        return sum * Math.pow(x, a) * Math.exp(-x) / tgamma(a);
    }

    private double lgamma(double x) {
        if (x <= 0) return 0;
        return 0.5 * Math.log(2 * Math.PI / x) + x * (Math.log(x + 1.0 / (12.0 * x - 1.0 / (10.0 * x))) - 1);
    }

    private double tgamma(double x) {
        return Math.exp(lgamma(x));
    }

    /// Internal record for parameter comparison results.
    private record ParameterComparison(double maxDrift, double avgDrift, String details) {}

    /// Result of internal verification.
    ///
    /// @param passed true if parameter drift is within threshold
    /// @param maxDrift maximum parameter drift (0.0 to 1.0, as fraction)
    /// @param avgDrift average parameter drift
    /// @param details human-readable comparison details
    /// @param level the verification level used
    /// @param refitGoodnessOfFit the goodness-of-fit score from re-fitting
    public record VerificationResult(
        boolean passed,
        double maxDrift,
        double avgDrift,
        String details,
        VerificationLevel level,
        double refitGoodnessOfFit
    ) {
        /// Returns the maximum drift as a percentage.
        public double maxDriftPercent() {
            return maxDrift * 100;
        }

        /// Returns the average drift as a percentage.
        public double avgDriftPercent() {
            return avgDrift * 100;
        }

        @Override
        public String toString() {
            return String.format("VerificationResult[%s, maxDrift=%.2f%%, avgDrift=%.2f%%, level=%s]%n  %s",
                passed ? "PASSED" : "FAILED", maxDrift * 100, avgDrift * 100, level, details);
        }
    }
}
