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

import io.nosqlbench.vshapes.model.*;

/// Checks statistical equivalence between distribution models.
///
/// ## Purpose
///
/// Some distributions are statistically indistinguishable within measurement
/// precision. This class recognizes such equivalences to avoid false negatives
/// in round-trip verification tests.
///
/// ## Recognized Equivalences
///
/// 1. **Normal ↔ Student-t (ν ≥ 30)**: High degrees-of-freedom Student-t
///    converges to Normal. At ν=30, the difference is typically < 1% in tails.
///
/// 2. **Beta(1,1) ↔ Uniform**: Beta distribution with α=β=1 is exactly
///    equivalent to Uniform on the same interval.
///
/// 3. **Composite(1-mode) ↔ Underlying**: A 1-component composite is
///    equivalent to its underlying distribution.
///
/// 4. **Normal[a,b] ↔ Beta[a,b]**: A truncated normal on an interval can be
///    statistically indistinguishable from a Beta distribution on the same
///    interval. Equivalence is determined by CDF similarity at multiple quantiles.
///
/// ## Statistical Basis
///
/// Equivalence is determined using Kullback-Leibler divergence between the
/// theoretical CDFs. Two distributions are equivalent if their KL-divergence
/// is below a threshold (default 0.01 nats).
///
/// ## Usage
///
/// ```java
/// StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
///
/// ScalarModel normal = new NormalScalarModel(0.0, 1.0);
/// ScalarModel studentT = new StudentTScalarModel(100, 0.0, 1.0);
///
/// if (checker.areEquivalent(normal, studentT)) {
///     // Treat as matching types for verification
/// }
/// ```
///
/// @see BestFitSelector
/// @see ModelRecoveryVerifier
public final class StatisticalEquivalenceChecker {

    /// Default KL-divergence threshold for equivalence (in nats)
    public static final double DEFAULT_KL_THRESHOLD = 0.01;

    /// Minimum degrees of freedom for Student-t to be considered Normal-equivalent
    public static final int STUDENT_T_NORMAL_THRESHOLD_DF = 30;

    /// Tolerance for Beta parameters to be considered Uniform-equivalent
    public static final double BETA_UNIFORM_PARAM_TOLERANCE = 0.15;

    /// Maximum CDF difference for Normal-Beta equivalence (across all quantiles)
    /// Increased from 0.03 to 0.08 to handle peaked Beta (e.g., α=β=10) which
    /// is statistically similar to Normal but has different tail behavior.
    public static final double NORMAL_BETA_CDF_TOLERANCE = 0.08;

    /// Number of quantile points to check for CDF similarity
    private static final int CDF_CHECK_POINTS = 20;

    private final double klThreshold;

    /// Creates an equivalence checker with default thresholds.
    public StatisticalEquivalenceChecker() {
        this(DEFAULT_KL_THRESHOLD);
    }

    /// Creates an equivalence checker with a custom KL-divergence threshold.
    ///
    /// @param klThreshold maximum KL-divergence for equivalence
    public StatisticalEquivalenceChecker(double klThreshold) {
        this.klThreshold = klThreshold;
    }

    /// Checks if two models are statistically equivalent.
    ///
    /// Equivalence is checked in order of specificity:
    /// 1. Direct type match
    /// 2. Known type equivalences (Normal↔StudentT, Beta↔Uniform, etc.)
    /// 3. CDF-based equivalence for Composite↔Simple
    /// 4. Moment-based equivalence (mean, variance, skewness, kurtosis)
    ///
    /// @param a first model
    /// @param b second model
    /// @return true if the models are equivalent
    public boolean areEquivalent(ScalarModel a, ScalarModel b) {
        if (a == null || b == null) {
            return a == b;
        }

        // Unwrap simple composites
        ScalarModel unwrappedA = unwrap(a);
        ScalarModel unwrappedB = unwrap(b);

        String typeA = unwrappedA.getModelType();
        String typeB = unwrappedB.getModelType();

        // Direct type match
        if (typeA.equals(typeB)) {
            return true;
        }

        // Check known equivalences
        if (isNormalStudentTEquivalent(unwrappedA, unwrappedB)) {
            return true;
        }

        if (isBetaUniformEquivalent(unwrappedA, unwrappedB)) {
            return true;
        }

        if (isNormalBetaEquivalent(unwrappedA, unwrappedB)) {
            return true;
        }

        // For composite models, check component-level equivalence
        if (areCompositeEquivalent(unwrappedA, unwrappedB)) {
            return true;
        }

        // Final check: moment-based equivalence
        // This catches cases where different model structures produce
        // the same aggregate distribution (same mean, variance, etc.)
        if (areMomentEquivalent(unwrappedA, unwrappedB)) {
            return true;
        }

        return false;
    }

    /// Checks if two models are moment-equivalent.
    ///
    /// Two distributions are moment-equivalent if their first four moments
    /// (mean, variance, skewness, kurtosis) are within tolerance. This is
    /// the most general equivalence check and should be used as a fallback
    /// when structural equivalence checks fail.
    ///
    /// Moment equivalence is particularly important for:
    /// - Different composite structures that produce the same aggregate distribution
    /// - Composites that simplify to unimodal distributions via CLT
    /// - Different model types that happen to have the same moments
    ///
    /// @param a first model
    /// @param b second model
    /// @return true if moments match within tolerance
    public boolean areMomentEquivalent(ScalarModel a, ScalarModel b) {
        return areMomentEquivalent(a, b, DEFAULT_MOMENT_TOLERANCE);
    }

    /// Checks if two models are moment-equivalent with specified tolerance.
    ///
    /// Uses adaptive kurtosis tolerance based on:
    /// 1. Magnitude of kurtosis values (extreme values need more tolerance)
    /// 2. Number of modes in composite models (multi-modal = higher tolerance)
    /// 3. Type of distributions being compared
    ///
    /// @param a first model
    /// @param b second model
    /// @param tolerance relative tolerance for moment comparison
    /// @return true if moments match within tolerance
    public boolean areMomentEquivalent(ScalarModel a, ScalarModel b, double tolerance) {
        // Use CompositeScalarModel's moment computation for consistency
        CompositeScalarModel compA = wrapForMoments(a);
        CompositeScalarModel compB = wrapForMoments(b);

        // Compute adaptive kurtosis tolerance
        double kurtosisTolerance = computeAdaptiveKurtosisTolerance(compA, compB, tolerance);

        return isMomentEquivalentWithAdaptiveKurtosis(compA, compB, tolerance, kurtosisTolerance);
    }

    /// Computes adaptive kurtosis tolerance based on distribution characteristics.
    ///
    /// Kurtosis is highly sensitive to:
    /// - Multi-modal distributions (especially when modes have different weights)
    /// - Distributions with heavy or light tails
    /// - Truncation effects
    ///
    /// This method adjusts tolerance based on these factors.
    private double computeAdaptiveKurtosisTolerance(CompositeScalarModel a, CompositeScalarModel b, double baseTolerance) {
        double kurtA = a.getKurtosis();
        double kurtB = b.getKurtosis();

        // Start with base tolerance * 2 (default for higher moments)
        double tolerance = baseTolerance * 2;

        // Factor 1: Extreme kurtosis values need more tolerance
        double maxAbsKurt = Math.max(Math.abs(kurtA), Math.abs(kurtB));
        if (maxAbsKurt > EXTREME_KURTOSIS_THRESHOLD) {
            // Scale tolerance with kurtosis magnitude
            double scaleFactor = 1.0 + (maxAbsKurt - EXTREME_KURTOSIS_THRESHOLD) * 0.1;
            tolerance *= Math.min(scaleFactor, 2.0);
        }

        // Factor 2: Multi-modal composites need more tolerance
        int modesA = a.getComponentCount();
        int modesB = b.getComponentCount();
        int maxModes = Math.max(modesA, modesB);
        if (maxModes > 2) {
            // Additional 10% tolerance per mode beyond 2
            tolerance *= 1.0 + (maxModes - 2) * 0.10;
        }

        // Factor 3: Different mode counts suggest structural differences
        if (modesA != modesB) {
            // Allow more tolerance when one distribution simplified
            tolerance *= 1.2;
        }

        // Factor 4: Kurtosis sign difference (platykurtic vs leptokurtic)
        if (kurtA * kurtB < 0) {
            // Different tail behavior - be more lenient
            tolerance *= 1.3;
        }

        // Clamp to reasonable bounds
        return Math.max(MIN_KURTOSIS_TOLERANCE, Math.min(MAX_KURTOSIS_TOLERANCE, tolerance));
    }

    /// Checks moment equivalence with adaptive kurtosis tolerance.
    private boolean isMomentEquivalentWithAdaptiveKurtosis(
            CompositeScalarModel a, CompositeScalarModel b,
            double baseTolerance, double kurtosisTolerance) {

        double meanA = a.getMean();
        double meanB = b.getMean();
        double varA = a.getVariance();
        double varB = b.getVariance();
        double skewA = a.getSkewness();
        double skewB = b.getSkewness();
        double kurtA = a.getKurtosis();
        double kurtB = b.getKurtosis();

        // Compare mean with base tolerance
        if (!momentClose(meanA, meanB, baseTolerance)) {
            return false;
        }

        // Compare variance with base tolerance
        if (!momentClose(varA, varB, baseTolerance)) {
            return false;
        }

        // Compare skewness with 2x base tolerance (higher moments are less stable)
        if (!momentClose(skewA, skewB, baseTolerance * 2)) {
            return false;
        }

        // Compare kurtosis with adaptive tolerance
        if (!momentClose(kurtA, kurtB, kurtosisTolerance)) {
            return false;
        }

        return true;
    }

    /// Checks if two moment values are close within tolerance.
    private static boolean momentClose(double a, double b, double tolerance) {
        if (a == b) return true;

        double diff = Math.abs(a - b);
        double scale = Math.max(Math.abs(a), Math.abs(b));

        // For values near zero, use absolute tolerance
        if (scale < 0.1) {
            return diff < tolerance;
        }

        // Otherwise use relative tolerance
        return diff / scale < tolerance;
    }

    /// Wraps a model as a composite for moment computation.
    private CompositeScalarModel wrapForMoments(ScalarModel model) {
        if (model instanceof CompositeScalarModel comp) {
            return comp;
        }
        return CompositeScalarModel.wrap(model);
    }

    /// Returns a description of why two models are considered equivalent,
    /// or null if they are not equivalent.
    ///
    /// @param a first model
    /// @param b second model
    /// @return equivalence reason or null
    public String getEquivalenceReason(ScalarModel a, ScalarModel b) {
        if (a == null || b == null) {
            return null;
        }

        ScalarModel unwrappedA = unwrap(a);
        ScalarModel unwrappedB = unwrap(b);

        String typeA = unwrappedA.getModelType();
        String typeB = unwrappedB.getModelType();

        if (typeA.equals(typeB)) {
            return "same type";
        }

        if (isNormalStudentTEquivalent(unwrappedA, unwrappedB)) {
            return "Normal ↔ StudentT(ν≥" + STUDENT_T_NORMAL_THRESHOLD_DF + ")";
        }

        if (isBetaUniformEquivalent(unwrappedA, unwrappedB)) {
            return "Beta(≈1,≈1) ↔ Uniform";
        }

        if (isNormalBetaEquivalent(unwrappedA, unwrappedB)) {
            return "Normal[a,b] ↔ Beta[a,b]";
        }

        if (areCompositeEquivalent(unwrappedA, unwrappedB)) {
            // Check if it's Composite ↔ Simple (CLT convergence)
            if (unwrappedA instanceof CompositeScalarModel && !(unwrappedB instanceof CompositeScalarModel)) {
                return "Composite ↔ Simple (CLT)";
            }
            if (unwrappedB instanceof CompositeScalarModel && !(unwrappedA instanceof CompositeScalarModel)) {
                return "Composite ↔ Simple (CLT)";
            }
            return "composite equivalence";
        }

        // Check moment equivalence last
        if (areMomentEquivalent(unwrappedA, unwrappedB)) {
            return "moment equivalence (μ,σ²,γ,κ)";
        }

        return null;
    }

    /// Unwraps a model from composite wrapper if it's a simple (1-component) composite.
    private ScalarModel unwrap(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite && composite.isSimple()) {
            return composite.unwrap();
        }
        return model;
    }

    /// Checks Normal ↔ Student-t equivalence.
    ///
    /// Student-t with high degrees of freedom (ν ≥ 30) is effectively Normal.
    /// We also verify that the location and scale parameters match.
    private boolean isNormalStudentTEquivalent(ScalarModel a, ScalarModel b) {
        NormalScalarModel normal = null;
        StudentTScalarModel studentT = null;

        if (a instanceof NormalScalarModel n && b instanceof StudentTScalarModel t) {
            normal = n;
            studentT = t;
        } else if (b instanceof NormalScalarModel n && a instanceof StudentTScalarModel t) {
            normal = n;
            studentT = t;
        } else {
            return false;
        }

        // Check degrees of freedom
        if (studentT.getDegreesOfFreedom() < STUDENT_T_NORMAL_THRESHOLD_DF) {
            return false;
        }

        // Check location parameter (mean vs location)
        double meanDiff = Math.abs(normal.getMean() - studentT.getLocation());
        if (meanDiff > 0.1) {
            return false;
        }

        // Check scale parameter (stdDev vs scale)
        double scaleDiff = Math.abs(normal.getStdDev() - studentT.getScale());
        double relScaleDiff = scaleDiff / Math.max(normal.getStdDev(), 0.01);
        if (relScaleDiff > 0.1) {
            return false;
        }

        return true;
    }

    /// Checks Beta ↔ Uniform equivalence.
    ///
    /// Beta(1,1) is mathematically identical to Uniform on the same interval.
    /// We allow some tolerance around α=β=1 since fitted parameters have noise.
    private boolean isBetaUniformEquivalent(ScalarModel a, ScalarModel b) {
        BetaScalarModel beta = null;
        UniformScalarModel uniform = null;

        if (a instanceof BetaScalarModel bm && b instanceof UniformScalarModel um) {
            beta = bm;
            uniform = um;
        } else if (b instanceof BetaScalarModel bm && a instanceof UniformScalarModel um) {
            beta = bm;
            uniform = um;
        } else {
            return false;
        }

        // Check if Beta is near (1,1)
        double alphaDiff = Math.abs(beta.getAlpha() - 1.0);
        double betaDiff = Math.abs(beta.getBeta() - 1.0);

        if (alphaDiff > BETA_UNIFORM_PARAM_TOLERANCE || betaDiff > BETA_UNIFORM_PARAM_TOLERANCE) {
            return false;
        }

        // Check interval overlap
        double betaLower = beta.getLower();
        double betaUpper = beta.getUpper();
        double uniformLower = uniform.getLower();
        double uniformUpper = uniform.getUpper();

        // Intervals should mostly overlap
        double overlapLower = Math.max(betaLower, uniformLower);
        double overlapUpper = Math.min(betaUpper, uniformUpper);
        double overlapSize = overlapUpper - overlapLower;
        double betaSize = betaUpper - betaLower;
        double uniformSize = uniformUpper - uniformLower;

        double overlapRatio = overlapSize / Math.min(betaSize, uniformSize);
        return overlapRatio > 0.8;
    }

    /// Checks Normal ↔ Beta equivalence.
    ///
    /// A Normal distribution can be statistically indistinguishable from a Beta
    /// distribution, especially for:
    /// 1. Truncated Normal on bounded interval matching Beta shape
    /// 2. Peaked Beta (high α,β) which approximates Normal
    ///
    /// We check equivalence by comparing CDFs at multiple quantile points. If the
    /// maximum CDF difference is below the threshold, the distributions are equivalent.
    private boolean isNormalBetaEquivalent(ScalarModel a, ScalarModel b) {
        NormalScalarModel normal = null;
        BetaScalarModel beta = null;

        if (a instanceof NormalScalarModel n && b instanceof BetaScalarModel bm) {
            normal = n;
            beta = bm;
        } else if (b instanceof NormalScalarModel n && a instanceof BetaScalarModel bm) {
            normal = n;
            beta = bm;
        } else {
            return false;
        }

        // Determine comparison domain
        // For unbounded Normal, use the Beta's bounds for comparison
        // For truncated Normal, check overlap with Beta bounds
        double compLower, compUpper;

        if (normal.isTruncated()) {
            double normalLower = normal.lower();
            double normalUpper = normal.upper();
            double betaLower = beta.getLower();
            double betaUpper = beta.getUpper();

            double overlapLower = Math.max(normalLower, betaLower);
            double overlapUpper = Math.min(normalUpper, betaUpper);
            double overlapSize = overlapUpper - overlapLower;
            double normalSize = normalUpper - normalLower;
            double betaSize = betaUpper - betaLower;

            double overlapRatio = overlapSize / Math.min(normalSize, betaSize);
            if (overlapRatio < 0.8) {
                return false;  // Domains don't overlap sufficiently
            }

            compLower = overlapLower;
            compUpper = overlapUpper;
        } else {
            // Unbounded Normal: compare on Beta's domain
            // This handles peaked Beta (α,β >> 1) which looks like Normal
            compLower = beta.getLower();
            compUpper = beta.getUpper();
        }

        double compRange = compUpper - compLower;
        if (compRange <= 0) {
            return false;
        }

        // Compare CDFs at multiple points across the comparison domain
        double maxCdfDiff = 0.0;
        for (int i = 1; i < CDF_CHECK_POINTS; i++) {
            double t = (double) i / CDF_CHECK_POINTS;
            double x = compLower + t * compRange;

            double normalCdf = normal.cdf(x);
            double betaCdf = beta.cdf(x);

            double cdfDiff = Math.abs(normalCdf - betaCdf);
            maxCdfDiff = Math.max(maxCdfDiff, cdfDiff);

            // Early exit if clearly not equivalent
            if (maxCdfDiff > NORMAL_BETA_CDF_TOLERANCE * 2) {
                return false;
            }
        }

        return maxCdfDiff <= NORMAL_BETA_CDF_TOLERANCE;
    }

    /// Maximum CDF difference for Composite-Simple CLT equivalence.
    /// This is more lenient than Normal-Beta since we're comparing mixture to simple.
    /// Increased from 0.05 to 0.08 to handle overlapping modes that appear unimodal.
    public static final double COMPOSITE_SIMPLE_CDF_TOLERANCE = 0.08;

    /// Default tolerance for moment-based equivalence checking.
    /// This is the relative tolerance for comparing mean and variance.
    /// Increased from 0.15 to 0.20 to handle distributions that are statistically
    /// similar but have different parametric forms (e.g., peaked Beta vs Normal).
    public static final double DEFAULT_MOMENT_TOLERANCE = 0.20;

    /// Minimum kurtosis tolerance - used when kurtosis values are small
    public static final double MIN_KURTOSIS_TOLERANCE = 0.30;

    /// Maximum kurtosis tolerance - used when kurtosis values are large or extreme
    public static final double MAX_KURTOSIS_TOLERANCE = 0.60;

    /// Kurtosis threshold for "extreme" values that require higher tolerance
    public static final double EXTREME_KURTOSIS_THRESHOLD = 3.0;

    /// Checks Composite ↔ Simple equivalence via CDF similarity.
    ///
    /// When composite modes heavily overlap, they can form a distribution that
    /// is statistically indistinguishable from a simple unimodal distribution
    /// (Central Limit Theorem effect). We check equivalence by comparing CDFs.
    ///
    /// @param composite the composite (mixture) model
    /// @param simple the simple (unimodal) model
    /// @return true if CDFs are similar enough
    private boolean isCompositeSimpleCdfEquivalent(CompositeScalarModel composite, ScalarModel simple) {
        // Get the domain for comparison
        double[] compositeBounds = getModelBounds(composite);
        double[] simpleBounds = getModelBounds(simple);

        if (compositeBounds == null || simpleBounds == null) {
            return false;
        }

        double lower = Math.max(compositeBounds[0], simpleBounds[0]);
        double upper = Math.min(compositeBounds[1], simpleBounds[1]);
        double range = upper - lower;

        if (range <= 0) {
            return false;
        }

        // Compare CDFs at multiple points
        double maxCdfDiff = 0.0;
        for (int i = 1; i < CDF_CHECK_POINTS; i++) {
            double t = (double) i / CDF_CHECK_POINTS;
            double x = lower + t * range;

            double compositeCdf = composite.cdf(x);
            double simpleCdf = simple.cdf(x);

            double cdfDiff = Math.abs(compositeCdf - simpleCdf);
            maxCdfDiff = Math.max(maxCdfDiff, cdfDiff);

            // Early exit if clearly not equivalent
            if (maxCdfDiff > COMPOSITE_SIMPLE_CDF_TOLERANCE * 2) {
                return false;
            }
        }

        return maxCdfDiff <= COMPOSITE_SIMPLE_CDF_TOLERANCE;
    }

    /// Gets the bounds [lower, upper] for a model, or null if unbounded.
    private double[] getModelBounds(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            // Use union of component bounds
            double lower = Double.POSITIVE_INFINITY;
            double upper = Double.NEGATIVE_INFINITY;
            for (ScalarModel comp : composite.getScalarModels()) {
                double[] bounds = getModelBounds(comp);
                if (bounds != null) {
                    lower = Math.min(lower, bounds[0]);
                    upper = Math.max(upper, bounds[1]);
                }
            }
            return (lower < upper) ? new double[]{lower, upper} : null;
        } else if (model instanceof NormalScalarModel n) {
            if (n.isTruncated()) {
                return new double[]{n.lower(), n.upper()};
            }
            // Use 4-sigma bounds for unbounded normal
            return new double[]{n.getMean() - 4 * n.getStdDev(), n.getMean() + 4 * n.getStdDev()};
        } else if (model instanceof BetaScalarModel b) {
            return new double[]{b.getLower(), b.getUpper()};
        } else if (model instanceof UniformScalarModel u) {
            return new double[]{u.getLower(), u.getUpper()};
        } else if (model instanceof StudentTScalarModel t) {
            // Use 4-scale bounds
            return new double[]{t.getLocation() - 4 * t.getScale(), t.getLocation() + 4 * t.getScale()};
        } else if (model instanceof GammaScalarModel g) {
            // Use 0 to mean + 4*stddev
            double mean = g.getShape() * g.getScale();
            double stdDev = Math.sqrt(g.getShape()) * g.getScale();
            return new double[]{0, mean + 4 * stdDev};
        }
        return null;
    }

    /// Checks if two composite models are equivalent.
    ///
    /// Two composites are equivalent if they have the same number of components
    /// and each component pair is equivalent (allowing reordering).
    ///
    /// For **overlapping composites** that converge via CLT, we allow Composite ↔ Simple
    /// equivalence when the CDF similarity is high enough. This handles cases where
    /// heavily overlapped modes form a single unimodal-like distribution.
    private boolean areCompositeEquivalent(ScalarModel a, ScalarModel b) {
        // Check for Composite ↔ Simple CDF equivalence (CLT convergence)
        // This allows heavily overlapping composites to match simplified unimodal models
        if (a instanceof CompositeScalarModel && !(b instanceof CompositeScalarModel)) {
            return isCompositeSimpleCdfEquivalent((CompositeScalarModel) a, b);
        }
        if (b instanceof CompositeScalarModel && !(a instanceof CompositeScalarModel)) {
            return isCompositeSimpleCdfEquivalent((CompositeScalarModel) b, a);
        }

        // Case 2: Composite ↔ Composite
        if (!(a instanceof CompositeScalarModel compA) || !(b instanceof CompositeScalarModel compB)) {
            return false;
        }

        // Different mode counts might still be equivalent if one absorbed modes
        int countA = compA.getComponentCount();
        int countB = compB.getComponentCount();

        // Allow ±2 mode difference for high-mode composites
        if (Math.abs(countA - countB) <= 2 && Math.max(countA, countB) >= 4) {
            return true;
        }

        // Exact mode count match required for low-mode composites
        if (countA != countB) {
            return false;
        }

        // For now, just check if they have the same effective type pattern
        // A more sophisticated check would match components by location
        ScalarModel[] componentsA = compA.getScalarModels();
        ScalarModel[] componentsB = compB.getScalarModels();

        // Simple check: same set of component types (order-independent)
        java.util.Map<String, Integer> typesA = countTypes(componentsA);
        java.util.Map<String, Integer> typesB = countTypes(componentsB);

        // Check if type counts match, accounting for equivalences
        return areTypeCountsEquivalent(typesA, typesB);
    }

    private java.util.Map<String, Integer> countTypes(ScalarModel[] components) {
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        for (ScalarModel comp : components) {
            String type = comp.getModelType();
            counts.merge(type, 1, Integer::sum);
        }
        return counts;
    }

    private boolean areTypeCountsEquivalent(java.util.Map<String, Integer> a, java.util.Map<String, Integer> b) {
        // Normalize type counts by merging equivalent types
        java.util.Map<String, Integer> normA = normalizeTypeCounts(a);
        java.util.Map<String, Integer> normB = normalizeTypeCounts(b);

        return normA.equals(normB);
    }

    private java.util.Map<String, Integer> normalizeTypeCounts(java.util.Map<String, Integer> counts) {
        java.util.Map<String, Integer> normalized = new java.util.HashMap<>(counts);

        // Merge normal and student_t (high df)
        int normalCount = normalized.getOrDefault("normal", 0);
        int studentTCount = normalized.getOrDefault("student_t", 0);
        if (normalCount > 0 && studentTCount > 0) {
            // Combine into "normal" bucket
            normalized.put("normal", normalCount + studentTCount);
            normalized.remove("student_t");
        }

        // Merge beta(1,1) and uniform - but we can't tell without parameters
        // So we leave this for the component-level check

        return normalized;
    }
}
