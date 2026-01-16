package io.nosqlbench.vshapes.model;

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

import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Composite (mixture) scalar model combining multiple distributions with weights.
 *
 * <h2>Purpose</h2>
 *
 * <p>This scalar model combines multiple scalar models into a mixture
 * distribution. Each component has an associated weight that determines the
 * probability of sampling from that component. This is useful for modeling
 * multi-modal distributions.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>CompositeScalarModel is a first-order tensor model (ScalarModel) that
 * represents a mixture of multiple scalar distributions:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Design</h2>
 *
 * <p>This model is a pure data container holding the component models and
 * their weights. It does not compute derived statistics (mean, stdDev, bounds)
 * because that would require assuming all component types support those methods.
 * Instead, the CompositeSampler in virtdata handles sampling logic.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Create a bimodal distribution (two Gaussians)
 * CompositeScalarModel bimodal = CompositeScalarModel.of(
 *     new NormalScalarModel(-2.0, 0.5),  // left mode
 *     new NormalScalarModel(2.0, 0.5)    // right mode
 * );  // equal weights
 *
 * // Create with custom weights
 * CompositeScalarModel weighted = new CompositeScalarModel(
 *     List.of(
 *         new NormalScalarModel(0.0, 1.0),
 *         new UniformScalarModel(-1.0, 1.0)
 *     ),
 *     new double[]{0.7, 0.3}  // 70% Gaussian, 30% Uniform
 * );
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see VectorSpaceModel
 */
@ModelType(CompositeScalarModel.MODEL_TYPE)
public class CompositeScalarModel implements ScalarModel {

    public static final String MODEL_TYPE = "composite";

    @SerializedName("components")
    private final ScalarModel[] components;

    @SerializedName("weights")
    private final double[] weights;

    /**
     * Constructs a composite model with specified components and weights.
     *
     * @param components the scalar models
     * @param weights the weights for each component (will be normalized to sum to 1.0)
     * @throws IllegalArgumentException if arrays have different lengths or weights are negative
     */
    public CompositeScalarModel(List<? extends ScalarModel> components, double[] weights) {
        Objects.requireNonNull(components, "components cannot be null");
        Objects.requireNonNull(weights, "weights cannot be null");
        if (components.isEmpty()) {
            throw new IllegalArgumentException("components cannot be empty");
        }
        if (components.size() != weights.length) {
            throw new IllegalArgumentException("components and weights must have same length");
        }

        // Validate and normalize weights
        double sum = 0;
        for (double w : weights) {
            if (w < 0) {
                throw new IllegalArgumentException("weights must be non-negative");
            }
            sum += w;
        }
        if (sum <= 0) {
            throw new IllegalArgumentException("weights must sum to a positive value");
        }

        this.components = components.toArray(new ScalarModel[0]);
        this.weights = new double[weights.length];
        for (int i = 0; i < weights.length; i++) {
            this.weights[i] = weights[i] / sum;  // Normalize
        }
    }

    /**
     * Creates a composite model with equal weights.
     *
     * @param scalars the scalar models
     * @return a CompositeScalarModel with equal weights
     */
    public static CompositeScalarModel of(ScalarModel... scalars) {
        double[] weights = new double[scalars.length];
        Arrays.fill(weights, 1.0 / scalars.length);
        return new CompositeScalarModel(Arrays.asList(scalars), weights);
    }

    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }

    /**
     * Returns the scalar models.
     * @return a copy of the scalar models array
     */
    public ScalarModel[] getScalarModels() {
        return Arrays.copyOf(components, components.length);
    }

    /**
     * Returns the normalized weights.
     * @return a copy of the weights array (sums to 1.0)
     */
    public double[] getWeights() {
        return Arrays.copyOf(weights, weights.length);
    }

    /**
     * Returns the number of components.
     * @return the component count
     */
    public int getComponentCount() {
        return components.length;
    }

    /**
     * Returns whether this composite has only one component.
     *
     * <p>A "simple" composite is functionally equivalent to its single underlying
     * distribution. This method supports unified handling where all distributions
     * are represented as composites, but single-component composites can be
     * treated specially for display and serialization.
     *
     * @return true if this composite has exactly one component
     */
    public boolean isSimple() {
        return components.length == 1;
    }

    /**
     * Unwraps a simple composite to its underlying model.
     *
     * <p>If this composite has exactly one component, returns that component.
     * Otherwise, returns this composite model. This supports unified processing
     * where all models are composites internally, but single-component cases
     * can be unwrapped for display or serialization.
     *
     * @return the single component if simple, or this composite if multi-component
     */
    public ScalarModel unwrap() {
        return isSimple() ? components[0] : this;
    }

    /**
     * Returns the effective model type for display purposes.
     *
     * <p>For simple (1-component) composites, returns the underlying model's type
     * (e.g., "normal", "beta"). For multi-component composites, returns "composite".
     * This allows unified internal representation while maintaining user-friendly
     * display of single distributions.
     *
     * @return the effective model type string
     */
    public String getEffectiveModelType() {
        return isSimple() ? components[0].getModelType() : MODEL_TYPE;
    }

    /**
     * Creates a simple (1-component) composite wrapping the given model.
     *
     * <p>This factory method supports the unified model representation where
     * all dimension models are composites. A simple composite behaves identically
     * to its underlying model but can be processed uniformly with multi-component
     * composites.
     *
     * @param model the model to wrap
     * @return a 1-component CompositeScalarModel
     */
    public static CompositeScalarModel wrap(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            return composite;  // Already a composite, don't double-wrap
        }
        return new CompositeScalarModel(List.of(model), new double[]{1.0});
    }

    /**
     * Computes the cumulative distribution function (CDF) at a given value.
     *
     * <p>For a mixture model, the CDF is the weighted sum of component CDFs:
     * F(x) = Σ wᵢ Fᵢ(x)
     *
     * <p>The result is clamped to [0, 1] to guard against floating-point
     * accumulation errors in the weighted sum.
     *
     * @param x the value at which to evaluate the CDF
     * @return the cumulative probability P(X ≤ x), in range [0, 1]
     */
    @Override
    public double cdf(double x) {
        double sum = 0;
        for (int i = 0; i < components.length; i++) {
            sum += weights[i] * components[i].cdf(x);
        }
        // Clamp to [0, 1] to guard against floating-point accumulation errors
        return Math.max(0.0, Math.min(1.0, sum));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CompositeScalarModel)) return false;
        CompositeScalarModel that = (CompositeScalarModel) o;
        return Arrays.equals(components, that.components) &&
               Arrays.equals(weights, that.weights);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(components);
        result = 31 * result + Arrays.hashCode(weights);
        return result;
    }

    /**
     * Returns this composite model in canonical (reduced and sorted) form.
     *
     * <p>Canonical form enables deterministic comparison of composite models
     * regardless of the order in which components were originally added.
     * The canonical form applies:
     * <ol>
     *   <li>Model reduction (merging equivalent uniforms, Beta(1,1)→Uniform, etc.)</li>
     *   <li>Sorting by characteristic location in ascending order</li>
     *   <li>Recursive canonicalization of nested composites</li>
     * </ol>
     *
     * <p>Use {@link #toCanonicalForm(double, double)} when data bounds are known
     * (e.g., [-1,1] for normalized vectors) to enable additional reductions.
     *
     * @return the canonical model (may be non-composite if fully reduced)
     * @see CompositeModelReducer
     */
    @Override
    public ScalarModel toCanonicalForm() {
        return toCanonicalForm(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    /**
     * Returns this composite model in canonical form with known data bounds.
     *
     * <p>When data bounds are known (e.g., [-1,1] for normalized vectors),
     * additional reductions are possible such as detecting when multiple
     * uniform components collectively cover the full range uniformly.
     *
     * @param knownLower known lower bound of data range (or -Infinity if unknown)
     * @param knownUpper known upper bound of data range (or +Infinity if unknown)
     * @return the canonical model (may be non-composite if fully reduced)
     */
    public ScalarModel toCanonicalForm(double knownLower, double knownUpper) {
        // Step 1: Apply model reductions
        ScalarModel reduced = CompositeModelReducer.reduce(this, knownLower, knownUpper);

        // If reduced to non-composite, return it
        if (!(reduced instanceof CompositeScalarModel composite)) {
            return reduced;
        }

        // Step 2: Sort by characteristic location
        return sortByLocation(composite);
    }

    /**
     * Sorts composite components by characteristic location.
     */
    private static ScalarModel sortByLocation(CompositeScalarModel composite) {
        ScalarModel[] comps = composite.getScalarModels();
        double[] wts = composite.getWeights();

        if (comps.length <= 1) {
            return composite;
        }

        // Create index array for sorting
        Integer[] indices = new Integer[comps.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }

        // Sort indices by component location
        java.util.Arrays.sort(indices, (a, b) -> {
            double locA = ScalarModelComparator.getCharacteristicLocation(comps[a]);
            double locB = ScalarModelComparator.getCharacteristicLocation(comps[b]);
            return Double.compare(locA, locB);
        });

        // Check if already in canonical order
        boolean alreadyCanonical = true;
        for (int i = 0; i < indices.length; i++) {
            if (indices[i] != i) {
                alreadyCanonical = false;
                break;
            }
        }

        if (alreadyCanonical) {
            // Still need to canonicalize nested composites
            boolean hasNestedComposite = false;
            for (ScalarModel component : comps) {
                if (component instanceof CompositeScalarModel) {
                    hasNestedComposite = true;
                    break;
                }
            }
            if (!hasNestedComposite) {
                return composite;
            }
        }

        // Create sorted arrays
        ScalarModel[] sortedComponents = new ScalarModel[comps.length];
        double[] sortedWeights = new double[wts.length];
        for (int i = 0; i < indices.length; i++) {
            ScalarModel comp = comps[indices[i]];
            sortedComponents[i] = comp.toCanonicalForm();  // Recursively canonicalize
            sortedWeights[i] = wts[indices[i]];
        }

        return new CompositeScalarModel(java.util.List.of(sortedComponents), sortedWeights);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CompositeScalarModel[");
        for (int i = 0; i < components.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(String.format("%.2f*%s", weights[i], components[i].getModelType()));
        }
        sb.append("]");
        return sb.toString();
    }

    // ==================== Moment Computation ====================
    //
    // For mixture distributions, moments are computed from component moments
    // using the law of total expectation and law of total variance.
    //
    // Mean:     μ = Σ wᵢ μᵢ
    // Variance: σ² = Σ wᵢ (σᵢ² + (μᵢ - μ)²)
    //
    // Higher moments follow similar weighted sum formulas.

    /// Computes the mean of this mixture distribution.
    ///
    /// For a mixture with component means μᵢ and weights wᵢ:
    /// `μ = Σ wᵢ μᵢ`
    ///
    /// @return the mixture mean
    public double getMean() {
        double mean = 0.0;
        for (int i = 0; i < components.length; i++) {
            mean += weights[i] * getComponentMean(components[i]);
        }
        return mean;
    }

    /// Computes the variance of this mixture distribution.
    ///
    /// Uses the law of total variance:
    /// `σ² = Σ wᵢ (σᵢ² + (μᵢ - μ)²)`
    ///
    /// This accounts for both within-component variance and between-component variance.
    ///
    /// @return the mixture variance
    public double getVariance() {
        double mixtureMean = getMean();
        double variance = 0.0;

        for (int i = 0; i < components.length; i++) {
            double compMean = getComponentMean(components[i]);
            double compVar = getComponentVariance(components[i]);

            // Law of total variance: E[Var(X|Y)] + Var(E[X|Y])
            // = Σ wᵢ σᵢ² + Σ wᵢ (μᵢ - μ)²
            variance += weights[i] * (compVar + Math.pow(compMean - mixtureMean, 2));
        }

        return variance;
    }

    /// Returns the standard deviation of this mixture distribution.
    ///
    /// @return the mixture standard deviation
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /// Computes the skewness of this mixture distribution.
    ///
    /// For mixtures, skewness depends on:
    /// - Component skewnesses
    /// - Relative positions of component means
    /// - Component variances
    ///
    /// @return the mixture skewness (third standardized moment)
    public double getSkewness() {
        double mixtureMean = getMean();
        double mixtureVar = getVariance();
        double mixtureStd = Math.sqrt(mixtureVar);

        if (mixtureStd < 1e-10) {
            return 0.0;  // Degenerate case
        }

        // Third central moment: E[(X - μ)³]
        // For mixture: Σ wᵢ E[(Xᵢ - μ)³]
        // where E[(Xᵢ - μ)³] = E[(Xᵢ - μᵢ + μᵢ - μ)³]
        // = E[(Xᵢ - μᵢ)³] + 3(μᵢ - μ)·Var(Xᵢ) + (μᵢ - μ)³
        double thirdMoment = 0.0;

        for (int i = 0; i < components.length; i++) {
            double compMean = getComponentMean(components[i]);
            double compVar = getComponentVariance(components[i]);
            double compStd = Math.sqrt(compVar);
            double compSkew = getComponentSkewness(components[i]);

            double meanDiff = compMean - mixtureMean;
            double compThirdCentral = compSkew * compStd * compStd * compStd;

            // E[(Xᵢ - μ)³] = γᵢσᵢ³ + 3(μᵢ-μ)σᵢ² + (μᵢ-μ)³
            double contribution = compThirdCentral
                                + 3 * meanDiff * compVar
                                + Math.pow(meanDiff, 3);

            thirdMoment += weights[i] * contribution;
        }

        return thirdMoment / (mixtureStd * mixtureStd * mixtureStd);
    }

    /// Computes the excess kurtosis of this mixture distribution.
    ///
    /// @return the mixture excess kurtosis (fourth standardized moment minus 3)
    public double getKurtosis() {
        double mixtureMean = getMean();
        double mixtureVar = getVariance();
        double mixtureStd = Math.sqrt(mixtureVar);

        if (mixtureStd < 1e-10) {
            return 0.0;  // Degenerate case
        }

        // Fourth central moment: E[(X - μ)⁴]
        double fourthMoment = 0.0;

        for (int i = 0; i < components.length; i++) {
            double compMean = getComponentMean(components[i]);
            double compVar = getComponentVariance(components[i]);
            double compStd = Math.sqrt(compVar);
            double compKurt = getComponentKurtosis(components[i]);

            double meanDiff = compMean - mixtureMean;

            // Fourth central moment of component (excess kurtosis + 3) * σ⁴
            double compFourthCentral = (compKurt + 3) * Math.pow(compVar, 2);

            // E[(Xᵢ - μ)⁴] expansion (binomial)
            double contribution = compFourthCentral
                                + 4 * meanDiff * getComponentSkewness(components[i]) * Math.pow(compStd, 3)
                                + 6 * meanDiff * meanDiff * compVar
                                + Math.pow(meanDiff, 4);

            fourthMoment += weights[i] * contribution;
        }

        double standardizedFourth = fourthMoment / Math.pow(mixtureVar, 2);
        return standardizedFourth - 3.0;  // Excess kurtosis
    }

    // ==================== Component Moment Extraction ====================

    /// Extracts the mean from a scalar model.
    ///
    /// Supports all built-in scalar model types.
    private static double getComponentMean(ScalarModel model) {
        if (model instanceof NormalScalarModel n) {
            return n.getMean();
        } else if (model instanceof UniformScalarModel u) {
            return u.getMean();
        } else if (model instanceof BetaScalarModel b) {
            return b.getMean();
        } else if (model instanceof StudentTScalarModel t) {
            return t.getMean();
        } else if (model instanceof GammaScalarModel g) {
            return g.getMean();
        } else if (model instanceof EmpiricalScalarModel e) {
            return e.getMean();
        } else if (model instanceof CompositeScalarModel c) {
            return c.getMean();  // Recursive
        } else if (model instanceof InverseGammaScalarModel ig) {
            return ig.getMean();
        } else if (model instanceof BetaPrimeScalarModel bp) {
            return bp.getMean();
        } else if (model instanceof PearsonIVScalarModel p4) {
            return p4.getMean();
        }

        // Fallback: numerical integration using CDF
        return numericalMean(model);
    }

    /// Extracts the variance from a scalar model.
    private static double getComponentVariance(ScalarModel model) {
        if (model instanceof NormalScalarModel n) {
            return n.getStdDev() * n.getStdDev();
        } else if (model instanceof UniformScalarModel u) {
            double range = u.getUpper() - u.getLower();
            return range * range / 12.0;  // Uniform variance formula
        } else if (model instanceof BetaScalarModel b) {
            return b.getVariance();
        } else if (model instanceof StudentTScalarModel t) {
            return t.getVariance();
        } else if (model instanceof GammaScalarModel g) {
            return g.getVariance();
        } else if (model instanceof EmpiricalScalarModel) {
            return numericalVariance(model);  // Use numerical
        } else if (model instanceof CompositeScalarModel c) {
            return c.getVariance();  // Recursive
        } else if (model instanceof InverseGammaScalarModel ig) {
            return ig.getVariance();
        } else if (model instanceof BetaPrimeScalarModel bp) {
            return bp.getVariance();
        }

        return numericalVariance(model);
    }

    /// Extracts the skewness from a scalar model.
    private static double getComponentSkewness(ScalarModel model) {
        if (model instanceof NormalScalarModel) {
            return 0.0;  // Normal is symmetric
        } else if (model instanceof UniformScalarModel) {
            return 0.0;  // Uniform is symmetric
        } else if (model instanceof BetaScalarModel b) {
            return b.getSkewness();
        } else if (model instanceof StudentTScalarModel t) {
            return t.getSkewness();
        } else if (model instanceof GammaScalarModel g) {
            return g.getSkewness();
        } else if (model instanceof CompositeScalarModel c) {
            return c.getSkewness();  // Recursive
        } else if (model instanceof InverseGammaScalarModel ig) {
            return ig.getSkewness();
        } else if (model instanceof BetaPrimeScalarModel bp) {
            return bp.getSkewness();
        }

        return 0.0;  // Default to symmetric
    }

    /// Extracts the excess kurtosis from a scalar model.
    private static double getComponentKurtosis(ScalarModel model) {
        if (model instanceof NormalScalarModel) {
            return 0.0;  // Normal has zero excess kurtosis
        } else if (model instanceof UniformScalarModel) {
            return -1.2;  // Uniform has negative excess kurtosis
        } else if (model instanceof BetaScalarModel b) {
            return b.getKurtosis();
        } else if (model instanceof StudentTScalarModel t) {
            return t.getKurtosis();
        } else if (model instanceof GammaScalarModel g) {
            return g.getKurtosis();
        } else if (model instanceof CompositeScalarModel c) {
            return c.getKurtosis();  // Recursive
        }

        return 0.0;  // Default to mesokurtic
    }

    // ==================== Numerical Moment Computation ====================

    /// Computes mean using numerical integration of inverse CDF.
    private static double numericalMean(ScalarModel model) {
        // Use trapezoidal rule on inverse CDF
        int n = 100;
        double sum = 0.0;
        for (int i = 1; i < n; i++) {
            double p = (double) i / n;
            sum += inverseCdfApprox(model, p);
        }
        return sum / (n - 1);
    }

    /// Computes variance using numerical integration.
    private static double numericalVariance(ScalarModel model) {
        double mean = numericalMean(model);
        int n = 100;
        double sumSq = 0.0;
        for (int i = 1; i < n; i++) {
            double p = (double) i / n;
            double x = inverseCdfApprox(model, p);
            sumSq += (x - mean) * (x - mean);
        }
        return sumSq / (n - 1);
    }

    /// Approximates inverse CDF using bisection.
    private static double inverseCdfApprox(ScalarModel model, double p) {
        double lo = -10.0;
        double hi = 10.0;

        // Expand bounds if needed
        while (model.cdf(lo) > p && lo > -1e6) lo *= 2;
        while (model.cdf(hi) < p && hi < 1e6) hi *= 2;

        // Bisection
        for (int i = 0; i < 50; i++) {
            double mid = (lo + hi) / 2;
            if (model.cdf(mid) < p) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return (lo + hi) / 2;
    }

    // ==================== Moment-Based Equivalence ====================

    /// Checks if this composite is moment-equivalent to another model.
    ///
    /// Two distributions are moment-equivalent if their first four moments
    /// (mean, variance, skewness, kurtosis) are within tolerance.
    ///
    /// This is useful for round-trip verification where different composite
    /// structures may represent the same aggregate distribution.
    ///
    /// @param other the model to compare to
    /// @param tolerance relative tolerance for moment comparison
    /// @return true if moments match within tolerance
    public boolean isMomentEquivalent(ScalarModel other, double tolerance) {
        double thisMean = getMean();
        double thisVar = getVariance();
        double thisSkew = getSkewness();
        double thisKurt = getKurtosis();

        double otherMean, otherVar, otherSkew, otherKurt;

        if (other instanceof CompositeScalarModel c) {
            otherMean = c.getMean();
            otherVar = c.getVariance();
            otherSkew = c.getSkewness();
            otherKurt = c.getKurtosis();
        } else {
            otherMean = getComponentMean(other);
            otherVar = getComponentVariance(other);
            otherSkew = getComponentSkewness(other);
            otherKurt = getComponentKurtosis(other);
        }

        // Compare moments with tolerance
        if (!momentClose(thisMean, otherMean, tolerance)) return false;
        if (!momentClose(thisVar, otherVar, tolerance)) return false;

        // Skewness and kurtosis are less stable, use looser tolerance
        if (!momentClose(thisSkew, otherSkew, tolerance * 2)) return false;
        if (!momentClose(thisKurt, otherKurt, tolerance * 2)) return false;

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
}
