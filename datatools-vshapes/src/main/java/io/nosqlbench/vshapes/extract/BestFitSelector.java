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

import io.nosqlbench.vshapes.extract.ComponentModelFitter.FitResult;
import io.nosqlbench.vshapes.model.PearsonType;
import io.nosqlbench.vshapes.model.ScalarModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/// Selects the best-fitting distribution type for each dimension.
///
/// ## Purpose
///
/// Given observed data for a dimension, this class fits multiple distribution
/// types and selects the one with the best goodness-of-fit score. This allows
/// automatic model selection based on the actual data characteristics.
///
/// ## Selection Algorithm
///
/// ```
/// For each dimension:
///   1. Compute dimension statistics
///   2. For each fitter in the candidate list:
///      a. Fit the model to data
///      b. Compute goodness-of-fit score
///   3. Select the model with the lowest (best) score
/// ```
///
/// ## Default Fitters
///
/// By default, the selector considers:
/// 1. Normal distribution (Pearson Type 0)
/// 2. Uniform distribution
/// 3. Empirical (histogram) distribution
///
/// ## Usage
///
/// ```java
/// BestFitSelector selector = BestFitSelector.defaultSelector();
/// ScalarModel bestModel = selector.selectBest(dimensionData);
///
/// // Or get all fit results for comparison
/// List<FitResult> allFits = selector.fitAll(dimensionData);
/// ```
///
/// @see ComponentModelFitter
/// @see DatasetModelExtractor
public final class BestFitSelector {

    private final List<ComponentModelFitter> fitters;
    private final double empiricalPenalty;

    /// Creates a selector with specified fitters.
    ///
    /// @param fitters the list of fitters to consider
    public BestFitSelector(List<ComponentModelFitter> fitters) {
        this(fitters, 0.1);
    }

    /// Creates a selector with specified fitters and empirical penalty.
    ///
    /// @param fitters the list of fitters to consider
    /// @param empiricalPenalty additional penalty for empirical models (to prefer parametric)
    public BestFitSelector(List<ComponentModelFitter> fitters, double empiricalPenalty) {
        Objects.requireNonNull(fitters, "fitters cannot be null");
        if (fitters.isEmpty()) {
            throw new IllegalArgumentException("fitters cannot be empty");
        }
        this.fitters = new ArrayList<>(fitters);
        this.empiricalPenalty = empiricalPenalty;
    }

    /// Creates a selector with the default set of fitters.
    ///
    /// @return a BestFitSelector with Normal, Uniform, and Empirical fitters
    public static BestFitSelector defaultSelector() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new UniformModelFitter(),
            new EmpiricalModelFitter()
        ));
    }

    /// Creates a selector with only parametric fitters (no empirical).
    ///
    /// @return a BestFitSelector with Normal and Uniform fitters only
    public static BestFitSelector parametricOnly() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new UniformModelFitter()
        ));
    }

    /// Creates a selector for bounded/unit interval data.
    ///
    /// This selector is appropriate for data constrained to a finite range
    /// such as [-1, 1] or [0, 1]. It includes only distributions that are
    /// naturally bounded or have meaningful interpretation in bounded ranges:
    ///
    /// - **Normal** - Useful for central tendency, even when truncated
    /// - **Beta** - Flexible bounded distribution, can model many shapes
    /// - **Uniform** - Flat distribution across the range
    ///
    /// Heavy-tailed distributions (Gamma, StudentT, InverseGamma, BetaPrime)
    /// are excluded because:
    /// 1. Their distinguishing features (heavy tails) are truncated in bounded ranges
    /// 2. They become indistinguishable from bounded distributions when constrained
    /// 3. Testing them on bounded data introduces artificial ambiguity
    ///
    /// Use [#fullPearsonSelector()] for unbounded or semi-bounded data
    /// where heavy-tail distributions are appropriate.
    ///
    /// @return a BestFitSelector with Normal, Beta, and Uniform fitters
    public static BestFitSelector boundedDataSelector() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new BetaModelFitter(),
            new UniformModelFitter()
        ));
    }

    /// Creates a selector for bounded data with empirical fallback.
    ///
    /// Same as [#boundedDataSelector()] but includes an empirical
    /// distribution fallback for cases where parametric models don't fit well.
    ///
    /// @return a BestFitSelector with Normal, Beta, Uniform, and Empirical fitters
    public static BestFitSelector boundedDataWithEmpirical() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new BetaModelFitter(),
            new UniformModelFitter(),
            new EmpiricalModelFitter()
        ));
    }

    /// Creates a selector for L2-normalized vector data.
    ///
    /// Normalized vectors have values bounded in [-1, 1], so this creates
    /// fitters with explicit bounds rather than detecting bounds from data.
    /// This is appropriate for embeddings that have been L2-normalized.
    ///
    /// Optimized for round-trip stability by excluding Beta, which oscillates
    /// with Normal for bell-shaped data. Uses Normal, Uniform, and Empirical.
    ///
    /// @return a BestFitSelector configured for normalized vector data
    public static BestFitSelector normalizedVectorSelector() {
        return new BestFitSelector(List.of(
            NormalModelFitter.forNormalizedVectors(),
            UniformModelFitter.forNormalizedVectors(),
            new EmpiricalModelFitter()
        ));
    }

    /// Creates a Pearson distribution system selector.
    ///
    /// Includes fitters for:
    /// - Normal (Type 0)
    /// - Beta (Type I/II)
    /// - Gamma (Type III)
    /// - Student's t (Type VII)
    /// - Uniform (special case)
    ///
    /// Note: Types IV, V, and VI are less common and can be added via custom selector.
    ///
    /// @return a BestFitSelector with Pearson distribution fitters
    public static BestFitSelector pearsonSelector() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new BetaModelFitter(),
            new GammaModelFitter(),
            new StudentTModelFitter(),
            new UniformModelFitter()
        ));
    }

    /// Creates a full Pearson selector including empirical fallback.
    ///
    /// @return a BestFitSelector with all Pearson fitters plus empirical
    public static BestFitSelector pearsonWithEmpirical() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new BetaModelFitter(),
            new GammaModelFitter(),
            new StudentTModelFitter(),
            new UniformModelFitter(),
            new EmpiricalModelFitter()
        ));
    }

    /// Creates a comprehensive Pearson selector with all distribution types.
    ///
    /// Includes fitters for all Pearson types:
    /// - Normal (Type 0)
    /// - Beta (Type I/II)
    /// - Gamma (Type III)
    /// - Pearson IV (Type IV)
    /// - Inverse Gamma (Type V)
    /// - Beta Prime (Type VI)
    /// - Student's t (Type VII)
    /// - Uniform (special case)
    ///
    /// @return a BestFitSelector with all Pearson distribution fitters
    public static BestFitSelector fullPearsonSelector() {
        return new BestFitSelector(List.of(
            new NormalModelFitter(),
            new BetaModelFitter(),
            new GammaModelFitter(),
            new PearsonIVModelFitter(),
            new InverseGammaModelFitter(),
            new BetaPrimeModelFitter(),
            new StudentTModelFitter(),
            new UniformModelFitter()
        ));
    }

    /// Creates a Pearson selector for normalized vector data (bounded [-1, 1]).
    ///
    /// Uses fitters with explicit bounds for L2-normalized embeddings.
    /// This selector is optimized for round-trip stability by using only:
    /// - Normal (truncated) - the expected distribution for embeddings
    /// - Uniform - for flat dimensions
    /// - Empirical - fallback for complex/multimodal cases
    ///
    /// Beta is intentionally excluded because:
    /// 1. For bell-shaped data, Beta with high α/β is indistinguishable from Normal
    /// 2. Beta's flexibility causes round-trip instability (normal→beta oscillation)
    /// 3. Normal is simpler and generates faster
    ///
    /// Heavy-tailed distributions (Gamma, StudentT, etc.) are excluded as
    /// they're meaningless for bounded [-1, 1] data.
    ///
    /// @return a BestFitSelector optimized for normalized vectors
    public static BestFitSelector normalizedPearsonSelector() {
        return new BestFitSelector(List.of(
            NormalModelFitter.forNormalizedVectors(),
            UniformModelFitter.forNormalizedVectors(),
            new EmpiricalModelFitter()
        ));
    }

    /// Creates a Pearson selector with multimodal support for normalized vectors.
    ///
    /// Combines normalized Pearson distributions with composite model fitting.
    /// Optimized for round-trip stability by excluding Beta (which oscillates with Normal).
    ///
    /// For multimodal data, the CompositeModelFitter can combine Normal components,
    /// which is more stable than using Beta for each mode.
    ///
    /// @return a BestFitSelector with normalized Pearson and multimodal support
    public static BestFitSelector normalizedPearsonMultimodalSelector() {
        BestFitSelector componentSelector = normalizedPearsonSelector();
        return new BestFitSelector(
            List.of(
                NormalModelFitter.forNormalizedVectors(),
                UniformModelFitter.forNormalizedVectors(),
                new CompositeModelFitter(componentSelector),
                new EmpiricalModelFitter()
            ),
            0.15
        );
    }

    /// Creates a selector for bounded data with multimodal detection.
    ///
    /// This selector first attempts to detect multi-modal distributions
    /// and fit composite (mixture) models. If data is unimodal, it falls
    /// back to standard bounded distribution fitting.
    ///
    /// Includes fitters for:
    /// - Normal (truncated)
    /// - Beta
    /// - Uniform
    /// - Composite (mixture of above) - NEW
    /// - Empirical (fallback)
    ///
    /// Composite models are preferred over empirical when data is multimodal,
    /// as they provide a more interpretable parametric representation.
    ///
    /// @return a BestFitSelector with multimodal awareness
    public static BestFitSelector multimodalAwareSelector() {
        // Component selector for fitting each mode (no composite or empirical to avoid recursion)
        BestFitSelector componentSelector = boundedDataSelector();
        return new BestFitSelector(
            List.of(
                new NormalModelFitter(),
                new BetaModelFitter(),
                new UniformModelFitter(),
                new CompositeModelFitter(componentSelector),
                new EmpiricalModelFitter()
            ),
            0.15  // Higher penalty for empirical when composite is available
        );
    }

    /// Creates a Pearson selector with multimodal detection.
    ///
    /// Combines the full Pearson distribution family with multimodal
    /// detection. Each mode can be fit with any Pearson distribution type.
    ///
    /// @return a BestFitSelector with full Pearson family and multimodal support
    public static BestFitSelector pearsonMultimodalSelector() {
        // Component selector using full Pearson family for each mode
        BestFitSelector componentSelector = pearsonSelector();
        return new BestFitSelector(
            List.of(
                new NormalModelFitter(),
                new BetaModelFitter(),
                new GammaModelFitter(),
                new StudentTModelFitter(),
                new UniformModelFitter(),
                new CompositeModelFitter(componentSelector),
                new EmpiricalModelFitter()
            ),
            0.15
        );
    }

    /// Creates an adaptive selector with composite fallback and EM clustering.
    ///
    /// This selector is designed for the adaptive extraction pipeline:
    /// 1. Tries full Pearson parametric family first
    /// 2. Uses composite models with EM clustering for multi-modal data
    /// 3. Falls back to empirical only as last resort
    ///
    /// Features:
    /// - Full Pearson distribution types for comprehensive parametric coverage
    /// - Composite fitting with up to 4 components
    /// - EM clustering for accurate overlapping mode detection
    /// - Higher empirical penalty (0.2) to strongly prefer parametric/composite
    ///
    /// @return a BestFitSelector optimized for adaptive extraction
    /// @see AdaptiveModelExtractor
    public static BestFitSelector adaptiveCompositeSelector() {
        // Component selector for fitting each mode in composite
        BestFitSelector componentSelector = pearsonSelector();
        return new BestFitSelector(
            List.of(
                new NormalModelFitter(),
                new BetaModelFitter(),
                new GammaModelFitter(),
                new PearsonIVModelFitter(),
                new InverseGammaModelFitter(),
                new BetaPrimeModelFitter(),
                new StudentTModelFitter(),
                new UniformModelFitter(),
                new CompositeModelFitter(
                    componentSelector,
                    4,  // Up to 4 components
                    0.05,  // Max CDF deviation
                    CompositeModelFitter.ClusteringStrategy.EM
                ),
                new EmpiricalModelFitter()
            ),
            0.2  // Strong penalty for empirical to prefer parametric/composite
        );
    }

    /// Classifies the distribution type using the Pearson criterion.
    ///
    /// This method uses skewness and kurtosis from the data to determine
    /// the most appropriate Pearson distribution type.
    ///
    /// @param values the observed values
    /// @return the classified PearsonType
    public static PearsonType classifyPearsonType(float[] values) {
        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return PearsonClassifier.classify(stats.skewness(), stats.kurtosis());
    }

    /// Gets detailed Pearson classification result.
    ///
    /// @param values the observed values
    /// @return the classification result with β₁, β₂, and κ values
    public static PearsonClassifier.ClassificationResult classifyPearsonDetailed(float[] values) {
        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return PearsonClassifier.classifyDetailed(stats.skewness(), stats.kurtosis());
    }

    /// Selects the best-fitting model for the given data.
    ///
    /// @param values the observed values for one dimension
    /// @return the best-fitting component model
    public ScalarModel selectBest(float[] values) {
        return selectBestResult(values).model();
    }

    /// Selects the best-fitting model and returns the full result.
    ///
    /// @param values the observed values for one dimension
    /// @return the FitResult for the best model
    public FitResult selectBestResult(float[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return selectBestResult(stats, values);
    }

    /// Selects the best-fitting model using pre-computed statistics.
    ///
    /// Model selection uses a two-stage process:
    /// 1. Find the model with the lowest KS D-statistic (best fit)
    /// 2. Apply simplicity bias: prefer simpler models when fit quality is close
    ///
    /// The simplicity bias ensures round-trip stability by canonically selecting
    /// the simpler model when both Normal and Beta produce equivalent results.
    /// This prevents oscillation between equivalent model types.
    ///
    /// @param stats pre-computed dimension statistics
    /// @param values the observed values
    /// @return the FitResult for the best model
    public FitResult selectBestResult(DimensionStatistics stats, float[] values) {
        List<FitResult> results = fitAll(stats, values);

        if (results.isEmpty()) {
            return null;
        }

        // First pass: find the best raw score
        FitResult rawBest = null;
        double rawBestScore = Double.MAX_VALUE;

        for (FitResult result : results) {
            double score = result.goodnessOfFit();
            if ("empirical".equals(result.modelType())) {
                score += empiricalPenalty;
            }
            if (score < rawBestScore) {
                rawBestScore = score;
                rawBest = result;
            }
        }

        // Second pass: apply simplicity bias
        // If a simpler model is within threshold of best, prefer the simpler one
        // Use RELATIVE threshold: a model must be within X% of the best score (not absolute difference)
        // This prevents loose thresholds from overriding good fits when scores are small
        FitResult simplestWithinThreshold = null;
        int simplestComplexity = Integer.MAX_VALUE;

        // Compute the relative threshold: model must score <= rawBestScore * (1 + SIMPLICITY_MULTIPLIER)
        double relativeThreshold = rawBestScore * (1.0 + SIMPLICITY_MULTIPLIER);

        for (FitResult result : results) {
            double score = result.goodnessOfFit();
            if ("empirical".equals(result.modelType())) {
                score += empiricalPenalty;
            }

            // Check if within relative threshold of best
            if (score <= relativeThreshold) {
                int complexity = getModelComplexity(result.modelType());
                if (complexity < simplestComplexity) {
                    simplestComplexity = complexity;
                    simplestWithinThreshold = result;
                }
            }
        }

        return simplestWithinThreshold != null ? simplestWithinThreshold : rawBest;
    }

    /// Multiplier for simplicity bias: prefer simpler model if within this relative margin.
    /// A value of 0.5 means a model is considered equivalent if its score is within 50% of the best.
    /// For example, if the best score is 0.01, models scoring up to 0.015 are considered equivalent.
    /// This relative threshold prevents loose margins when fits are very good (low scores)
    /// while still allowing simplicity preference when fits are close.
    ///
    /// Note: For normalized vectors, the Beta distribution is excluded entirely from selectors
    /// to avoid round-trip instability, so this threshold mainly affects unbounded data selectors.
    private static final double SIMPLICITY_MULTIPLIER = 0.5;

    /// Returns model complexity score (lower = simpler, preferred).
    ///
    /// Complexity is based on number of parameters and stability under round-trip:
    /// - Normal: 2 params (μ, σ), highly stable
    /// - Uniform: 2 params (lower, upper), stable
    /// - Beta: 4 params (α, β, lower, upper), can oscillate with Normal
    /// - Gamma/StudentT: 3+ params, unbounded distributions
    /// - Empirical: many bins, always stable but not parametric
    private static int getModelComplexity(String modelType) {
        return switch (modelType) {
            case "normal" -> 1;       // Simplest parametric
            case "uniform" -> 2;      // Simple but less common
            case "beta" -> 3;         // More parameters than Normal
            case "gamma" -> 4;        // Unbounded tail
            case "student_t" -> 5;    // Heavy tails
            case "inverse_gamma" -> 6;
            case "beta_prime" -> 7;
            case "pearson_iv" -> 8;
            case "composite" -> 9;    // Mixture model
            case "empirical" -> 10;   // Fallback (but penalized separately)
            default -> 100;           // Unknown - least preferred
        };
    }

    /// Fits all candidate models and returns all results.
    ///
    /// @param values the observed values for one dimension
    /// @return list of FitResults from all fitters
    public List<FitResult> fitAll(float[] values) {
        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return fitAll(stats, values);
    }

    /// Fits all candidate models using pre-computed statistics.
    ///
    /// @param stats pre-computed dimension statistics
    /// @param values the observed values
    /// @return list of FitResults from all fitters
    public List<FitResult> fitAll(DimensionStatistics stats, float[] values) {
        List<FitResult> results = new ArrayList<>();

        for (ComponentModelFitter fitter : fitters) {
            try {
                FitResult result = fitter.fit(stats, values);
                results.add(result);
            } catch (Exception e) {
                // Skip fitters that fail (e.g., insufficient data)
            }
        }

        return results;
    }

    /// Selects the best model while also returning all fit scores.
    ///
    /// This method is useful when you need both the best model AND
    /// all fit scores for comparison/reporting (e.g., fit quality tables).
    ///
    /// @param stats pre-computed dimension statistics
    /// @param values the observed values
    /// @return result containing best fit, all scores, and best index
    public SelectionWithAllFits selectBestWithAllFits(DimensionStatistics stats, float[] values) {
        double[] allScores = new double[fitters.size()];
        int bestIndex = -1;
        double bestScore = Double.MAX_VALUE;
        FitResult bestResult = null;

        for (int i = 0; i < fitters.size(); i++) {
            try {
                FitResult result = fitters.get(i).fit(stats, values);
                allScores[i] = result.goodnessOfFit();

                double score = result.goodnessOfFit();
                // Apply empirical penalty
                if ("empirical".equals(result.modelType())) {
                    score += empiricalPenalty;
                }

                if (score < bestScore) {
                    bestScore = score;
                    bestIndex = i;
                    bestResult = result;
                }
            } catch (Exception e) {
                allScores[i] = Double.NaN;
            }
        }

        return new SelectionWithAllFits(bestResult, allScores, bestIndex);
    }

    /// Result containing the best fit selection along with all fit scores.
    ///
    /// @param bestFit the best-fitting model result
    /// @param allScores goodness-of-fit scores for all fitters (indexed by fitter order)
    /// @param bestIndex index of the best fitter in the fitters list
    public record SelectionWithAllFits(
        FitResult bestFit,
        double[] allScores,
        int bestIndex
    ) {}

    /// Returns the list of fitters used by this selector.
    public List<ComponentModelFitter> getFitters() {
        return new ArrayList<>(fitters);
    }

    /// Computes a summary of fit results for debugging/analysis.
    ///
    /// @param values the observed values
    /// @return a formatted string describing all fit results
    public String summarizeFits(float[] values) {
        List<FitResult> results = fitAll(values);
        StringBuilder sb = new StringBuilder();

        sb.append("Fit Summary:\n");
        for (FitResult result : results) {
            sb.append(String.format("  %s: score=%.4f%s\n",
                result.modelType(),
                result.goodnessOfFit(),
                result.equals(selectBestResult(values)) ? " (BEST)" : ""));
        }

        return sb.toString();
    }
}
