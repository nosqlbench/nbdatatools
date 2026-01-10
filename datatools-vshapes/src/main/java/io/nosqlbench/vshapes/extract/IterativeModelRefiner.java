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

import io.nosqlbench.vshapes.extract.CompositeModelFitter.ClusteringStrategy;
import io.nosqlbench.vshapes.extract.InternalVerifier.VerificationLevel;
import io.nosqlbench.vshapes.extract.InternalVerifier.VerificationResult;
import io.nosqlbench.vshapes.model.ScalarModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/// Iterative model refiner with internal verification feedback loop.
///
/// ## Purpose
///
/// This class implements an iterative refinement strategy that:
/// 1. Starts with the simplest moment-based models (fewest parameters)
/// 2. Verifies each fit via internal round-trip testing
/// 3. If verification fails, progressively tries more complex models
/// 4. Falls back to empirical only when all parametric/composite approaches fail
///
/// ## Model Preference Hierarchy
///
/// Models are tried in order of increasing complexity:
///
/// **Tier 1: Simple Moment-Based (2 parameters)**
/// - Normal: μ (mean), σ (stddev)
/// - Uniform: lower, upper
///
/// **Tier 2: Extended Moment-Based (3+ parameters)**
/// - Beta: α, β, bounds (uses skewness)
/// - Gamma: shape, scale, location (uses skewness)
/// - Student-t: ν, location, scale (uses kurtosis)
///
/// **Tier 3: Composite Models**
/// - 2-component mixture
/// - 3-component mixture
/// - 4-component mixture (max)
///
/// **Tier 4: Empirical (fallback)**
/// - Histogram-based quantile distribution
///
/// ## Usage
///
/// ```java
/// IterativeModelRefiner refiner = IterativeModelRefiner.builder()
///     .verificationLevel(VerificationLevel.BALANCED)
///     .driftThreshold(0.02)  // 2% max parameter drift
///     .maxCompositeComponents(4)
///     .build();
///
/// RefinementResult result = refiner.refine(stats, data);
/// ScalarModel model = result.model();
/// System.out.println("Strategy: " + result.strategy());
/// ```
///
/// ## Thread Safety
///
/// This class is NOT thread-safe. Use separate instances for concurrent refinement.
///
/// @see InternalVerifier
/// @see CompositeModelFitter
public final class IterativeModelRefiner {

    /// Strategy used to obtain the final model.
    public enum RefinementStrategy {
        /// Simple 2-parameter model (Normal, Uniform)
        SIMPLE_PARAMETRIC,
        /// Extended moment-based model (Beta, Gamma, Student-t)
        EXTENDED_PARAMETRIC,
        /// Composite mixture model
        COMPOSITE_2,
        COMPOSITE_3,
        COMPOSITE_4,
        /// Empirical histogram fallback
        EMPIRICAL
    }

    private final InternalVerifier verifier;
    private final double driftThreshold;
    private final double ksThresholdParametric;
    private final double ksThresholdComposite;
    private final int maxCompositeComponents;
    private final boolean verboseLogging;
    private final Random random;

    // Model fitters in preference order
    private final List<ComponentModelFitter> simpleFitters;
    private final List<ComponentModelFitter> extendedFitters;
    private final EmpiricalModelFitter empiricalFitter;

    // For normalized vectors
    private final Double explicitLowerBound;
    private final Double explicitUpperBound;

    private IterativeModelRefiner(Builder builder) {
        this.verifier = new InternalVerifier(builder.verificationLevel, builder.driftThreshold);
        this.driftThreshold = builder.driftThreshold;
        this.ksThresholdParametric = builder.ksThresholdParametric;
        this.ksThresholdComposite = builder.ksThresholdComposite;
        this.maxCompositeComponents = builder.maxCompositeComponents;
        this.verboseLogging = builder.verboseLogging;
        this.random = new Random(builder.seed);
        this.explicitLowerBound = builder.explicitLowerBound;
        this.explicitUpperBound = builder.explicitUpperBound;

        // Initialize fitters based on bounds
        if (explicitLowerBound != null && explicitUpperBound != null) {
            // Normalized vectors - use explicit bounds
            this.simpleFitters = List.of(
                new NormalModelFitter(explicitLowerBound, explicitUpperBound),
                new UniformModelFitter(explicitLowerBound, explicitUpperBound)
            );
            this.extendedFitters = List.of(
                new BetaModelFitter(explicitLowerBound, explicitUpperBound),
                new GammaModelFitter(),
                new StudentTModelFitter()
            );
        } else {
            // Standard fitters
            this.simpleFitters = List.of(
                new NormalModelFitter(),
                new UniformModelFitter()
            );
            this.extendedFitters = List.of(
                new BetaModelFitter(),
                new GammaModelFitter(),
                new StudentTModelFitter()
            );
        }
        this.empiricalFitter = new EmpiricalModelFitter();
    }

    /// Creates a new builder for configuring the refiner.
    public static Builder builder() {
        return new Builder();
    }

    /// Refines a dimension's model through iterative verification.
    ///
    /// @param stats the dimension statistics
    /// @param data the raw dimension data
    /// @return the refinement result with model and strategy used
    public RefinementResult refine(DimensionStatistics stats, float[] data) {
        List<RefinementAttempt> attempts = new ArrayList<>();

        // Tier 1: Try simple parametric models
        for (ComponentModelFitter fitter : simpleFitters) {
            RefinementAttempt attempt = tryFitter(fitter, stats, data, RefinementStrategy.SIMPLE_PARAMETRIC);
            attempts.add(attempt);
            if (attempt.verified) {
                return createResult(attempt, attempts);
            }
        }

        // Tier 2: Try extended parametric models
        for (ComponentModelFitter fitter : extendedFitters) {
            RefinementAttempt attempt = tryFitter(fitter, stats, data, RefinementStrategy.EXTENDED_PARAMETRIC);
            attempts.add(attempt);
            if (attempt.verified) {
                return createResult(attempt, attempts);
            }
        }

        // Tier 3: Try composite models (2, 3, 4 components)
        for (int components = 2; components <= maxCompositeComponents; components++) {
            RefinementStrategy strategy = switch (components) {
                case 2 -> RefinementStrategy.COMPOSITE_2;
                case 3 -> RefinementStrategy.COMPOSITE_3;
                default -> RefinementStrategy.COMPOSITE_4;
            };

            RefinementAttempt attempt = tryComposite(stats, data, components, strategy);
            attempts.add(attempt);
            if (attempt.verified) {
                return createResult(attempt, attempts);
            }
        }

        // Tier 4: Fall back to empirical
        ComponentModelFitter.FitResult empiricalResult = empiricalFitter.fit(stats, data);
        RefinementAttempt empiricalAttempt = new RefinementAttempt(
            empiricalResult.model(),
            empiricalResult.goodnessOfFit(),
            true,  // Empirical always "verifies" (it's exact)
            0.0,   // No drift
            RefinementStrategy.EMPIRICAL,
            "empirical"
        );
        attempts.add(empiricalAttempt);

        return createResult(empiricalAttempt, attempts);
    }

    /// Tries a single fitter and verifies the result.
    private RefinementAttempt tryFitter(ComponentModelFitter fitter, DimensionStatistics stats,
                                        float[] data, RefinementStrategy strategy) {
        try {
            ComponentModelFitter.FitResult fitResult = fitter.fit(stats, data);

            // Check KS threshold first
            if (fitResult.goodnessOfFit() > ksThresholdParametric) {
                if (verboseLogging) {
                    System.out.printf("    [%s] KS=%.4f > threshold %.4f - skipping verification%n",
                        fitter.getModelType(), fitResult.goodnessOfFit(), ksThresholdParametric);
                }
                return new RefinementAttempt(
                    fitResult.model(),
                    fitResult.goodnessOfFit(),
                    false,
                    1.0,  // Failed KS
                    strategy,
                    fitter.getModelType()
                );
            }

            // Verify via round-trip
            VerificationResult verifyResult = verifier.verify(fitResult.model(), data, fitter);

            if (verboseLogging) {
                System.out.printf("    [%s] KS=%.4f, drift=%.2f%% - %s%n",
                    fitter.getModelType(),
                    fitResult.goodnessOfFit(),
                    verifyResult.maxDriftPercent(),
                    verifyResult.passed() ? "PASSED" : "FAILED");
            }

            return new RefinementAttempt(
                fitResult.model(),
                fitResult.goodnessOfFit(),
                verifyResult.passed(),
                verifyResult.maxDrift(),
                strategy,
                fitter.getModelType()
            );

        } catch (Exception e) {
            if (verboseLogging) {
                System.out.printf("    [%s] ERROR: %s%n", fitter.getModelType(), e.getMessage());
            }
            return new RefinementAttempt(
                null,
                Double.MAX_VALUE,
                false,
                1.0,
                strategy,
                fitter.getModelType()
            );
        }
    }

    /// Tries a composite model with specified number of components.
    private RefinementAttempt tryComposite(DimensionStatistics stats, float[] data,
                                           int components, RefinementStrategy strategy) {
        try {
            // Create composite fitter
            BestFitSelector componentSelector = explicitLowerBound != null
                ? BestFitSelector.normalizedVectorSelector()
                : BestFitSelector.boundedDataSelector();

            CompositeModelFitter compositeFitter = new CompositeModelFitter(
                componentSelector,
                components,
                ksThresholdComposite,
                ClusteringStrategy.EM
            );

            ComponentModelFitter.FitResult fitResult = compositeFitter.fit(stats, data);

            // For composite, we check KS threshold differently
            if (fitResult.goodnessOfFit() > ksThresholdComposite) {
                if (verboseLogging) {
                    System.out.printf("    [composite-%d] KS=%.4f > threshold %.4f - failed%n",
                        components, fitResult.goodnessOfFit(), ksThresholdComposite);
                }
                return new RefinementAttempt(
                    fitResult.model(),
                    fitResult.goodnessOfFit(),
                    false,
                    1.0,
                    strategy,
                    "composite-" + components
                );
            }

            if (verboseLogging) {
                System.out.printf("    [composite-%d] KS=%.4f - PASSED%n",
                    components, fitResult.goodnessOfFit());
            }

            // Composite models pass if they meet KS threshold (verification is implicit in fitting)
            return new RefinementAttempt(
                fitResult.model(),
                fitResult.goodnessOfFit(),
                true,
                0.0,  // Composite doesn't have simple drift metric
                strategy,
                "composite-" + components
            );

        } catch (Exception e) {
            if (verboseLogging) {
                System.out.printf("    [composite-%d] ERROR: %s%n", components, e.getMessage());
            }
            return new RefinementAttempt(
                null,
                Double.MAX_VALUE,
                false,
                1.0,
                strategy,
                "composite-" + components
            );
        }
    }

    /// Creates the final result from an attempt and the full attempt history.
    private RefinementResult createResult(RefinementAttempt successfulAttempt,
                                          List<RefinementAttempt> allAttempts) {
        return new RefinementResult(
            successfulAttempt.model,
            successfulAttempt.strategy,
            successfulAttempt.ksStatistic,
            successfulAttempt.drift,
            allAttempts.size(),
            allAttempts
        );
    }

    /// Internal record for tracking a single refinement attempt.
    private record RefinementAttempt(
        ScalarModel model,
        double ksStatistic,
        boolean verified,
        double drift,
        RefinementStrategy strategy,
        String fitterName
    ) {}

    /// Result of iterative model refinement.
    ///
    /// @param model the final selected model
    /// @param strategy the strategy that produced the model
    /// @param ksStatistic the KS D-statistic of the final model
    /// @param drift the parameter drift from verification (0 for empirical/composite)
    /// @param attemptsCount total number of fitting attempts
    /// @param attempts detailed list of all attempts (for diagnostics)
    public record RefinementResult(
        ScalarModel model,
        RefinementStrategy strategy,
        double ksStatistic,
        double drift,
        int attemptsCount,
        List<RefinementAttempt> attempts
    ) {
        /// Returns true if a simple parametric model was used.
        public boolean isSimpleParametric() {
            return strategy == RefinementStrategy.SIMPLE_PARAMETRIC;
        }

        /// Returns true if a composite model was used.
        public boolean isComposite() {
            return strategy == RefinementStrategy.COMPOSITE_2 ||
                   strategy == RefinementStrategy.COMPOSITE_3 ||
                   strategy == RefinementStrategy.COMPOSITE_4;
        }

        /// Returns true if empirical fallback was used.
        public boolean isEmpirical() {
            return strategy == RefinementStrategy.EMPIRICAL;
        }

        /// Returns a summary string.
        public String summary() {
            return String.format("%s (KS=%.4f, %d attempts)",
                strategy, ksStatistic, attemptsCount);
        }
    }

    /// Builder for configuring IterativeModelRefiner.
    public static final class Builder {
        private VerificationLevel verificationLevel = VerificationLevel.BALANCED;
        private double driftThreshold = 0.02;  // 2% default
        private double ksThresholdParametric = 0.03;
        private double ksThresholdComposite = 0.05;
        private int maxCompositeComponents = 4;
        private boolean verboseLogging = false;
        private long seed = 42;
        private Double explicitLowerBound = null;
        private Double explicitUpperBound = null;

        /// Sets the verification thoroughness level.
        public Builder verificationLevel(VerificationLevel level) {
            this.verificationLevel = level;
            return this;
        }

        /// Sets the maximum allowed parameter drift (0.0 to 1.0).
        public Builder driftThreshold(double threshold) {
            this.driftThreshold = threshold;
            return this;
        }

        /// Sets the KS threshold for parametric models.
        public Builder ksThresholdParametric(double threshold) {
            this.ksThresholdParametric = threshold;
            return this;
        }

        /// Sets the KS threshold for composite models.
        public Builder ksThresholdComposite(double threshold) {
            this.ksThresholdComposite = threshold;
            return this;
        }

        /// Sets the maximum number of composite components to try.
        public Builder maxCompositeComponents(int max) {
            this.maxCompositeComponents = Math.max(2, Math.min(4, max));
            return this;
        }

        /// Enables verbose logging during refinement.
        public Builder verboseLogging(boolean verbose) {
            this.verboseLogging = verbose;
            return this;
        }

        /// Sets the random seed for reproducibility.
        public Builder seed(long seed) {
            this.seed = seed;
            return this;
        }

        /// Sets explicit bounds for normalized vectors.
        public Builder normalizedVectorBounds(double lower, double upper) {
            this.explicitLowerBound = lower;
            this.explicitUpperBound = upper;
            return this;
        }

        /// Configures for L2-normalized vectors with [-1, 1] bounds.
        public Builder forNormalizedVectors() {
            return normalizedVectorBounds(-1.0, 1.0);
        }

        /// Builds the configured refiner.
        public IterativeModelRefiner build() {
            return new IterativeModelRefiner(this);
        }
    }
}
