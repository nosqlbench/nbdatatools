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
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.trace.StateObserver;

import java.util.ArrayList;
import java.util.List;

/// Adaptive model extractor with internal verification and composite fallback.
///
/// ## Purpose
///
/// This extractor implements the adaptive fallback chain:
/// ```
/// Parametric → (verify) → Composite (2-4 modes) → Empirical
/// ```
///
/// It uses internal mini-verification to detect parameter instability
/// and progressively tries more complex models before falling back to empirical.
///
/// ## Algorithm
///
/// For each dimension:
/// 1. Try parametric fit (Normal, Beta, Gamma, etc.)
/// 2. Run internal mini-verification on the fit
/// 3. If verification passes (drift < threshold), accept the model
/// 4. If verification fails or KS > threshold:
///    a. Try composite with 2 modes
///    b. Try composite with 3 modes
///    c. Try composite with 4 modes
/// 5. If all composite attempts fail, use empirical
/// 6. Record which fallback strategy was used
///
/// ## Configuration
///
/// - **Verification level**: FAST (500), BALANCED (1000), THOROUGH (5000)
/// - **Clustering strategy**: HARD (fast) or EM (accurate)
/// - **Max composite modes**: 2-10 (default 10)
/// - **KS threshold for parametric**: 0.03 (trigger composite)
/// - **KS threshold for composite**: 0.05 (trigger empirical)
///
/// ## Usage
///
/// ```java
/// // Default configuration (adaptive enabled)
/// AdaptiveModelExtractor extractor = new AdaptiveModelExtractor();
/// VectorSpaceModel model = extractor.extractVectorModel(data);
///
/// // Check which strategies were used
/// AdaptiveExtractionResult result = extractor.extractAdaptive(data);
/// for (DimensionStrategy strat : result.strategies()) {
///     System.out.println("Dim " + strat.dimension() + ": " + strat.strategyUsed());
/// }
/// ```
///
/// ## Thread Safety
///
/// This class is NOT thread-safe. Use separate instances for concurrent extraction.
///
/// @see InternalVerifier
/// @see CompositeModelFitter
/// @see BestFitSelector
public final class AdaptiveModelExtractor implements ModelExtractor {

    /// KS D-statistic threshold for parametric models - above this triggers composite
    public static final double KS_THRESHOLD_PARAMETRIC = 0.03;

    /// KS D-statistic threshold for composite models - above this triggers empirical
    public static final double KS_THRESHOLD_COMPOSITE = 0.05;

    /// Default number of unique vectors for generated model
    public static final long DEFAULT_UNIQUE_VECTORS = 1_000_000;

    /// Strategy used for a dimension
    public enum Strategy {
        PARAMETRIC,
        COMPOSITE_2,
        COMPOSITE_3,
        COMPOSITE_4,
        COMPOSITE_5,
        COMPOSITE_6,
        COMPOSITE_7,
        COMPOSITE_8,
        COMPOSITE_9,
        COMPOSITE_10,
        EMPIRICAL
    }

    private final BestFitSelector parametricSelector;
    private final InternalVerifier verifier;
    private final ClusteringStrategy clusteringStrategy;
    private final int maxCompositeComponents;
    private final long uniqueVectors;
    private final boolean internalVerificationEnabled;
    private volatile StateObserver observer = StateObserver.NOOP;

    /// Creates an adaptive extractor with default settings.
    public AdaptiveModelExtractor() {
        this(BestFitSelector.pearsonSelector(),
             new InternalVerifier(VerificationLevel.BALANCED),
             ClusteringStrategy.EM,
             10,
             DEFAULT_UNIQUE_VECTORS,
             true);
    }

    /// Creates an adaptive extractor with custom verification level.
    ///
    /// @param verificationLevel the internal verification thoroughness
    public AdaptiveModelExtractor(VerificationLevel verificationLevel) {
        this(BestFitSelector.pearsonSelector(),
             new InternalVerifier(verificationLevel),
             ClusteringStrategy.EM,
             10,
             DEFAULT_UNIQUE_VECTORS,
             true);
    }

    /// Creates an adaptive extractor with full configuration.
    ///
    /// @param parametricSelector selector for parametric model fitting
    /// @param verifier the internal verifier to use
    /// @param clusteringStrategy clustering strategy for composite models
    /// @param maxCompositeComponents max components for composite (2-10)
    /// @param uniqueVectors target unique vectors for generated model
    /// @param internalVerificationEnabled whether to run internal verification
    public AdaptiveModelExtractor(
            BestFitSelector parametricSelector,
            InternalVerifier verifier,
            ClusteringStrategy clusteringStrategy,
            int maxCompositeComponents,
            long uniqueVectors,
            boolean internalVerificationEnabled) {
        this.parametricSelector = parametricSelector;
        this.verifier = verifier;
        this.clusteringStrategy = clusteringStrategy;
        this.maxCompositeComponents = Math.max(2, Math.min(maxCompositeComponents, 10));
        this.uniqueVectors = uniqueVectors;
        this.internalVerificationEnabled = internalVerificationEnabled;
    }

    /// Builder for AdaptiveModelExtractor configuration.
    ///
    /// @return a new Builder instance
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public VectorSpaceModel extractVectorModel(float[][] data) {
        return extractAdaptive(data).model();
    }

    @Override
    public ExtractionResult extractFromTransposed(float[][] transposedData) {
        return extractAdaptiveFromTransposed(transposedData).toExtractionResult();
    }

    @Override
    public ExtractionResult extractWithStats(float[][] data) {
        return extractAdaptive(data).toExtractionResult();
    }

    /// Extracts with full adaptive result including per-dimension strategies.
    ///
    /// @param data vector data in row-major format
    /// @return adaptive extraction result with strategy information
    public AdaptiveExtractionResult extractAdaptive(float[][] data) {
        validateData(data);
        float[][] transposed = transpose(data, data.length, data[0].length);
        return extractAdaptiveFromTransposed(transposed);
    }

    /// Extracts from transposed data with full adaptive result.
    ///
    /// @param transposedData vector data in column-major format
    /// @return adaptive extraction result with strategy information
    public AdaptiveExtractionResult extractAdaptiveFromTransposed(float[][] transposedData) {
        validateTransposedData(transposedData);

        long startTime = System.currentTimeMillis();
        int numDimensions = transposedData.length;

        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];
        List<DimensionStrategy> strategies = new ArrayList<>(numDimensions);

        EmpiricalModelFitter empiricalFitter = new EmpiricalModelFitter();

        for (int d = 0; d < numDimensions; d++) {
            observer.onDimensionStart(d);

            float[] dimensionData = transposedData[d];
            stats[d] = DimensionStatistics.compute(d, dimensionData);

            observer.onAccumulatorUpdate(d, stats[d]);

            // Adaptive fitting for this dimension
            DimensionFitResult dimResult = fitDimensionAdaptively(d, stats[d], dimensionData, empiricalFitter);

            fitResults[d] = dimResult.fitResult();
            // Wrap all models as composites for unified handling
            components[d] = CompositeScalarModel.wrap(dimResult.fitResult().model());
            strategies.add(dimResult.strategy());

            observer.onDimensionComplete(d, components[d]);
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new AdaptiveExtractionResult(model, stats, fitResults, extractionTime, strategies);
    }

    /// Fits a single dimension adaptively.
    private DimensionFitResult fitDimensionAdaptively(int dimension, DimensionStatistics stats,
                                                       float[] data, EmpiricalModelFitter empiricalFitter) {
        // Step 1: Try parametric fit
        ComponentModelFitter.FitResult parametricFit;
        try {
            parametricFit = parametricSelector.selectBestResult(stats, data);
        } catch (Exception e) {
            // Parametric fitting failed entirely - go straight to empirical
            return new DimensionFitResult(
                new DimensionStrategy(dimension, Strategy.EMPIRICAL, 0.0, "Parametric fit failed: " + e.getMessage()),
                empiricalFitter.fit(stats, data)
            );
        }

        // Step 2: Check if parametric fit is good enough
        double ksStatistic = parametricFit.goodnessOfFit();
        boolean parametricGood = ksStatistic <= KS_THRESHOLD_PARAMETRIC;

        // Step 3: If internal verification is enabled, verify the parametric fit
        if (parametricGood && internalVerificationEnabled) {
            VerificationResult verification = verifier.verify(
                parametricFit.model(), data, findFitterForType(parametricFit.modelType()));
            parametricGood = verification.passed();
            if (!parametricGood) {
                // Verification failed - model is unstable
                ksStatistic = Math.max(ksStatistic, KS_THRESHOLD_PARAMETRIC + 0.01);
            }
        }

        if (parametricGood) {
            return new DimensionFitResult(
                new DimensionStrategy(dimension, Strategy.PARAMETRIC, ksStatistic,
                    "KS=" + String.format("%.4f", ksStatistic)),
                parametricFit
            );
        }

        // Step 4: Try composite models progressively
        for (int numModes = 2; numModes <= maxCompositeComponents; numModes++) {
            try {
                CompositeModelFitter compositeFitter = new CompositeModelFitter(
                    BestFitSelector.boundedDataSelector(),
                    numModes,
                    KS_THRESHOLD_COMPOSITE,
                    clusteringStrategy
                );

                ComponentModelFitter.FitResult compositeFit = compositeFitter.fit(stats, data);

                // Check if composite fit is good enough
                if (compositeFit.goodnessOfFit() <= KS_THRESHOLD_COMPOSITE) {
                    Strategy strategy = switch (numModes) {
                        case 2 -> Strategy.COMPOSITE_2;
                        case 3 -> Strategy.COMPOSITE_3;
                        case 4 -> Strategy.COMPOSITE_4;
                        case 5 -> Strategy.COMPOSITE_5;
                        case 6 -> Strategy.COMPOSITE_6;
                        case 7 -> Strategy.COMPOSITE_7;
                        case 8 -> Strategy.COMPOSITE_8;
                        case 9 -> Strategy.COMPOSITE_9;
                        case 10 -> Strategy.COMPOSITE_10;
                        default -> Strategy.EMPIRICAL;
                    };

                    return new DimensionFitResult(
                        new DimensionStrategy(dimension, strategy, compositeFit.goodnessOfFit(),
                            numModes + "-mode composite, KS=" + String.format("%.4f", compositeFit.goodnessOfFit())),
                        compositeFit
                    );
                }
            } catch (Exception e) {
                // Composite fitting failed for this number of modes - try more
            }
        }

        // Step 5: Fall back to empirical
        ComponentModelFitter.FitResult empiricalFit = empiricalFitter.fit(stats, data);
        return new DimensionFitResult(
            new DimensionStrategy(dimension, Strategy.EMPIRICAL, empiricalFit.goodnessOfFit(),
                "Fallback to empirical after composite attempts failed"),
            empiricalFit
        );
    }

    /// Finds a fitter for a given model type.
    private ComponentModelFitter findFitterForType(String modelType) {
        for (ComponentModelFitter fitter : parametricSelector.getFitters()) {
            if (fitter.getModelType().equals(modelType)) {
                return fitter;
            }
        }
        // Fallback
        return new NormalModelFitter();
    }

    // Validation methods

    private void validateData(float[][] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("data cannot be null or empty");
        }
        if (data[0] == null || data[0].length == 0) {
            throw new IllegalArgumentException("data rows cannot be null or empty");
        }
        int expectedDimensions = data[0].length;
        for (int i = 1; i < data.length; i++) {
            if (data[i] == null || data[i].length != expectedDimensions) {
                throw new IllegalArgumentException("data is jagged at row " + i);
            }
        }
    }

    private void validateTransposedData(float[][] transposedData) {
        if (transposedData == null || transposedData.length == 0) {
            throw new IllegalArgumentException("transposedData cannot be null or empty");
        }
        if (transposedData[0] == null || transposedData[0].length == 0) {
            throw new IllegalArgumentException("dimension arrays cannot be null or empty");
        }
        int expectedVectors = transposedData[0].length;
        for (int d = 1; d < transposedData.length; d++) {
            if (transposedData[d] == null || transposedData[d].length != expectedVectors) {
                throw new IllegalArgumentException("transposedData is jagged at dimension " + d);
            }
        }
    }

    private float[][] transpose(float[][] data, int numVectors, int numDimensions) {
        float[][] transposed = new float[numDimensions][numVectors];
        for (int v = 0; v < numVectors; v++) {
            for (int d = 0; d < numDimensions; d++) {
                transposed[d][v] = data[v][d];
            }
        }
        return transposed;
    }

    @Override
    public void setObserver(StateObserver observer) {
        this.observer = (observer != null) ? observer : StateObserver.NOOP;
    }

    /// Internal record for dimension fit result.
    private record DimensionFitResult(DimensionStrategy strategy, ComponentModelFitter.FitResult fitResult) {}

    /// Strategy used for a single dimension.
    ///
    /// @param dimension the dimension index
    /// @param strategyUsed which fallback strategy was used
    /// @param finalKsStatistic the final KS statistic of the chosen model
    /// @param details human-readable details about the fitting process
    public record DimensionStrategy(
        int dimension,
        Strategy strategyUsed,
        double finalKsStatistic,
        String details
    ) {
        /// Returns true if a composite model was used.
        public boolean isComposite() {
            return switch (strategyUsed) {
                case COMPOSITE_2, COMPOSITE_3, COMPOSITE_4, COMPOSITE_5,
                     COMPOSITE_6, COMPOSITE_7, COMPOSITE_8, COMPOSITE_9, COMPOSITE_10 -> true;
                default -> false;
            };
        }

        /// Returns true if empirical fallback was used.
        public boolean isEmpirical() {
            return strategyUsed == Strategy.EMPIRICAL;
        }

        /// Returns true if a parametric model was used.
        public boolean isParametric() {
            return strategyUsed == Strategy.PARAMETRIC;
        }
    }

    /// Result of adaptive extraction including per-dimension strategies.
    ///
    /// @param model the extracted vector space model
    /// @param dimensionStats per-dimension statistics
    /// @param fitResults per-dimension fit results
    /// @param extractionTimeMs extraction time in milliseconds
    /// @param strategies per-dimension strategy information
    public record AdaptiveExtractionResult(
        VectorSpaceModel model,
        DimensionStatistics[] dimensionStats,
        ComponentModelFitter.FitResult[] fitResults,
        long extractionTimeMs,
        List<DimensionStrategy> strategies
    ) {
        /// Converts to standard ExtractionResult.
        ///
        /// @return standard extraction result without strategy information
        public ExtractionResult toExtractionResult() {
            return new ExtractionResult(model, dimensionStats, fitResults, extractionTimeMs);
        }

        /// Returns count of dimensions using parametric models.
        ///
        /// @return number of dimensions fitted with parametric models
        public long parametricCount() {
            return strategies.stream().filter(DimensionStrategy::isParametric).count();
        }

        /// Returns count of dimensions using composite models.
        ///
        /// @return number of dimensions fitted with composite (mixture) models
        public long compositeCount() {
            return strategies.stream().filter(DimensionStrategy::isComposite).count();
        }

        /// Returns count of dimensions using empirical models.
        ///
        /// @return number of dimensions using empirical fallback
        public long empiricalCount() {
            return strategies.stream().filter(DimensionStrategy::isEmpirical).count();
        }

        /// Returns a summary of the extraction.
        ///
        /// @return human-readable summary string
        public String summary() {
            return String.format(
                "Adaptive extraction: %d dimensions in %dms [parametric=%d, composite=%d, empirical=%d]",
                dimensionStats.length, extractionTimeMs,
                parametricCount(), compositeCount(), empiricalCount());
        }
    }

    /// Builder for AdaptiveModelExtractor.
    public static class Builder {
        private BestFitSelector parametricSelector = BestFitSelector.pearsonSelector();
        private VerificationLevel verificationLevel = VerificationLevel.BALANCED;
        private double driftThreshold = InternalVerifier.DEFAULT_DRIFT_THRESHOLD;
        private ClusteringStrategy clusteringStrategy = ClusteringStrategy.EM;
        private int maxCompositeComponents = 10;
        private long uniqueVectors = DEFAULT_UNIQUE_VECTORS;
        private boolean internalVerificationEnabled = true;

        /// Sets the parametric model selector.
        ///
        /// @param selector the selector for parametric model fitting
        /// @return this builder for chaining
        public Builder parametricSelector(BestFitSelector selector) {
            this.parametricSelector = selector;
            return this;
        }

        /// Sets the internal verification level.
        ///
        /// @param level the verification thoroughness level
        /// @return this builder for chaining
        public Builder verificationLevel(VerificationLevel level) {
            this.verificationLevel = level;
            return this;
        }

        /// Sets the drift threshold for internal verification.
        ///
        /// @param threshold the maximum allowed parameter drift
        /// @return this builder for chaining
        public Builder driftThreshold(double threshold) {
            this.driftThreshold = threshold;
            return this;
        }

        /// Sets the clustering strategy for composite models.
        ///
        /// @param strategy the clustering strategy (HARD or EM)
        /// @return this builder for chaining
        public Builder clusteringStrategy(ClusteringStrategy strategy) {
            this.clusteringStrategy = strategy;
            return this;
        }

        /// Sets the maximum composite components (2-10).
        ///
        /// @param max maximum number of modes in composite models
        /// @return this builder for chaining
        public Builder maxCompositeComponents(int max) {
            this.maxCompositeComponents = max;
            return this;
        }

        /// Sets the target unique vectors for generated model.
        ///
        /// @param vectors target number of unique vectors
        /// @return this builder for chaining
        public Builder uniqueVectors(long vectors) {
            this.uniqueVectors = vectors;
            return this;
        }

        /// Enables or disables internal verification.
        ///
        /// @param enabled true to enable internal verification
        /// @return this builder for chaining
        public Builder internalVerification(boolean enabled) {
            this.internalVerificationEnabled = enabled;
            return this;
        }

        /// Builds the AdaptiveModelExtractor.
        ///
        /// @return configured AdaptiveModelExtractor instance
        public AdaptiveModelExtractor build() {
            return new AdaptiveModelExtractor(
                parametricSelector,
                new InternalVerifier(verificationLevel, driftThreshold),
                clusteringStrategy,
                maxCompositeComponents,
                uniqueVectors,
                internalVerificationEnabled
            );
        }
    }
}
