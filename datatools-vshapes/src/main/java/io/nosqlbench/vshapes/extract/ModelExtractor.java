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

import io.nosqlbench.vshapes.model.VectorModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.trace.StateObserver;

/// Interface for extracting a [VectorModel] from observed vector data.
///
/// ## Tensor Hierarchy
///
/// ModelExtractor corresponds to the second-order tensor level (VectorModel):
/// - [ComponentModelFitter] - Fits ScalarModels to per-dimension data
/// - **ModelExtractor** - Extracts VectorModels from multi-dimensional data
///
/// ## Purpose
///
/// Implementations analyze a dataset of vectors and produce a statistical model
/// that captures the distribution of values in each dimension. This model can then
/// be used to generate synthetic vectors with similar statistical properties.
///
/// ## Extraction Process
///
/// ```
/// Dataset ──► ModelExtractor ──► VectorModel
///                   │
///                   ├── Computes statistics per dimension
///                   ├── Fits distribution models
///                   └── Selects best-fit model type
/// ```
///
/// ## Usage
///
/// ```java
/// ModelExtractor extractor = new DatasetModelExtractor();
/// VectorSpaceModel model = extractor.extractVectorModel(vectorData);
/// ```
///
/// ## Implementation Notes
///
/// Implementations should:
/// - Handle datasets of any size (streaming if necessary for large data)
/// - Support configurable model fitting strategies
/// - Provide progress feedback for long-running extractions
///
/// @see DatasetModelExtractor
/// @see VectorModel
/// @see VectorSpaceModel
public interface ModelExtractor {

    /// Extracts a vector space model from the given data.
    ///
    /// Each row represents a vector, and each column represents a dimension.
    /// The data array should have shape `[numVectors][numDimensions]`.
    ///
    /// @param data the vector data with shape `[numVectors][numDimensions]`
    /// @return the extracted vector space model
    /// @throws IllegalArgumentException if data is null, empty, or jagged
    VectorSpaceModel extractVectorModel(float[][] data);

    /// Extracts a vector space model from pre-transposed data.
    ///
    /// This variant accepts data organized by dimension rather than by vector,
    /// which can be more efficient when data is already in columnar format.
    /// The data array should have shape `[numDimensions][numVectors]`.
    ///
    /// @param transposedData dimension-organized data with shape `[numDimensions][numVectors]`
    /// @return the extraction result with model and statistics
    /// @throws IllegalArgumentException if data is null, empty, or jagged
    ExtractionResult extractFromTransposed(float[][] transposedData);

    /// Creates an extraction result with detailed statistics.
    ///
    /// This method is useful when you need access to per-dimension statistics
    /// and fit quality metrics in addition to the model itself.
    ///
    /// @param data the vector data with shape `[numVectors][numDimensions]`
    /// @return the extraction result with model and statistics
    ExtractionResult extractWithStats(float[][] data);

    /// Sets an observer to receive extraction lifecycle events.
    ///
    /// The observer will be called at key points during extraction:
    /// - [StateObserver#onDimensionStart(int)] - when dimension processing begins
    /// - [StateObserver#onAccumulatorUpdate(int, Object)] - periodically during data accumulation
    /// - [StateObserver#onDimensionComplete(int, ScalarModel)] - when dimension model is fitted
    ///
    /// @param observer the observer to receive events, or null to disable observation
    /// @see StateObserver
    default void setObserver(StateObserver observer) {
        // Default no-op implementation for backward compatibility
    }

    /// Result of model extraction including detailed statistics.
    ///
    /// @param model the extracted vector space model
    /// @param dimensionStats per-dimension statistics
    /// @param fitResults per-dimension fit results (if best-fit selection was used)
    /// @param extractionTimeMs time taken for extraction in milliseconds
    /// @param allFitsData optional detailed fit data for all model types (for fit quality tables)
    record ExtractionResult(
        VectorSpaceModel model,
        DimensionStatistics[] dimensionStats,
        ComponentModelFitter.FitResult[] fitResults,
        long extractionTimeMs,
        AllFitsData allFitsData
    ) {
        /// Creates an ExtractionResult without all-fits data (backwards compatible).
        public ExtractionResult(
            VectorSpaceModel model,
            DimensionStatistics[] dimensionStats,
            ComponentModelFitter.FitResult[] fitResults,
            long extractionTimeMs
        ) {
            this(model, dimensionStats, fitResults, extractionTimeMs, null);
        }

        /// Returns the number of dimensions in the extracted model.
        public int numDimensions() {
            return dimensionStats.length;
        }

        /// Returns the total number of vectors analyzed.
        public long numVectors() {
            return dimensionStats.length > 0 ? dimensionStats[0].count() : 0;
        }

        /// Returns whether this result includes detailed fit data for all model types.
        public boolean hasAllFitsData() {
            return allFitsData != null;
        }

        /// Returns a summary of the extraction for logging/debugging.
        public String summary() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Extracted %d-dimensional model from %d vectors in %dms%n",
                numDimensions(), numVectors(), extractionTimeMs));

            if (fitResults != null) {
                java.util.Map<String, Integer> typeCounts = new java.util.HashMap<>();
                for (ComponentModelFitter.FitResult result : fitResults) {
                    typeCounts.merge(result.modelType(), 1, Integer::sum);
                }
                sb.append("Model types: ");
                typeCounts.forEach((type, count) ->
                    sb.append(String.format("%s=%d ", type, count)));
            }

            return sb.toString();
        }
    }

    /// Contains all fit scores for all dimensions and all model types.
    ///
    /// This data structure enables generating fit quality comparison tables
    /// without recomputing the fits.
    ///
    /// @param modelTypes list of model type names (column headers)
    /// @param fitScores 2D array of scores: `fitScores[dimension][modelTypeIndex]`
    /// @param bestFitIndices best model type index per dimension
    /// @param sparklines Unicode sparkline histograms per dimension (may be null)
    record AllFitsData(
        java.util.List<String> modelTypes,
        double[][] fitScores,
        int[] bestFitIndices,
        String[] sparklines
    ) {
        /// Creates AllFitsData without sparklines.
        public AllFitsData(
            java.util.List<String> modelTypes,
            double[][] fitScores,
            int[] bestFitIndices
        ) {
            this(modelTypes, fitScores, bestFitIndices, null);
        }

        /// Returns the number of dimensions.
        public int numDimensions() {
            return fitScores.length;
        }

        /// Returns the number of model types evaluated.
        public int numModelTypes() {
            return modelTypes.size();
        }

        /// Gets the fit score for a specific dimension and model type.
        public double getScore(int dimension, int modelTypeIndex) {
            return fitScores[dimension][modelTypeIndex];
        }

        /// Gets the best model type name for a dimension.
        public String getBestModelType(int dimension) {
            int idx = bestFitIndices[dimension];
            return (idx >= 0 && idx < modelTypes.size()) ? modelTypes.get(idx) : "unknown";
        }

        /// Gets the sparkline for a dimension, or empty string if not available.
        public String getSparkline(int dimension) {
            return (sparklines != null && dimension < sparklines.length)
                ? sparklines[dimension]
                : "";
        }

        /// Returns whether sparklines are available.
        public boolean hasSparklines() {
            return sparklines != null && sparklines.length > 0;
        }
    }
}
