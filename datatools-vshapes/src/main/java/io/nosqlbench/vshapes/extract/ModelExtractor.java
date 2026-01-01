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

/**
 * Interface for extracting a {@link VectorModel} from observed vector data.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>ModelExtractor corresponds to the second-order tensor level (VectorModel):
 * <ul>
 *   <li>{@link ScalarModelFitter} - Fits ScalarModels to per-dimension data</li>
 *   <li><b>ModelExtractor</b> - Extracts VectorModels from multi-dimensional data</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 *
 * <p>Implementations analyze a dataset of vectors and produce a statistical model
 * that captures the distribution of values in each dimension. This model can then
 * be used to generate synthetic vectors with similar statistical properties.
 *
 * <h2>Extraction Process</h2>
 *
 * <pre>{@code
 * Dataset ──► ModelExtractor ──► VectorModel
 *                   │
 *                   ├── Computes statistics per dimension
 *                   ├── Fits distribution models
 *                   └── Selects best-fit model type
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // From float[][] data
 * ModelExtractor extractor = new DatasetModelExtractor();
 * VectorModel model = extractor.extractVectorModel(vectorData);
 *
 * // Or use the legacy method
 * VectorSpaceModel vsm = extractor.extract(vectorData);
 * }</pre>
 *
 * <h2>Implementation Notes</h2>
 *
 * <p>Implementations should:
 * <ul>
 *   <li>Handle datasets of any size (streaming if necessary for large data)</li>
 *   <li>Support configurable model fitting strategies</li>
 *   <li>Provide progress feedback for long-running extractions</li>
 * </ul>
 *
 * @see DatasetModelExtractor
 * @see VectorModel
 * @see VectorSpaceModel
 */
public interface ModelExtractor {

    /**
     * Extracts a vector model from the given data.
     *
     * <p>This is the preferred method using the tensor model terminology.
     * Each row represents a vector, and each column represents a dimension.
     * The data array should have shape [numVectors][numDimensions].
     *
     * @param data the vector data with shape [numVectors][numDimensions]
     * @return the extracted vector model
     * @throws IllegalArgumentException if data is null, empty, or jagged
     */
    default VectorModel extractVectorModel(float[][] data) {
        return extract(data);
    }

    /**
     * Extracts a vector space model from the given data.
     *
     * <p>Each row represents a vector, and each column represents a dimension.
     * The data array should have shape [numVectors][numDimensions].
     *
     * @param data the vector data with shape [numVectors][numDimensions]
     * @return the extracted vector space model
     * @throws IllegalArgumentException if data is null, empty, or jagged
     * @deprecated Use {@link #extractVectorModel(float[][])} instead
     */
    @Deprecated(since = "2.0", forRemoval = true)
    VectorSpaceModel extract(float[][] data);

    /**
     * Extracts a vector space model from pre-transposed data.
     *
     * <p>This variant accepts data organized by dimension rather than by vector,
     * which can be more efficient when data is already in columnar format.
     * The data array should have shape [numDimensions][numVectors].
     *
     * @param transposedData dimension-organized data with shape [numDimensions][numVectors]
     * @return the extracted vector space model
     * @throws IllegalArgumentException if data is null, empty, or jagged
     */
    VectorSpaceModel extractFromTransposed(float[][] transposedData);

    /**
     * Creates an extraction result with detailed statistics.
     *
     * <p>This method is useful when you need access to per-dimension statistics
     * and fit quality metrics in addition to the model itself.
     *
     * @param data the vector data with shape [numVectors][numDimensions]
     * @return the extraction result with model and statistics
     */
    ExtractionResult extractWithStats(float[][] data);

    /**
     * Result of model extraction including detailed statistics.
     *
     * @param model the extracted vector space model
     * @param dimensionStats per-dimension statistics
     * @param fitResults per-dimension fit results (if best-fit selection was used)
     * @param extractionTimeMs time taken for extraction in milliseconds
     */
    record ExtractionResult(
        VectorSpaceModel model,
        DimensionStatistics[] dimensionStats,
        ComponentModelFitter.FitResult[] fitResults,
        long extractionTimeMs
    ) {
        /**
         * Returns the number of dimensions in the extracted model.
         */
        public int numDimensions() {
            return dimensionStats.length;
        }

        /**
         * Returns the total number of vectors analyzed.
         */
        public long numVectors() {
            return dimensionStats.length > 0 ? dimensionStats[0].count() : 0;
        }

        /**
         * Returns a summary of the extraction for logging/debugging.
         */
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
}
