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
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.trace.StateObserver;

/**
 * Extracts a {@link VectorSpaceModel} from observed vector data by fitting
 * optimal distribution models to each dimension.
 *
 * <h2>Algorithm</h2>
 *
 * <pre>{@code
 * For each dimension d in [0, numDimensions):
 *   1. Extract column d from the dataset
 *   2. Compute dimension statistics
 *   3. Fit candidate models (Normal, Uniform, Empirical)
 *   4. Select best-fitting model based on goodness-of-fit
 *   5. Store selected model as component d
 *
 * Construct VectorSpaceModel from component models
 * }</pre>
 *
 * <h2>Configuration</h2>
 *
 * <p>The extractor can be configured with:
 * <ul>
 *   <li>Custom {@link BestFitSelector} for model selection</li>
 *   <li>Forced model type (skip best-fit selection)</li>
 *   <li>Target unique vectors for the generated model</li>
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Default extraction (automatic model selection)
 * DatasetModelExtractor extractor = new DatasetModelExtractor();
 * VectorSpaceModel model = extractor.extractVectorModel(data);
 *
 * // Force Normal models only
 * DatasetModelExtractor normalExtractor = DatasetModelExtractor.normalOnly();
 * VectorSpaceModel normalModel = normalExtractor.extractVectorModel(data);
 *
 * // With custom selector and target unique vectors
 * BestFitSelector selector = BestFitSelector.parametricOnly();
 * DatasetModelExtractor customExtractor = new DatasetModelExtractor(selector, 1_000_000);
 * }</pre>
 *
 * @see ModelExtractor
 * @see BestFitSelector
 * @see VectorSpaceModel
 */
public final class DatasetModelExtractor implements ModelExtractor {

    /** Default number of unique vectors to generate */
    public static final long DEFAULT_UNIQUE_VECTORS = 1_000_000;

    private final BestFitSelector selector;
    private final long uniqueVectors;
    private final ComponentModelFitter forcedFitter;
    private final boolean collectAllFits;
    private volatile StateObserver observer = StateObserver.NOOP;

    /**
     * Creates an extractor with default settings (automatic model selection).
     */
    public DatasetModelExtractor() {
        this(BestFitSelector.defaultSelector(), DEFAULT_UNIQUE_VECTORS, null, false);
    }

    /**
     * Creates an extractor with a custom selector and target unique vectors.
     *
     * @param selector the best-fit selector to use
     * @param uniqueVectors target number of unique vectors for the model
     */
    public DatasetModelExtractor(BestFitSelector selector, long uniqueVectors) {
        this(selector, uniqueVectors, null, false);
    }

    /**
     * Creates an extractor with a forced fitter (bypasses best-fit selection).
     *
     * @param forcedFitter the fitter to use for all dimensions
     * @param uniqueVectors target number of unique vectors for the model
     */
    public DatasetModelExtractor(ComponentModelFitter forcedFitter, long uniqueVectors) {
        this(null, uniqueVectors, forcedFitter, false);
    }

    private DatasetModelExtractor(BestFitSelector selector, long uniqueVectors,
                                   ComponentModelFitter forcedFitter, boolean collectAllFits) {
        this.selector = selector;
        if (uniqueVectors <= 0) {
            throw new IllegalArgumentException("uniqueVectors must be positive, got: " + uniqueVectors);
        }
        this.uniqueVectors = uniqueVectors;
        this.forcedFitter = forcedFitter;
        this.collectAllFits = collectAllFits;
    }

    /**
     * Returns a copy of this extractor configured to collect all fit scores.
     *
     * <p>When enabled, {@link #extractWithStats(float[][])} will populate
     * {@link ExtractionResult#allFitsData()} with scores for all model types,
     * not just the selected best. This enables fit quality comparison tables.
     *
     * @return a new extractor with all-fits collection enabled
     */
    public DatasetModelExtractor withAllFitsCollection() {
        return new DatasetModelExtractor(selector, uniqueVectors, forcedFitter, true);
    }

    /**
     * Creates an extractor that uses only Normal models.
     *
     * @return an extractor configured for Normal-only extraction
     */
    public static DatasetModelExtractor normalOnly() {
        return new DatasetModelExtractor(new NormalModelFitter(), DEFAULT_UNIQUE_VECTORS);
    }

    /**
     * Creates an extractor that uses only Uniform models.
     *
     * @return an extractor configured for Uniform-only extraction
     */
    public static DatasetModelExtractor uniformOnly() {
        return new DatasetModelExtractor(new UniformModelFitter(), DEFAULT_UNIQUE_VECTORS);
    }

    /**
     * Creates an extractor that uses only Empirical models.
     *
     * @return an extractor configured for Empirical-only extraction
     */
    public static DatasetModelExtractor empiricalOnly() {
        return new DatasetModelExtractor(new EmpiricalModelFitter(), DEFAULT_UNIQUE_VECTORS);
    }

    /**
     * Creates an extractor using parametric models only (no empirical).
     *
     * @return an extractor that chooses between Normal and Uniform
     */
    public static DatasetModelExtractor parametricOnly() {
        return new DatasetModelExtractor(BestFitSelector.parametricOnly(), DEFAULT_UNIQUE_VECTORS);
    }

    @Override
    public VectorSpaceModel extractVectorModel(float[][] data) {
        return extractWithStats(data).model();
    }

    @Override
    public ExtractionResult extractFromTransposed(float[][] transposedData) {
        validateTransposedData(transposedData);

        long startTime = System.currentTimeMillis();
        int numDimensions = transposedData.length;

        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];

        // For all-fits collection
        AllFitsData allFitsData = null;
        double[][] allFitScores = null;
        int[] bestFitIndices = null;
        String[] sparklines = null;
        java.util.List<String> modelTypes = null;

        if (collectAllFits && selector != null) {
            modelTypes = selector.getFitters().stream()
                .map(ComponentModelFitter::getModelType)
                .toList();
            allFitScores = new double[numDimensions][modelTypes.size()];
            bestFitIndices = new int[numDimensions];
            sparklines = new String[numDimensions];
        }

        for (int d = 0; d < numDimensions; d++) {
            observer.onDimensionStart(d);

            float[] dimensionData = transposedData[d];
            stats[d] = DimensionStatistics.compute(d, dimensionData);

            observer.onAccumulatorUpdate(d, stats[d]);

            ComponentModelFitter.FitResult result;
            if (forcedFitter != null) {
                result = forcedFitter.fit(stats[d], dimensionData);
            } else if (collectAllFits) {
                BestFitSelector.SelectionWithAllFits selection =
                    selector.selectBestWithAllFits(stats[d], dimensionData);
                result = selection.bestFit();
                allFitScores[d] = selection.allScores();
                bestFitIndices[d] = selection.bestIndex();
                sparklines[d] = Sparkline.generate(dimensionData, Sparkline.DEFAULT_WIDTH);
            } else {
                result = selector.selectBestResult(stats[d], dimensionData);
            }

            fitResults[d] = result;
            components[d] = result.model();

            observer.onDimensionComplete(d, result.model());
        }

        if (collectAllFits && modelTypes != null) {
            allFitsData = new AllFitsData(modelTypes, allFitScores, bestFitIndices, sparklines);
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, stats, fitResults, extractionTime, allFitsData);
    }

    @Override
    public ExtractionResult extractWithStats(float[][] data) {
        validateData(data);

        long startTime = System.currentTimeMillis();

        int numVectors = data.length;
        int numDimensions = data[0].length;

        // Transpose data for efficient per-dimension access
        float[][] transposed = transpose(data, numVectors, numDimensions);

        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];

        // For all-fits collection
        AllFitsData allFitsData = null;
        double[][] allFitScores = null;
        int[] bestFitIndices = null;
        String[] sparklines = null;
        java.util.List<String> modelTypes = null;

        if (collectAllFits && selector != null) {
            modelTypes = selector.getFitters().stream()
                .map(ComponentModelFitter::getModelType)
                .toList();
            allFitScores = new double[numDimensions][modelTypes.size()];
            bestFitIndices = new int[numDimensions];
            sparklines = new String[numDimensions];
        }

        for (int d = 0; d < numDimensions; d++) {
            observer.onDimensionStart(d);

            float[] dimensionData = transposed[d];
            stats[d] = DimensionStatistics.compute(d, dimensionData);

            // Notify observer of accumulated statistics
            observer.onAccumulatorUpdate(d, stats[d]);

            ComponentModelFitter.FitResult result;
            if (forcedFitter != null) {
                result = forcedFitter.fit(stats[d], dimensionData);
            } else if (collectAllFits) {
                // Use the method that returns all scores
                BestFitSelector.SelectionWithAllFits selection =
                    selector.selectBestWithAllFits(stats[d], dimensionData);
                result = selection.bestFit();
                allFitScores[d] = selection.allScores();
                bestFitIndices[d] = selection.bestIndex();
                // Generate sparkline histogram
                sparklines[d] = Sparkline.generate(dimensionData, Sparkline.DEFAULT_WIDTH);
            } else {
                result = selector.selectBestResult(stats[d], dimensionData);
            }

            fitResults[d] = result;
            components[d] = result.model();

            observer.onDimensionComplete(d, result.model());
        }

        if (collectAllFits && modelTypes != null) {
            allFitsData = new AllFitsData(modelTypes, allFitScores, bestFitIndices, sparklines);
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, stats, fitResults, extractionTime, allFitsData);
    }

    /**
     * Extracts a model with progress reporting.
     *
     * @param data the vector data
     * @param progressCallback callback invoked with progress (0.0 to 1.0)
     * @return the extraction result
     */
    public ExtractionResult extractWithProgress(float[][] data, ProgressCallback progressCallback) {
        validateData(data);

        long startTime = System.currentTimeMillis();

        int numVectors = data.length;
        int numDimensions = data[0].length;

        // Transpose data
        if (progressCallback != null) {
            progressCallback.onProgress(0.0, "Transposing data...");
        }
        float[][] transposed = transpose(data, numVectors, numDimensions);

        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];

        for (int d = 0; d < numDimensions; d++) {
            observer.onDimensionStart(d);

            if (progressCallback != null) {
                double progress = (d + 1.0) / numDimensions;
                progressCallback.onProgress(progress, String.format("Fitting dimension %d/%d", d + 1, numDimensions));
            }

            float[] dimensionData = transposed[d];
            stats[d] = DimensionStatistics.compute(d, dimensionData);

            observer.onAccumulatorUpdate(d, stats[d]);

            ComponentModelFitter.FitResult result;
            if (forcedFitter != null) {
                result = forcedFitter.fit(stats[d], dimensionData);
            } else {
                result = selector.selectBestResult(stats[d], dimensionData);
            }

            fitResults[d] = result;
            components[d] = result.model();

            observer.onDimensionComplete(d, result.model());
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        if (progressCallback != null) {
            progressCallback.onProgress(1.0, "Extraction complete");
        }

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, stats, fitResults, extractionTime);
    }

    /**
     * Extracts from pre-transposed data with progress reporting.
     *
     * <p>When data is already in column-major format, this avoids the transpose step.
     *
     * @param transposedData data in format {@code [dimensions][vectorCount]}
     * @param progressCallback callback invoked with progress (0.0 to 1.0)
     * @return the extraction result
     */
    public ExtractionResult extractFromTransposedWithProgress(float[][] transposedData,
            ProgressCallback progressCallback) {
        validateTransposedData(transposedData);

        long startTime = System.currentTimeMillis();

        int numDimensions = transposedData.length;

        if (progressCallback != null) {
            progressCallback.onProgress(0.0, "Processing transposed data...");
        }

        ScalarModel[] components = new ScalarModel[numDimensions];
        DimensionStatistics[] stats = new DimensionStatistics[numDimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[numDimensions];

        // For all-fits collection
        AllFitsData allFitsData = null;
        double[][] allFitScores = null;
        int[] bestFitIndices = null;
        String[] sparklines = null;
        java.util.List<String> modelTypes = null;

        if (collectAllFits && selector != null) {
            modelTypes = selector.getFitters().stream()
                .map(ComponentModelFitter::getModelType)
                .toList();
            allFitScores = new double[numDimensions][modelTypes.size()];
            bestFitIndices = new int[numDimensions];
            sparklines = new String[numDimensions];
        }

        for (int d = 0; d < numDimensions; d++) {
            observer.onDimensionStart(d);

            if (progressCallback != null) {
                double progress = (d + 1.0) / numDimensions;
                progressCallback.onProgress(progress, String.format("Fitting dimension %d/%d", d + 1, numDimensions));
            }

            float[] dimensionData = transposedData[d];
            stats[d] = DimensionStatistics.compute(d, dimensionData);

            observer.onAccumulatorUpdate(d, stats[d]);

            ComponentModelFitter.FitResult result;
            if (forcedFitter != null) {
                result = forcedFitter.fit(stats[d], dimensionData);
            } else if (collectAllFits) {
                BestFitSelector.SelectionWithAllFits selection =
                    selector.selectBestWithAllFits(stats[d], dimensionData);
                result = selection.bestFit();
                allFitScores[d] = selection.allScores();
                bestFitIndices[d] = selection.bestIndex();
                sparklines[d] = Sparkline.generate(dimensionData, Sparkline.DEFAULT_WIDTH);
            } else {
                result = selector.selectBestResult(stats[d], dimensionData);
            }

            fitResults[d] = result;
            components[d] = result.model();

            observer.onDimensionComplete(d, result.model());
        }

        if (collectAllFits && modelTypes != null) {
            allFitsData = new AllFitsData(modelTypes, allFitScores, bestFitIndices, sparklines);
        }

        long extractionTime = System.currentTimeMillis() - startTime;

        if (progressCallback != null) {
            progressCallback.onProgress(1.0, "Extraction complete");
        }

        VectorSpaceModel model = new VectorSpaceModel(uniqueVectors, components);
        return new ExtractionResult(model, stats, fitResults, extractionTime, allFitsData);
    }

    /**
     * Callback interface for progress reporting.
     */
    @FunctionalInterface
    public interface ProgressCallback {
        /**
         * Called to report extraction progress.
         *
         * @param progress progress from 0.0 to 1.0
         * @param message descriptive message about current operation
         */
        void onProgress(double progress, String message);
    }

    private void validateData(float[][] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        if (data.length == 0) {
            throw new IllegalArgumentException("data cannot be empty");
        }
        if (data[0] == null || data[0].length == 0) {
            throw new IllegalArgumentException("data rows cannot be null or empty");
        }

        int expectedDimensions = data[0].length;
        for (int i = 1; i < data.length; i++) {
            if (data[i] == null || data[i].length != expectedDimensions) {
                throw new IllegalArgumentException(
                    "data is jagged: row 0 has " + expectedDimensions +
                    " dimensions, but row " + i + " has " +
                    (data[i] == null ? "null" : data[i].length));
            }
        }
    }

    private void validateTransposedData(float[][] transposedData) {
        if (transposedData == null) {
            throw new IllegalArgumentException("transposedData cannot be null");
        }
        if (transposedData.length == 0) {
            throw new IllegalArgumentException("transposedData cannot be empty");
        }
        if (transposedData[0] == null || transposedData[0].length == 0) {
            throw new IllegalArgumentException("dimension arrays cannot be null or empty");
        }

        int expectedVectors = transposedData[0].length;
        for (int d = 1; d < transposedData.length; d++) {
            if (transposedData[d] == null || transposedData[d].length != expectedVectors) {
                throw new IllegalArgumentException(
                    "transposedData is jagged: dimension 0 has " + expectedVectors +
                    " vectors, but dimension " + d + " has " +
                    (transposedData[d] == null ? "null" : transposedData[d].length));
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

    /**
     * Returns the target unique vectors for extracted models.
     */
    public long getUniqueVectors() {
        return uniqueVectors;
    }

    /**
     * Returns the best-fit selector used by this extractor, or null if using a forced fitter.
     */
    public BestFitSelector getSelector() {
        return selector;
    }

    /**
     * Returns the forced fitter, or null if using best-fit selection.
     */
    public ComponentModelFitter getForcedFitter() {
        return forcedFitter;
    }

    @Override
    public void setObserver(StateObserver observer) {
        this.observer = (observer != null) ? observer : StateObserver.NOOP;
    }

    /**
     * Returns the current state observer.
     */
    public StateObserver getObserver() {
        return observer;
    }
}
