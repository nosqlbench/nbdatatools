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
import io.nosqlbench.vshapes.model.ComponentModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Selects the best-fitting distribution type for each dimension.
 *
 * <h2>Purpose</h2>
 *
 * <p>Given observed data for a dimension, this class fits multiple distribution
 * types and selects the one with the best goodness-of-fit score. This allows
 * automatic model selection based on the actual data characteristics.
 *
 * <h2>Selection Algorithm</h2>
 *
 * <pre>{@code
 * For each dimension:
 *   1. Compute dimension statistics
 *   2. For each fitter in the candidate list:
 *      a. Fit the model to data
 *      b. Compute goodness-of-fit score
 *   3. Select the model with the lowest (best) score
 * }</pre>
 *
 * <h2>Default Fitters</h2>
 *
 * <p>By default, the selector considers:
 * <ol>
 *   <li>Gaussian (normal) distribution</li>
 *   <li>Uniform distribution</li>
 *   <li>Empirical (histogram) distribution</li>
 * </ol>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * BestFitSelector selector = BestFitSelector.defaultSelector();
 * ComponentModel bestModel = selector.selectBest(dimensionData);
 *
 * // Or get all fit results for comparison
 * List<FitResult> allFits = selector.fitAll(dimensionData);
 * }</pre>
 *
 * @see ComponentModelFitter
 * @see DatasetModelExtractor
 */
public final class BestFitSelector {

    private final List<ComponentModelFitter> fitters;
    private final double empiricalPenalty;

    /**
     * Creates a selector with specified fitters.
     *
     * @param fitters the list of fitters to consider
     */
    public BestFitSelector(List<ComponentModelFitter> fitters) {
        this(fitters, 0.1);
    }

    /**
     * Creates a selector with specified fitters and empirical penalty.
     *
     * @param fitters the list of fitters to consider
     * @param empiricalPenalty additional penalty for empirical models (to prefer parametric)
     */
    public BestFitSelector(List<ComponentModelFitter> fitters, double empiricalPenalty) {
        Objects.requireNonNull(fitters, "fitters cannot be null");
        if (fitters.isEmpty()) {
            throw new IllegalArgumentException("fitters cannot be empty");
        }
        this.fitters = new ArrayList<>(fitters);
        this.empiricalPenalty = empiricalPenalty;
    }

    /**
     * Creates a selector with the default set of fitters.
     *
     * @return a BestFitSelector with Gaussian, Uniform, and Empirical fitters
     */
    public static BestFitSelector defaultSelector() {
        return new BestFitSelector(List.of(
            new GaussianModelFitter(),
            new UniformModelFitter(),
            new EmpiricalModelFitter()
        ));
    }

    /**
     * Creates a selector with only parametric fitters (no empirical).
     *
     * @return a BestFitSelector with Gaussian and Uniform fitters only
     */
    public static BestFitSelector parametricOnly() {
        return new BestFitSelector(List.of(
            new GaussianModelFitter(),
            new UniformModelFitter()
        ));
    }

    /**
     * Selects the best-fitting model for the given data.
     *
     * @param values the observed values for one dimension
     * @return the best-fitting component model
     */
    public ComponentModel selectBest(float[] values) {
        return selectBestResult(values).model();
    }

    /**
     * Selects the best-fitting model and returns the full result.
     *
     * @param values the observed values for one dimension
     * @return the FitResult for the best model
     */
    public FitResult selectBestResult(float[] values) {
        Objects.requireNonNull(values, "values cannot be null");
        if (values.length == 0) {
            throw new IllegalArgumentException("values cannot be empty");
        }

        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return selectBestResult(stats, values);
    }

    /**
     * Selects the best-fitting model using pre-computed statistics.
     *
     * @param stats pre-computed dimension statistics
     * @param values the observed values
     * @return the FitResult for the best model
     */
    public FitResult selectBestResult(DimensionStatistics stats, float[] values) {
        List<FitResult> results = fitAll(stats, values);

        FitResult best = null;
        double bestScore = Double.MAX_VALUE;

        for (FitResult result : results) {
            double score = result.goodnessOfFit();

            // Apply penalty for empirical models (prefer parametric when close)
            if ("empirical".equals(result.modelType())) {
                score += empiricalPenalty;
            }

            if (score < bestScore) {
                bestScore = score;
                best = result;
            }
        }

        return best;
    }

    /**
     * Fits all candidate models and returns all results.
     *
     * @param values the observed values for one dimension
     * @return list of FitResults from all fitters
     */
    public List<FitResult> fitAll(float[] values) {
        DimensionStatistics stats = DimensionStatistics.compute(0, values);
        return fitAll(stats, values);
    }

    /**
     * Fits all candidate models using pre-computed statistics.
     *
     * @param stats pre-computed dimension statistics
     * @param values the observed values
     * @return list of FitResults from all fitters
     */
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

    /**
     * Returns the list of fitters used by this selector.
     */
    public List<ComponentModelFitter> getFitters() {
        return new ArrayList<>(fitters);
    }

    /**
     * Computes a summary of fit results for debugging/analysis.
     *
     * @param values the observed values
     * @return a formatted string describing all fit results
     */
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
