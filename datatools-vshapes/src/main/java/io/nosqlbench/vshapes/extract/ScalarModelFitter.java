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

/**
 * Interface for fitting a specific distribution type to observed data.
 *
 * <p>This interface is functionally equivalent to {@link ComponentModelFitter} and
 * is provided for consistency with the tensor model terminology. It extends
 * ComponentModelFitter to maintain backward compatibility.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>ComponentModelFitter corresponds to the first-order tensor level (ScalarModel):
 * <ul>
 *   <li>ComponentModelFitter - Fits ScalarModels to per-dimension data</li>
 *   <li>ModelExtractor - Extracts VectorModels from multi-dimensional data</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 *
 * <p>Implementations of this interface estimate distribution parameters from
 * observed data for a single dimension. Each implementation handles a specific
 * distribution family (Gaussian, Uniform, Empirical, etc.).
 *
 * <h2>Fitting Process</h2>
 *
 * <pre>{@code
 * Observed Data ──► Fitter ──► ScalarModel
 *                      │
 *                      └── Also computes goodness-of-fit score
 * }</pre>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * ComponentModelFitter gaussianFitter = new GaussianModelFitter();
 * FitResult result = gaussianFitter.fit(dimensionData);
 *
 * ScalarModel model = result.scalarModel();
 * double goodnessOfFit = result.goodnessOfFit();
 * }</pre>
 *
 * @see ComponentModelFitter
 * @see io.nosqlbench.vshapes.model.ScalarModel
 * @see BestFitSelector
 */
public interface ScalarModelFitter extends ComponentModelFitter {

    /**
     * Result of fitting a distribution to data using the new tensor terminology.
     *
     * <p>This is a convenience wrapper around {@link ComponentModelFitter.FitResult}
     * that provides a method to access the result as a ScalarModel.
     */
    interface ScalarFitResult {

        /**
         * Returns the fitted scalar model.
         *
         * @return the fitted ScalarModel
         */
        ScalarModel scalarModel();

        /**
         * Returns the goodness-of-fit score (lower is better).
         *
         * @return the goodness-of-fit score
         */
        double goodnessOfFit();

        /**
         * Returns the model type identifier.
         *
         * @return the model type (e.g., "gaussian", "uniform", "empirical")
         */
        String modelType();

        /**
         * Creates a ScalarFitResult from a ComponentModelFitter.FitResult.
         *
         * @param result the component model fitter result
         * @return the equivalent scalar fit result
         */
        static ScalarFitResult from(FitResult result) {
            return new ScalarFitResult() {
                @Override
                public ScalarModel scalarModel() {
                    return result.model();  // ScalarModel extends ScalarModel
                }

                @Override
                public double goodnessOfFit() {
                    return result.goodnessOfFit();
                }

                @Override
                public String modelType() {
                    return result.modelType();
                }
            };
        }
    }

    /**
     * Fits this distribution type to the observed data and returns a ScalarFitResult.
     *
     * @param values the observed values for one dimension
     * @return the fit result containing the scalar model and goodness-of-fit score
     */
    default ScalarFitResult fitScalar(float[] values) {
        return ScalarFitResult.from(fit(values));
    }

    /**
     * Fits this distribution type using pre-computed statistics and returns a ScalarFitResult.
     *
     * @param stats pre-computed dimension statistics
     * @param values the observed values (may be needed for some fitters)
     * @return the fit result
     */
    default ScalarFitResult fitScalar(DimensionStatistics stats, float[] values) {
        return ScalarFitResult.from(fit(stats, values));
    }
}
