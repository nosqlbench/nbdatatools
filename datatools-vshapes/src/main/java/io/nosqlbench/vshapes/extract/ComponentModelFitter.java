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

/// Interface for fitting a specific distribution type to observed data.
///
/// ## Purpose
///
/// Implementations of this interface estimate distribution parameters from
/// observed data for a single dimension. Each implementation handles a specific
/// distribution family (Gaussian, Uniform, Empirical, etc.).
///
/// ## Fitting Process
///
/// ```
/// Observed Data ──► Fitter ──► ScalarModel
///                      │
///                      └── Also computes goodness-of-fit score
/// ```
///
/// ## Goodness of Fit
///
/// Each fitter computes a goodness-of-fit score that can be used to compare
/// how well different distribution types match the data. Lower scores indicate
/// better fit. The [BestFitSelector] uses these scores to choose the
/// optimal distribution type for each dimension.
///
/// ## Usage
///
/// ```java
/// ComponentModelFitter gaussianFitter = new GaussianModelFitter();
/// FitResult result = gaussianFitter.fit(dimensionData);
///
/// ScalarModel model = result.model();
/// double goodnessOfFit = result.goodnessOfFit();
/// ```
///
/// @see NormalModelFitter
/// @see UniformModelFitter
/// @see EmpiricalModelFitter
/// @see BestFitSelector
public interface ComponentModelFitter {

    /// Result of fitting a distribution to data.
    ///
    /// @param model the fitted component model
    /// @param goodnessOfFit score indicating fit quality (lower is better)
    /// @param modelType the type identifier for this model
    record FitResult(ScalarModel model, double goodnessOfFit, String modelType) {

        /// Creates a fit result.
        ///
        /// @param model the fitted model
        /// @param goodnessOfFit the goodness-of-fit score (lower is better)
        /// @param modelType the model type identifier
        public FitResult {
            if (model == null) {
                throw new IllegalArgumentException("model cannot be null");
            }
            if (Double.isNaN(goodnessOfFit)) {
                throw new IllegalArgumentException("goodnessOfFit cannot be NaN");
            }
        }
    }

    /// Fits this distribution type to the observed data.
    ///
    /// @param values the observed values for one dimension
    /// @return the fit result containing the model and goodness-of-fit score
    FitResult fit(float[] values);

    /// Fits this distribution type using pre-computed statistics.
    ///
    /// This method is more efficient when statistics have already been
    /// computed for other purposes.
    ///
    /// @param stats pre-computed dimension statistics
    /// @param values the observed values (may be needed for some fitters)
    /// @return the fit result
    FitResult fit(DimensionStatistics stats, float[] values);

    /// Returns the model type identifier for this fitter.
    ///
    /// This should match [ScalarModel#getModelType()] for fitted models.
    ///
    /// @return the model type identifier (e.g., "gaussian", "uniform", "empirical")
    String getModelType();

    /// Returns whether this fitter is appropriate for bounded data.
    ///
    /// Some fitters (like Uniform) are designed for bounded ranges,
    /// while others (like Gaussian) work best with unbounded data.
    ///
    /// @return true if this fitter handles bounded data well
    default boolean supportsBoundedData() {
        return false;
    }

    /// Returns whether this fitter requires the raw data values.
    ///
    /// Some fitters (like Empirical) need the raw data to build a histogram,
    /// while others (like Gaussian) can work from statistics alone.
    ///
    /// @return true if this fitter needs raw data values
    default boolean requiresRawData() {
        return false;
    }
}
