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
import io.nosqlbench.vshapes.model.UniformScalarModel;

/**
 * Fits a uniform distribution to observed data.
 *
 * <h2>Algorithm</h2>
 *
 * <p>For a uniform distribution U(a, b), the parameters are estimated as:
 * <ul>
 *   <li>a = min(data) - epsilon (slight extension to avoid boundary issues)</li>
 *   <li>b = max(data) + epsilon</li>
 * </ul>
 *
 * <h2>Goodness of Fit</h2>
 *
 * <p>Inherits uniform scoring from {@link AbstractParametricFitter} using
 * the raw Kolmogorov-Smirnov D-statistic via the model's CDF.
 *
 * <h2>When to Use</h2>
 *
 * <p>Uniform distributions are appropriate when:
 * <ul>
 *   <li>Data is bounded within a known range</li>
 *   <li>Values are spread evenly across the range</li>
 *   <li>There's no central tendency (no mean-seeking behavior)</li>
 * </ul>
 *
 * @see UniformScalarModel
 * @see AbstractParametricFitter
 */
public final class UniformModelFitter extends AbstractParametricFitter {

    private final double boundaryExtension;
    private final Double explicitLowerBound;
    private final Double explicitUpperBound;

    /**
     * Creates a uniform model fitter with default boundary extension.
     */
    public UniformModelFitter() {
        this(0.0);  // No extension by default
    }

    /**
     * Creates a uniform model fitter with specified boundary extension.
     *
     * @param boundaryExtension fraction of range to extend beyond observed min/max
     */
    public UniformModelFitter(double boundaryExtension) {
        if (boundaryExtension < 0 || boundaryExtension > 0.5) {
            throw new IllegalArgumentException("boundaryExtension must be in [0, 0.5]");
        }
        this.boundaryExtension = boundaryExtension;
        this.explicitLowerBound = null;
        this.explicitUpperBound = null;
    }

    /**
     * Creates a uniform model fitter with explicit bounds.
     *
     * <p>This is useful for normalized vectors where the bounds are known
     * to be [-1, 1] regardless of the observed data range.
     *
     * @param lowerBound explicit lower bound
     * @param upperBound explicit upper bound
     */
    public UniformModelFitter(double lowerBound, double upperBound) {
        this.boundaryExtension = 0.0;
        this.explicitLowerBound = lowerBound;
        this.explicitUpperBound = upperBound;
    }

    /**
     * Creates a uniform model fitter configured for L2-normalized vectors.
     *
     * <p>Normalized vectors have values bounded in [-1, 1], so this
     * creates a fitter with explicit bounds.
     *
     * @return a fitter configured for normalized vector data
     */
    public static UniformModelFitter forNormalizedVectors() {
        return new UniformModelFitter(-1.0, 1.0);
    }

    @Override
    protected ScalarModel estimateParameters(DimensionStatistics stats, float[] values) {
        // Use explicit bounds if provided
        if (explicitLowerBound != null && explicitUpperBound != null) {
            return new UniformScalarModel(explicitLowerBound, explicitUpperBound);
        }

        double min = stats.min();
        double max = stats.max();

        // Apply boundary extension
        double range = max - min;
        if (range <= 0) {
            range = 1.0;  // Handle constant data
        }

        double extension = range * boundaryExtension;
        double lower = min - extension;
        double upper = max + extension;

        return new UniformScalarModel(lower, upper);
    }

    @Override
    public String getModelType() {
        return UniformScalarModel.MODEL_TYPE;
    }

    @Override
    public boolean supportsBoundedData() {
        return true;
    }
}
