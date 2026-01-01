package io.nosqlbench.vshapes.model;

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

/**
 * Empirical distribution scalar model based on observed data histogram.
 *
 * <p>This class is functionally identical to {@link EmpiricalComponentModel}.
 * It provides the preferred naming convention for the tensor model hierarchy.
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>EmpiricalScalarModel is a first-order tensor model (ScalarModel) that
 * represents a single-dimensional empirical (histogram-based) distribution:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension) - this class</li>
 *   <li>{@link VectorModel} - Second-order (M dimensions)</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Algorithm</h2>
 *
 * <p>The model builds a piecewise linear approximation of the CDF:
 * <ol>
 *   <li>Bin the observed data into a histogram</li>
 *   <li>Convert counts to a cumulative distribution</li>
 *   <li>For sampling: binary search to find the bin, then linear interpolation</li>
 * </ol>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Build from observed data
 * float[] observedValues = ...;
 * EmpiricalScalarModel model = EmpiricalScalarModel.fromData(observedValues, 100);
 * }</pre>
 *
 * @see ScalarModel
 * @see VectorModel
 * @see EmpiricalComponentModel
 */
public final class EmpiricalScalarModel extends EmpiricalComponentModel {

    /**
     * Constructs an empirical scalar model from precomputed histogram data.
     *
     * @param binEdges the bin edges (length = binCount + 1)
     * @param cdf the cumulative distribution values at each edge (length = binCount + 1)
     * @param mean the mean of the distribution
     * @param stdDev the standard deviation of the distribution
     */
    public EmpiricalScalarModel(double[] binEdges, double[] cdf, double mean, double stdDev) {
        super(binEdges, cdf, mean, stdDev);
    }

    /**
     * Builds an empirical scalar model from observed data.
     *
     * @param values the observed values
     * @param binCount the number of histogram bins
     * @return an EmpiricalScalarModel fitted to the data
     */
    public static EmpiricalScalarModel fromData(float[] values, int binCount) {
        EmpiricalComponentModel base = EmpiricalComponentModel.fromData(values, binCount);
        return new EmpiricalScalarModel(
            base.getBinEdges(),
            base.getCdf(),
            base.getMean(),
            base.getStdDev()
        );
    }

    /**
     * Builds an empirical scalar model from observed data with default bin count.
     *
     * <p>Uses Sturges' rule for bin count selection, clamped to [10, 1000].
     *
     * @param values the observed values
     * @return an EmpiricalScalarModel fitted to the data
     */
    public static EmpiricalScalarModel fromData(float[] values) {
        EmpiricalComponentModel base = EmpiricalComponentModel.fromData(values);
        return new EmpiricalScalarModel(
            base.getBinEdges(),
            base.getCdf(),
            base.getMean(),
            base.getStdDev()
        );
    }

    @Override
    public String toString() {
        return "EmpiricalScalarModel[bins=" + getBinCount() + ", range=[" + getMin() + ", " + getMax() + "]]";
    }
}
