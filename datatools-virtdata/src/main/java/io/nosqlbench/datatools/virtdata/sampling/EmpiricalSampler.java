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

package io.nosqlbench.datatools.virtdata.sampling;

import io.nosqlbench.vshapes.model.EmpiricalScalarModel;

/// Sampler for Empirical distributions, bound at construction.
///
/// Uses binary search to find the histogram bin and linear interpolation
/// within the bin to produce samples that match the observed distribution.
public final class EmpiricalSampler implements ComponentSampler {

    private final double[] binEdges;
    private final double[] cdf;
    private final int binCount;
    private final double min;
    private final double max;

    /// Creates a sampler bound to the given Empirical model.
    ///
    /// @param model the Empirical component model
    public EmpiricalSampler(EmpiricalScalarModel model) {
        this.binEdges = model.getBinEdges();
        this.cdf = model.getCdf();
        this.binCount = model.getBinCount();
        this.min = model.getMin();
        this.max = model.getMax();
    }

    @Override
    public double sample(double u) {
        if (u <= 0.0) return min;
        if (u >= 1.0) return max;

        // Binary search to find the bin
        int lo = 0;
        int hi = binCount;
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (cdf[mid + 1] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Linear interpolation within the bin
        double cdfLo = cdf[lo];
        double cdfHi = cdf[lo + 1];
        double edgeLo = binEdges[lo];
        double edgeHi = binEdges[lo + 1];

        if (cdfHi == cdfLo) {
            return (edgeLo + edgeHi) / 2.0;
        }

        double t = (u - cdfLo) / (cdfHi - cdfLo);
        return edgeLo + t * (edgeHi - edgeLo);
    }
}
