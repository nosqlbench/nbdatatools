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

import io.nosqlbench.vshapes.model.GaussianComponentModel;

/// Sampler for Gaussian distributions, bound at construction.
///
/// Handles both unbounded and truncated Gaussian distributions using
/// inverse CDF transform sampling. All parameters are extracted at
/// construction time for efficient sampling.
public final class GaussianSampler implements ComponentSampler {

    private final double mean;
    private final double stdDev;
    private final TruncatedGaussianSampler truncatedSampler; // null if unbounded

    /// Creates a sampler bound to the given Gaussian model.
    ///
    /// @param model the Gaussian component model
    public GaussianSampler(GaussianComponentModel model) {
        this.mean = model.getMean();
        this.stdDev = model.getStdDev();

        if (model.isTruncated()) {
            this.truncatedSampler = new TruncatedGaussianSampler(
                mean, stdDev, model.lower(), model.upper()
            );
        } else {
            this.truncatedSampler = null;
        }
    }

    @Override
    public double sample(double u) {
        if (truncatedSampler != null) {
            return truncatedSampler.sample(u);
        } else {
            return mean + stdDev * InverseGaussianCDF.standardNormalQuantile(u);
        }
    }
}
