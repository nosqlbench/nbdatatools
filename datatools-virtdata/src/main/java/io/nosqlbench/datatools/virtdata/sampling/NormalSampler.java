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

import io.nosqlbench.vshapes.model.NormalScalarModel;

/// Sampler for normal distributions, bound at construction.
///
/// Handles both unbounded and truncated normal distributions using
/// inverse CDF transform sampling. All parameters are extracted at
/// construction time for efficient sampling.
public final class NormalSampler implements ComponentSampler {

    private final double mean;
    private final double stdDev;
    private final TruncatedNormalSampler truncatedSampler; // null if unbounded

    /// Creates a sampler bound to the given normal model.
    ///
    /// @param model the normal component model
    public NormalSampler(NormalScalarModel model) {
        this.mean = model.getMean();
        this.stdDev = model.getStdDev();

        if (model.isTruncated()) {
            this.truncatedSampler = new TruncatedNormalSampler(
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
            return mean + stdDev * InverseNormalCDF.standardNormalQuantile(u);
        }
    }
}
