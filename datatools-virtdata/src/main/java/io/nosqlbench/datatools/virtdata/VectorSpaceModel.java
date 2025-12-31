package io.nosqlbench.datatools.virtdata;

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

import java.util.Arrays;
import java.util.Objects;

/**
 * Configuration model for a discrete vector space with per-dimension Gaussian distributions.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class defines the parameters for procedural vector generation:
 * <ul>
 *   <li><b>N</b> - The number of unique vectors that can be sampled</li>
 *   <li><b>M</b> - The dimensionality of each vector</li>
 *   <li><b>Per-dimension distributions</b> - Gaussian parameters (μ, σ) for each dimension</li>
 * </ul>
 *
 * <h2>Model Structure</h2>
 *
 * <pre>{@code
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                      VECTOR SPACE MODEL                            │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │ SPACE PARAMETERS                                                │
 *   │   N = uniqueVectors     (how many unique vectors, e.g., 1M)     │
 *   │   M = dimensions        (vector size, e.g., 128)                │
 *   └─────────────────────────────────────────────────────────────────┘
 *
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │ PER-DIMENSION DISTRIBUTIONS                                     │
 *   │                                                                 │
 *   │   dim 0: GaussianComponentModel(μ₀, σ₀)                         │
 *   │   dim 1: GaussianComponentModel(μ₁, σ₁)                         │
 *   │   dim 2: GaussianComponentModel(μ₂, σ₂)                         │
 *   │   ...                                                           │
 *   │   dim M-1: GaussianComponentModel(μₘ₋₁, σₘ₋₁)                   │
 *   │                                                                 │
 *   │   Each dimension can have different mean and stdDev,            │
 *   │   or all can share the same (uniform) distribution.             │
 *   └─────────────────────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>Ordinal to Vector Mapping</h2>
 *
 * <pre>{@code
 *   ORDINAL SPACE                    VECTOR SPACE
 *   [0, N-1]                         M-dimensional
 *
 *      0  ─────────────────────►  [v₀₀, v₀₁, ..., v₀ₘ₋₁]
 *      1  ─────────────────────►  [v₁₀, v₁₁, ..., v₁ₘ₋₁]
 *      2  ─────────────────────►  [v₂₀, v₂₁, ..., v₂ₘ₋₁]
 *      ⋮                             ⋮
 *    N-1  ─────────────────────►  [vₙ₋₁₀, vₙ₋₁₁, ..., vₙ₋₁ₘ₋₁]
 *
 *   Each ordinal maps to exactly one vector (bijective for [0,N-1])
 *   Ordinals outside [0,N-1] wrap: ordinal % N
 * }</pre>
 *
 * <h2>Usage Examples</h2>
 *
 * <pre>{@code
 * // Standard normal distribution across all 128 dimensions
 * VectorSpaceModel model1 = new VectorSpaceModel(1_000_000, 128);
 *
 * // Custom uniform distribution (mean=5, stdDev=2) across all dimensions
 * VectorSpaceModel model2 = new VectorSpaceModel(1_000_000, 128, 5.0, 2.0);
 *
 * // Per-dimension custom distributions
 * GaussianComponentModel[] custom = {
 *     new GaussianComponentModel(0.0, 1.0),   // dim 0: standard normal
 *     new GaussianComponentModel(10.0, 2.0),  // dim 1: mean=10, stdDev=2
 *     new GaussianComponentModel(-5.0, 0.5),  // dim 2: mean=-5, stdDev=0.5
 * };
 * VectorSpaceModel model3 = new VectorSpaceModel(1_000_000, custom);
 * }</pre>
 *
 * @see VectorGen
 * @see GaussianComponentModel
 */
public final class VectorSpaceModel {

    private final long uniqueVectors;
    private final int dimensions;
    private final GaussianComponentModel[] componentModels;

    /**
     * Constructs a vector space model with component-specific Gaussian distributions.
     *
     * <pre>{@code
     * componentModels[] ──► VectorSpaceModel
     *   [G₀, G₁, ..., Gₘ₋₁]     N=uniqueVectors, M=componentModels.length
     * }</pre>
     *
     * @param uniqueVectors the number of unique vectors N in the space; must be positive
     * @param componentModels the Gaussian model for each dimension; length determines M
     * @throws IllegalArgumentException if uniqueVectors is not positive or componentModels is empty
     */
    public VectorSpaceModel(long uniqueVectors, GaussianComponentModel[] componentModels) {
        if (uniqueVectors <= 0) {
            throw new IllegalArgumentException("Number of unique vectors must be positive, got: " + uniqueVectors);
        }
        Objects.requireNonNull(componentModels, "Component models cannot be null");
        if (componentModels.length == 0) {
            throw new IllegalArgumentException("Component models cannot be empty");
        }
        this.uniqueVectors = uniqueVectors;
        this.dimensions = componentModels.length;
        this.componentModels = Arrays.copyOf(componentModels, componentModels.length);
    }

    /**
     * Constructs a vector space model with uniform Gaussian distribution across all dimensions.
     *
     * <p>All M dimensions will share the same mean and standard deviation.
     *
     * @param uniqueVectors the number of unique vectors N in the space
     * @param dimensions the dimensionality M of the vector space
     * @param mean the mean for all component distributions
     * @param stdDev the standard deviation for all component distributions
     */
    public VectorSpaceModel(long uniqueVectors, int dimensions, double mean, double stdDev) {
        this(uniqueVectors, GaussianComponentModel.uniform(mean, stdDev, dimensions));
    }

    /**
     * Constructs a vector space model with standard normal distribution across all dimensions.
     *
     * <p>All M dimensions will have mean=0 and stdDev=1 (standard normal N(0,1)).
     *
     * @param uniqueVectors the number of unique vectors N in the space
     * @param dimensions the dimensionality M of the vector space
     */
    public VectorSpaceModel(long uniqueVectors, int dimensions) {
        this(uniqueVectors, dimensions, 0.0, 1.0);
    }

    /**
     * Constructs a vector space model with truncated Gaussian distribution across all dimensions.
     *
     * <p>All M dimensions will share the same mean, standard deviation, and truncation bounds.
     * The truncation ensures all generated values fall within [lower, upper].
     *
     * @param uniqueVectors the number of unique vectors N in the space
     * @param dimensions the dimensionality M of the vector space
     * @param mean the mean for all component distributions
     * @param stdDev the standard deviation for all component distributions
     * @param lower the lower truncation bound
     * @param upper the upper truncation bound
     */
    public VectorSpaceModel(long uniqueVectors, int dimensions, double mean, double stdDev,
                            double lower, double upper) {
        this(uniqueVectors, GaussianComponentModel.uniformTruncated(mean, stdDev, lower, upper, dimensions));
    }

    /**
     * Creates a vector space model with standard normal N(0,1) truncated to [-1, 1].
     *
     * <p>This is a common configuration for normalized vector embeddings.
     *
     * @param uniqueVectors the number of unique vectors N in the space
     * @param dimensions the dimensionality M of the vector space
     * @return a VectorSpaceModel with unit-bounded standard normal distribution
     */
    public static VectorSpaceModel unitBounded(long uniqueVectors, int dimensions) {
        return new VectorSpaceModel(uniqueVectors, dimensions, 0.0, 1.0, -1.0, 1.0);
    }

    /**
     * Returns the number of unique vectors in the space.
     *
     * @return N, the number of unique vectors that can be generated
     */
    public long uniqueVectors() {
        return uniqueVectors;
    }

    /**
     * Returns the dimensionality of the vector space.
     *
     * @return M, the number of dimensions in each generated vector
     */
    public int dimensions() {
        return dimensions;
    }

    /**
     * Returns the Gaussian model for a specific dimension.
     *
     * @param dimension the dimension index (0-based, must be in [0, M-1])
     * @return the Gaussian component model for that dimension
     * @throws IndexOutOfBoundsException if dimension is out of range
     */
    public GaussianComponentModel componentModel(int dimension) {
        if (dimension < 0 || dimension >= dimensions) {
            throw new IndexOutOfBoundsException("Dimension " + dimension + " out of range [0, " + dimensions + ")");
        }
        return componentModels[dimension];
    }

    /**
     * Returns all component models.
     *
     * @return a defensive copy of the component models array
     */
    public GaussianComponentModel[] componentModels() {
        return Arrays.copyOf(componentModels, componentModels.length);
    }
}
