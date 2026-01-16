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

import java.util.Arrays;
import java.util.Objects;

/**
 * Configuration model for a discrete vector space with per-dimension distribution models.
 *
 * <p>This is the primary implementation of {@link VectorModel}, providing a
 * second-order tensor model that defines a space of N unique vectors, each
 * with M dimensions.
 *
 * <h2>Purpose</h2>
 *
 * <p>This class defines the parameters for procedural vector generation:
 * <ul>
 *   <li><b>N</b> - The number of unique vectors that can be sampled</li>
 *   <li><b>M</b> - The dimensionality of each vector</li>
 *   <li><b>Per-dimension distributions</b> - ScalarModel for each dimension</li>
 * </ul>
 *
 * <h2>Tensor Hierarchy</h2>
 *
 * <p>VectorSpaceModel implements {@link VectorModel}, the second level in the
 * tensor model hierarchy:
 * <ul>
 *   <li>{@link ScalarModel} - First-order (single dimension)</li>
 *   <li><b>{@link VectorModel}</b> - Second-order (M dimensions) - this class</li>
 *   <li>{@link MatrixModel} - Third-order (K vector models)</li>
 * </ul>
 *
 * <h2>Heterogeneous Model Support</h2>
 *
 * <p>Each dimension can have a different distribution type:
 * <ul>
 *   <li>{@link NormalScalarModel} - Normal distribution (Pearson Type 0)</li>
 *   <li>{@link UniformScalarModel} - Uniform distribution</li>
 *   <li>{@link EmpiricalScalarModel} - Histogram-based distribution</li>
 *   <li>{@link CompositeScalarModel} - Mixture of distributions</li>
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
 *   │   dim 0: NormalScalarModel(μ₀, σ₀)                              │
 *   │   dim 1: UniformScalarModel(lower₁, upper₁)                     │
 *   │   dim 2: EmpiricalScalarModel(histogram₂)                       │
 *   │   ...                                                           │
 *   │   dim M-1: NormalScalarModel(μₘ₋₁, σₘ₋₁)                        │
 *   │                                                                 │
 *   │   Each dimension can have a different distribution type.        │
 *   └─────────────────────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>Usage Examples</h2>
 *
 * <pre>{@code
 * // Standard normal distribution across all 128 dimensions
 * VectorSpaceModel model1 = new VectorSpaceModel(1_000_000, 128);
 *
 * // Custom uniform Gaussian distribution across all dimensions
 * VectorSpaceModel model2 = new VectorSpaceModel(1_000_000, 128, 5.0, 2.0);
 *
 * // Heterogeneous: different model types per dimension
 * ScalarModel[] custom = {
 *     new NormalScalarModel(0.0, 1.0),           // dim 0: Normal
 *     new UniformScalarModel(0.0, 1.0),          // dim 1: Uniform
 *     new NormalScalarModel(-5.0, 0.5),          // dim 2: Normal
 * };
 * VectorSpaceModel model3 = new VectorSpaceModel(1_000_000, custom);
 * }</pre>
 *
 * @see VectorModel
 * @see ScalarModel
 * @see NormalScalarModel
 */
public final class VectorSpaceModel implements VectorModel, IsomorphicVectorModel {

    public static final String MODEL_TYPE = "vector_space";

    private final long uniqueVectors;
    private final int dimensions;
    private final ScalarModel[] scalarModels;

    /**
     * Constructs a vector space model with dimension-specific distribution models.
     *
     * <p>Supports heterogeneous models where each dimension can have a different
     * distribution type (Gaussian, Uniform, Empirical, etc.).
     *
     * @param uniqueVectors the number of unique vectors N in the space; must be positive
     * @param scalarModels the distribution model for each dimension; length determines M
     * @throws IllegalArgumentException if uniqueVectors is not positive or scalarModels is empty
     */
    public VectorSpaceModel(long uniqueVectors, ScalarModel[] scalarModels) {
        if (uniqueVectors <= 0) {
            throw new IllegalArgumentException("Number of unique vectors must be positive, got: " + uniqueVectors);
        }
        Objects.requireNonNull(scalarModels, "Scalar models cannot be null");
        if (scalarModels.length == 0) {
            throw new IllegalArgumentException("Scalar models cannot be empty");
        }
        this.uniqueVectors = uniqueVectors;
        this.dimensions = scalarModels.length;
        this.scalarModels = Arrays.copyOf(scalarModels, scalarModels.length);
    }

    /**
     * Constructs a vector space model with uniform normal distribution across all dimensions.
     *
     * <p>All M dimensions will share the same mean and standard deviation.
     *
     * @param uniqueVectors the number of unique vectors N in the space
     * @param dimensions the dimensionality M of the vector space
     * @param mean the mean for all component distributions
     * @param stdDev the standard deviation for all component distributions
     */
    public VectorSpaceModel(long uniqueVectors, int dimensions, double mean, double stdDev) {
        this(uniqueVectors, NormalScalarModel.uniformScalar(mean, stdDev, dimensions));
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
     * Constructs a vector space model with truncated normal distribution across all dimensions.
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
        this(uniqueVectors, NormalScalarModel.uniformScalar(mean, stdDev, lower, upper, dimensions));
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
    @Override
    public long uniqueVectors() {
        return uniqueVectors;
    }

    /**
     * Returns the dimensionality of the vector space.
     *
     * @return M, the number of dimensions in each generated vector
     */
    @Override
    public int dimensions() {
        return dimensions;
    }

    /**
     * Returns the scalar model for a specific dimension.
     *
     * @param dimension the dimension index (0-based, must be in [0, M-1])
     * @return the scalar model for that dimension
     * @throws IndexOutOfBoundsException if dimension is out of range
     */
    @Override
    public ScalarModel scalarModel(int dimension) {
        if (dimension < 0 || dimension >= dimensions) {
            throw new IndexOutOfBoundsException("Dimension " + dimension + " out of range [0, " + dimensions + ")");
        }
        return scalarModels[dimension];
    }

    /**
     * Returns all scalar models.
     *
     * <p>This is the preferred method for accessing per-dimension models
     * as part of the {@link VectorModel} interface.
     *
     * @return a defensive copy of the scalar models array
     */
    @Override
    public ScalarModel[] scalarModels() {
        return Arrays.copyOf(scalarModels, scalarModels.length);
    }

    /**
     * Returns whether all dimensions use the same {@link ScalarModel} implementation type.
     *
     * <p>When this returns {@code true}, sampler resolvers can use vectorized
     * sampling strategies that apply the same algorithm across all dimensions.
     *
     * @return true if all M dimensions share the same ScalarModel class
     */
    @Override
    public boolean isIsomorphic() {
        if (scalarModels.length == 0) return true;
        String firstType = scalarModels[0].getModelType();
        for (int i = 1; i < scalarModels.length; i++) {
            if (!scalarModels[i].getModelType().equals(firstType)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a representative {@link ScalarModel} instance from this model.
     *
     * <p>When {@link #isIsomorphic()} returns {@code true}, this provides a sample
     * instance that can be used to determine the appropriate sampler type.
     *
     * @return the ScalarModel for dimension 0, or null if the model is empty
     * @throws IllegalStateException if called when {@link #isIsomorphic()} is false
     */
    @Override
    public ScalarModel representativeScalarModel() {
        if (scalarModels.length == 0) return null;
        if (!isIsomorphic()) {
            throw new IllegalStateException("Cannot get representative model: model is heterogeneous");
        }
        return scalarModels[0];
    }

    /**
     * Returns the {@link ScalarModel} implementation class used across all dimensions.
     *
     * @return the Class of the ScalarModel implementation
     * @throws IllegalStateException if called when {@link #isIsomorphic()} is false
     */
    @Override
    public Class<? extends ScalarModel> scalarModelClass() {
        if (scalarModels.length == 0) {
            throw new IllegalStateException("Cannot get scalar model class: model is empty");
        }
        if (!isIsomorphic()) {
            throw new IllegalStateException("Cannot get scalar model class: model is heterogeneous");
        }
        return scalarModels[0].getClass();
    }

    /**
     * Returns whether all scalar models are Normal.
     *
     * @return true if all dimensions use NormalScalarModel
     */
    public boolean isAllNormal() {
        for (ScalarModel model : scalarModels) {
            if (!(model instanceof NormalScalarModel)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns all scalar models as NormalScalarModel array.
     *
     * <p>This is a convenience method for cases where you need
     * to work directly with NormalScalarModel[].
     *
     * @return array of NormalScalarModel
     * @throws IllegalStateException if not all models are Normal
     */
    public NormalScalarModel[] normalScalarModels() {
        if (!isAllNormal()) {
            throw new IllegalStateException("Not all scalar models are Normal. Use scalarModels() instead.");
        }
        NormalScalarModel[] result = new NormalScalarModel[scalarModels.length];
        for (int i = 0; i < scalarModels.length; i++) {
            result[i] = (NormalScalarModel) scalarModels[i];
        }
        return result;
    }

    /// Returns the model type identifier for serialization.
    ///
    /// @return "vector_space"
    @Override
    public String getModelType() {
        return MODEL_TYPE;
    }
}
