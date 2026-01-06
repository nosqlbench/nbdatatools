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
 * Pearson distribution system classification types.
 *
 * <h2>Pearson Distribution System</h2>
 *
 * <p>Karl Pearson's distribution system classifies continuous probability
 * distributions based on their first four moments, specifically skewness (β₁)
 * and kurtosis (β₂). The system encompasses many common distributions as
 * special or limiting cases.
 *
 * <h2>Classification Table</h2>
 *
 * <table border="1">
 * <caption>Pearson Distribution Types</caption>
 * <tr><th>Type</th><th>Distribution</th><th>β₁</th><th>β₂</th><th>Support</th></tr>
 * <tr><td>0</td><td>Normal</td><td>0</td><td>3</td><td>(-∞, +∞)</td></tr>
 * <tr><td>I</td><td>Beta</td><td>varies</td><td>&lt; 3</td><td>[a, b]</td></tr>
 * <tr><td>II</td><td>Symmetric Beta</td><td>0</td><td>&lt; 3</td><td>[a, b]</td></tr>
 * <tr><td>III</td><td>Gamma</td><td>varies</td><td>varies</td><td>[0, +∞)</td></tr>
 * <tr><td>IV</td><td>Pearson IV</td><td>varies</td><td>varies</td><td>(-∞, +∞)</td></tr>
 * <tr><td>V</td><td>Inverse Gamma</td><td>varies</td><td>varies</td><td>(0, +∞)</td></tr>
 * <tr><td>VI</td><td>Beta Prime (F)</td><td>varies</td><td>varies</td><td>(0, +∞)</td></tr>
 * <tr><td>VII</td><td>Student's t</td><td>0</td><td>&gt; 3</td><td>(-∞, +∞)</td></tr>
 * </table>
 *
 * <h2>Classification Criterion</h2>
 *
 * <p>The discriminant criterion κ is computed as:
 * <pre>{@code
 * κ = β₁(β₂ + 3)² / [4(2β₂ - 3β₁ - 6)(4β₂ - 3β₁)]
 * }</pre>
 *
 * <p>where β₁ = skewness² and β₂ = kurtosis (excess kurtosis + 3).
 *
 * @see NormalScalarModel
 * @see <a href="https://en.wikipedia.org/wiki/Pearson_distribution">Pearson distribution - Wikipedia</a>
 */
public enum PearsonType {

    /**
     * Type 0: Normal (Gaussian) distribution.
     *
     * <p>Characterized by β₁ = 0 (symmetric) and β₂ = 3 (mesokurtic).
     * This is the limiting case and most common distribution type.
     *
     * <p>Parameters: μ (mean), σ (standard deviation)
     * <p>Support: (-∞, +∞)
     */
    TYPE_0_NORMAL("normal", "Normal distribution", true, false),

    /**
     * Type I: Beta distribution.
     *
     * <p>Bounded distribution with flexible shape parameters.
     * Includes asymmetric beta distributions.
     *
     * <p>Parameters: α (shape1), β (shape2), a (lower), b (upper)
     * <p>Support: [a, b]
     */
    TYPE_I_BETA("beta", "Beta distribution", true, true),

    /**
     * Type II: Symmetric Beta distribution.
     *
     * <p>Special case of Type I where β₁ = 0 (symmetric).
     * Includes the uniform distribution as a special case (α = β = 1).
     *
     * <p>Parameters: α (shape), a (lower), b (upper)
     * <p>Support: [a, b]
     */
    TYPE_II_SYMMETRIC_BETA("symmetric-beta", "Symmetric Beta distribution", true, true),

    /**
     * Type III: Gamma distribution.
     *
     * <p>Models waiting times and positive skewed data.
     * Exponential and chi-squared are special cases.
     *
     * <p>Parameters: k (shape), θ (scale), γ (location shift)
     * <p>Support: [γ, +∞)
     */
    TYPE_III_GAMMA("gamma", "Gamma distribution", false, true),

    /**
     * Type IV: Pearson Type IV distribution.
     *
     * <p>Unbounded asymmetric distribution that covers regions
     * not reachable by other Pearson types.
     *
     * <p>Parameters: m, ν, a, λ
     * <p>Support: (-∞, +∞)
     */
    TYPE_IV("pearson-iv", "Pearson Type IV distribution", false, false),

    /**
     * Type V: Inverse Gamma distribution.
     *
     * <p>The reciprocal of a gamma-distributed variable.
     * Used in Bayesian statistics as a conjugate prior.
     *
     * <p>Parameters: α (shape), β (scale)
     * <p>Support: (0, +∞)
     */
    TYPE_V_INVERSE_GAMMA("inverse-gamma", "Inverse Gamma distribution", false, true),

    /**
     * Type VI: Beta Prime (F-distribution family).
     *
     * <p>Also known as the Beta distribution of the second kind.
     * The F-distribution is a special case.
     *
     * <p>Parameters: α, β (shape parameters)
     * <p>Support: (0, +∞)
     */
    TYPE_VI_BETA_PRIME("beta-prime", "Beta Prime (F) distribution", false, true),

    /**
     * Type VII: Student's t-distribution.
     *
     * <p>Symmetric heavy-tailed distribution used in hypothesis testing.
     * Converges to normal as degrees of freedom → ∞.
     *
     * <p>Parameters: ν (degrees of freedom), μ (location), σ (scale)
     * <p>Support: (-∞, +∞)
     */
    TYPE_VII_STUDENT_T("student-t", "Student's t distribution", true, false);

    private final String modelType;
    private final String description;
    private final boolean symmetric;
    private final boolean bounded;

    PearsonType(String modelType, String description, boolean symmetric, boolean bounded) {
        this.modelType = modelType;
        this.description = description;
        this.symmetric = symmetric;
        this.bounded = bounded;
    }

    /**
     * Returns the model type identifier used in serialization.
     *
     * @return the model type string (e.g., "normal", "beta", "gamma")
     */
    public String getModelType() {
        return modelType;
    }

    /**
     * Returns a human-readable description of this distribution type.
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns whether this distribution type is symmetric (β₁ = 0).
     *
     * @return true if the distribution is symmetric
     */
    public boolean isSymmetric() {
        return symmetric;
    }

    /**
     * Returns whether this distribution type has bounded support.
     *
     * @return true if the distribution has finite bounds on one or both sides
     */
    public boolean isBounded() {
        return bounded;
    }

    /**
     * Finds a PearsonType by its model type string.
     *
     * @param modelType the model type string
     * @return the corresponding PearsonType
     * @throws IllegalArgumentException if no matching type is found
     */
    public static PearsonType fromModelType(String modelType) {
        for (PearsonType type : values()) {
            if (type.modelType.equals(modelType)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown Pearson type: " + modelType);
    }
}
