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

import io.nosqlbench.vshapes.model.PearsonType;

/**
 * Classifier for Pearson distribution system types.
 *
 * <p>The Pearson distribution system classifies continuous probability distributions
 * based on their skewness (β₁) and kurtosis (β₂). This classifier implements the
 * standard Pearson criterion to determine which distribution type best matches
 * observed data moments.
 *
 * <h2>Classification Criterion</h2>
 *
 * <p>The discriminant κ (kappa) is computed as:
 * <pre>{@code
 * κ = β₁(β₂ + 3)² / [4(2β₂ - 3β₁ - 6)(4β₂ - 3β₁)]
 * }</pre>
 *
 * <p>where β₁ = skewness² and β₂ = kurtosis (not excess kurtosis).
 *
 * <h2>Classification Regions</h2>
 *
 * <ul>
 *   <li><b>Type 0 (Normal):</b> β₁ ≈ 0 and β₂ ≈ 3</li>
 *   <li><b>Type I (Beta):</b> κ &lt; 0 (below Type III line)</li>
 *   <li><b>Type II (Symmetric Beta):</b> β₁ ≈ 0 and β₂ &lt; 3</li>
 *   <li><b>Type III (Gamma):</b> κ = 0 or very close</li>
 *   <li><b>Type IV:</b> 0 &lt; κ &lt; 1</li>
 *   <li><b>Type V (Inverse Gamma):</b> κ = 1</li>
 *   <li><b>Type VI (Beta Prime):</b> κ &gt; 1</li>
 *   <li><b>Type VII (Student's t):</b> β₁ ≈ 0 and β₂ &gt; 3</li>
 * </ul>
 *
 * @see PearsonType
 * @see <a href="https://en.wikipedia.org/wiki/Pearson_distribution">Pearson distribution - Wikipedia</a>
 */
public final class PearsonClassifier {

    /**
     * Tolerance for considering skewness as zero (symmetric distribution).
     */
    public static final double SKEWNESS_TOLERANCE = 0.1;

    /**
     * Tolerance for considering kurtosis as 3 (normal distribution).
     */
    public static final double KURTOSIS_TOLERANCE = 0.2;

    /**
     * Tolerance for κ comparisons to boundary values.
     */
    public static final double KAPPA_TOLERANCE = 0.05;

    private PearsonClassifier() {
        // Utility class
    }

    /**
     * Classifies the distribution type based on sample skewness and kurtosis.
     *
     * <p>Note: This method expects standard kurtosis (β₂), not excess kurtosis.
     * Standard kurtosis = excess kurtosis + 3.
     * For a normal distribution, standard kurtosis = 3.
     *
     * @param skewness the sample skewness (can be negative or positive)
     * @param kurtosis the sample kurtosis (standard, not excess)
     * @return the classified Pearson distribution type
     */
    public static PearsonType classify(double skewness, double kurtosis) {
        double beta1 = skewness * skewness;  // β₁ = skewness²
        double beta2 = kurtosis;              // β₂ = standard kurtosis

        // Check for symmetric distributions first (β₁ ≈ 0)
        if (beta1 < SKEWNESS_TOLERANCE * SKEWNESS_TOLERANCE) {
            return classifySymmetric(beta2);
        }

        // For asymmetric distributions, compute the discriminant criterion
        return classifyAsymmetric(beta1, beta2);
    }

    /**
     * Classifies the distribution type using excess kurtosis.
     *
     * <p>This is a convenience method that converts excess kurtosis to standard
     * kurtosis before classification.
     *
     * @param skewness the sample skewness
     * @param excessKurtosis the excess kurtosis (standard kurtosis - 3)
     * @return the classified Pearson distribution type
     */
    public static PearsonType classifyWithExcessKurtosis(double skewness, double excessKurtosis) {
        return classify(skewness, excessKurtosis + 3.0);
    }

    /**
     * Classifies symmetric distributions (skewness ≈ 0).
     */
    private static PearsonType classifySymmetric(double beta2) {
        if (Math.abs(beta2 - 3.0) < KURTOSIS_TOLERANCE) {
            // β₂ ≈ 3: Normal distribution
            return PearsonType.TYPE_0_NORMAL;
        } else if (beta2 < 3.0) {
            // β₂ < 3: Symmetric beta (platykurtic)
            return PearsonType.TYPE_II_SYMMETRIC_BETA;
        } else {
            // β₂ > 3: Student's t (leptokurtic)
            return PearsonType.TYPE_VII_STUDENT_T;
        }
    }

    /**
     * Classifies asymmetric distributions (skewness ≠ 0).
     *
     * <p>Uses the Pearson criterion κ to determine the type.
     */
    private static PearsonType classifyAsymmetric(double beta1, double beta2) {
        // Compute the criterion κ
        // κ = β₁(β₂ + 3)² / [4(2β₂ - 3β₁ - 6)(4β₂ - 3β₁)]

        double numerator = beta1 * Math.pow(beta2 + 3, 2);
        double denom1 = 2 * beta2 - 3 * beta1 - 6;
        double denom2 = 4 * beta2 - 3 * beta1;
        double denominator = 4 * denom1 * denom2;

        // Handle edge cases where denominator is zero or very small
        if (Math.abs(denominator) < 1e-10) {
            // On or near the Type III (Gamma) line
            return PearsonType.TYPE_III_GAMMA;
        }

        double kappa = numerator / denominator;

        // Classify based on κ value
        if (kappa < -KAPPA_TOLERANCE) {
            // κ < 0: Type I (Beta)
            return PearsonType.TYPE_I_BETA;
        } else if (Math.abs(kappa) <= KAPPA_TOLERANCE) {
            // κ ≈ 0: Type III (Gamma)
            return PearsonType.TYPE_III_GAMMA;
        } else if (kappa < 1.0 - KAPPA_TOLERANCE) {
            // 0 < κ < 1: Type IV
            return PearsonType.TYPE_IV;
        } else if (Math.abs(kappa - 1.0) <= KAPPA_TOLERANCE) {
            // κ ≈ 1: Type V (Inverse Gamma)
            return PearsonType.TYPE_V_INVERSE_GAMMA;
        } else {
            // κ > 1: Type VI (Beta Prime / F-distribution)
            return PearsonType.TYPE_VI_BETA_PRIME;
        }
    }

    /**
     * Computes the Pearson criterion κ (kappa).
     *
     * <p>The criterion is defined as:
     * <pre>{@code
     * κ = β₁(β₂ + 3)² / [4(2β₂ - 3β₁ - 6)(4β₂ - 3β₁)]
     * }</pre>
     *
     * @param skewness the sample skewness
     * @param kurtosis the sample kurtosis (standard, not excess)
     * @return the criterion value κ, or NaN if undefined
     */
    public static double computeKappa(double skewness, double kurtosis) {
        double beta1 = skewness * skewness;
        double beta2 = kurtosis;

        double numerator = beta1 * Math.pow(beta2 + 3, 2);

        // If numerator is zero (symmetric distribution), κ = 0
        if (Math.abs(numerator) < 1e-10) {
            return 0.0;
        }

        double denom1 = 2 * beta2 - 3 * beta1 - 6;
        double denom2 = 4 * beta2 - 3 * beta1;
        double denominator = 4 * denom1 * denom2;

        if (Math.abs(denominator) < 1e-10) {
            return Double.NaN;
        }

        return numerator / denominator;
    }

    /**
     * Provides detailed classification result including the criterion value.
     *
     * @param skewness the sample skewness
     * @param kurtosis the sample kurtosis (standard, not excess)
     * @return the classification result with details
     */
    public static ClassificationResult classifyDetailed(double skewness, double kurtosis) {
        double beta1 = skewness * skewness;
        double beta2 = kurtosis;
        double kappa = computeKappa(skewness, kurtosis);
        PearsonType type = classify(skewness, kurtosis);
        return new ClassificationResult(type, beta1, beta2, kappa);
    }

    /**
     * Detailed classification result including Pearson parameters.
     *
     * @param type the classified Pearson type
     * @param beta1 the computed β₁ (skewness²)
     * @param beta2 the computed β₂ (kurtosis)
     * @param kappa the Pearson criterion κ
     */
    public record ClassificationResult(
        PearsonType type,
        double beta1,
        double beta2,
        double kappa
    ) {
        /**
         * Returns whether this distribution is symmetric (β₁ ≈ 0).
         */
        public boolean isSymmetric() {
            return beta1 < SKEWNESS_TOLERANCE * SKEWNESS_TOLERANCE;
        }

        /**
         * Returns whether this distribution is platykurtic (β₂ &lt; 3).
         */
        public boolean isPlatykurtic() {
            return beta2 < 3.0 - KURTOSIS_TOLERANCE;
        }

        /**
         * Returns whether this distribution is leptokurtic (β₂ &gt; 3).
         */
        public boolean isLeptokurtic() {
            return beta2 > 3.0 + KURTOSIS_TOLERANCE;
        }

        /**
         * Returns whether this distribution is mesokurtic (β₂ ≈ 3).
         */
        public boolean isMesokurtic() {
            return Math.abs(beta2 - 3.0) <= KURTOSIS_TOLERANCE;
        }

        @Override
        public String toString() {
            return String.format(
                "ClassificationResult[type=%s, β₁=%.4f, β₂=%.4f, κ=%s]",
                type, beta1, beta2,
                Double.isNaN(kappa) ? "undefined" : String.format("%.4f", kappa)
            );
        }
    }
}
