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

/// Forward Gaussian CDF (cumulative distribution function) using error function.
///
/// # Purpose
///
/// This class provides the forward CDF Φ(x) = P(X ≤ x) for the standard normal
/// distribution. It is used in conjunction with `InverseGaussianCDF` (in virtdata)
/// to implement truncated normal sampling.
///
/// # Mathematical Definition
///
/// ```text
///                    1      x
///   Φ(x) = ─────── ∫   e^(-t²/2) dt  =  ½[1 + erf(x/√2)]
///          √(2π)   -∞
///
///   Standard normal CDF:
///     Φ(-∞) = 0
///     Φ(-3) ≈ 0.00135
///     Φ(-2) ≈ 0.0228
///     Φ(-1) ≈ 0.1587
///     Φ(0)  = 0.5
///     Φ(1)  ≈ 0.8413
///     Φ(2)  ≈ 0.9772
///     Φ(3)  ≈ 0.99865
///     Φ(+∞) = 1
/// ```
///
/// # Role in Truncated Normal Sampling
///
/// ```text
/// For truncated normal on [lower, upper]:
///
///   1. Standardize bounds:  a = (lower - μ)/σ,  b = (upper - μ)/σ
///   2. Compute CDF at bounds:  Φ(a), Φ(b)
///   3. Probability mass:  Z = Φ(b) - Φ(a)
///   4. Rescale input:  u' = Φ(a) + u·Z    (maps u∈(0,1) to u'∈(Φ(a),Φ(b)))
///   5. Invert:  x = μ + σ·Φ⁻¹(u')         (gives x∈[lower, upper])
/// ```
///
/// @see GaussianComponentModel
/// @see GaussianScalarModel
public final class GaussianCDF {

    private GaussianCDF() {
        // Utility class
    }

    /// Computes the standard normal CDF Φ(x) = P(X ≤ x) for X ~ N(0,1).
    ///
    /// Uses the relationship Φ(x) = ½[1 + erf(x/√2)] where erf is the
    /// error function, approximated using Horner's method.
    ///
    /// @param x the value at which to evaluate the CDF
    /// @return the probability P(X ≤ x) for standard normal X
    public static double standardNormalCDF(double x) {
        return 0.5 * (1.0 + erf(x / Math.sqrt(2.0)));
    }

    /// Computes the CDF for a Gaussian with specified mean and stdDev.
    ///
    /// @param x the value at which to evaluate the CDF
    /// @param mean the mean (μ) of the distribution
    /// @param stdDev the standard deviation (σ) of the distribution
    /// @return the probability P(X ≤ x) for X ~ N(mean, stdDev²)
    public static double cdf(double x, double mean, double stdDev) {
        return standardNormalCDF((x - mean) / stdDev);
    }

    /// Error function approximation using Horner's method.
    ///
    /// Uses the Abramowitz and Stegun approximation (7.1.26) which provides
    /// accuracy to approximately 1.5 × 10⁻⁷.
    ///
    /// ```text
    ///                    2     ∞
    ///   erf(x) = ───── ∫  e^(-t²) dt
    ///            √π    0
    ///
    ///   erf(-x) = -erf(x)  (odd function)
    ///   erf(0) = 0
    ///   erf(∞) = 1
    /// ```
    ///
    /// @param x the argument
    /// @return erf(x)
    private static double erf(double x) {
        // Abramowitz and Stegun 7.1.26 approximation
        // |error| < 1.5 × 10⁻⁷

        double sign = x < 0 ? -1.0 : 1.0;
        x = Math.abs(x);

        // Constants
        double a1 = 0.254829592;
        double a2 = -0.284496736;
        double a3 = 1.421413741;
        double a4 = -1.453152027;
        double a5 = 1.061405429;
        double p = 0.3275911;

        double t = 1.0 / (1.0 + p * x);
        double t2 = t * t;
        double t3 = t2 * t;
        double t4 = t3 * t;
        double t5 = t4 * t;

        double y = 1.0 - (a1 * t + a2 * t2 + a3 * t3 + a4 * t4 + a5 * t5) * Math.exp(-x * x);

        return sign * y;
    }
}
