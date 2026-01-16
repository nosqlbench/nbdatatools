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

import java.util.ArrayList;
import java.util.List;

/// Reduces composite models to their simplest equivalent form.
///
/// ## Purpose
///
/// This class implements model reduction rules that simplify composite
/// distributions without changing their statistical properties. This is
/// essential for:
/// - Round-trip verification (comparing source and fitted models)
/// - Canonical representation (unique form for equivalent models)
/// - Efficiency (simpler models generate faster)
///
/// ## Reduction Rules
///
/// The following reductions are applied:
///
/// ### 1. Beta(1,1) to Uniform
/// `Beta(1,1,a,b)` is mathematically identical to `Uniform(a,b)`.
///
/// ### 2. Identical Uniform Merging
/// `w₁·U[a,b] + w₂·U[a,b]` → `(w₁+w₂)·U[a,b]`
///
/// ### 3. Adjacent Uniform Merging (equal density)
/// `w₁·U[a,b] + w₂·U[b,c]` where densities match → merged uniform
///
/// ### 4. Full-Range Uniform Detection
/// Multiple uniforms covering a known range (e.g., [-1,1] for normalized
/// vectors) with equal density → single `U[min,max]`
///
/// ### 5. Near-Identical Component Merging
/// Components with parameters within tolerance are merged.
///
/// ### 6. Single-Component Unwrapping
/// Composite with one component → unwrapped to that component.
///
/// ## Usage
///
/// ```java
/// CompositeScalarModel composite = ...;
/// ScalarModel reduced = CompositeModelReducer.reduce(composite);
///
/// // Or with known bounds for normalized data
/// ScalarModel reduced = CompositeModelReducer.reduce(composite, -1.0, 1.0);
/// ```
///
/// @see CompositeScalarModel#toCanonicalForm()
public final class CompositeModelReducer {

    /// Tolerance for considering bounds as "same" (relative)
    private static final double BOUNDS_TOLERANCE = 0.001;

    /// Tolerance for Beta parameters to be considered "effectively uniform"
    private static final double BETA_UNIFORM_TOLERANCE = 0.15;

    /// Tolerance for considering parameters as "near-identical"
    private static final double PARAM_MERGE_TOLERANCE = 0.05;

    /// Tolerance for uniform density matching (for adjacent merging)
    private static final double DENSITY_TOLERANCE = 0.10;

    private CompositeModelReducer() {
        // Static utility class
    }

    /// Reduces a composite model to its simplest equivalent form.
    ///
    /// @param composite the composite model to reduce
    /// @return the reduced model (may be non-composite if fully simplified)
    public static ScalarModel reduce(CompositeScalarModel composite) {
        return reduce(composite, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    /// Reduces a composite model with known data bounds.
    ///
    /// When bounds are known (e.g., [-1,1] for normalized vectors), additional
    /// reductions are possible such as detecting full-range uniform coverage.
    ///
    /// @param composite the composite model to reduce
    /// @param knownLower known lower bound of data range (or -Infinity if unknown)
    /// @param knownUpper known upper bound of data range (or +Infinity if unknown)
    /// @return the reduced model (may be non-composite if fully simplified)
    public static ScalarModel reduce(CompositeScalarModel composite,
                                      double knownLower, double knownUpper) {
        if (composite.getComponentCount() == 0) {
            return composite;
        }

        // Step 1: Convert Beta(1,1) to Uniform
        List<ScalarModel> components = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        boolean anyConversions = convertBetaToUniform(composite, components, weights);

        // Step 2: Merge identical uniforms
        int beforeMerge = components.size();
        mergeIdenticalUniforms(components, weights);
        boolean mergedIdentical = components.size() < beforeMerge;

        // Step 3: Merge adjacent uniforms with equal density
        beforeMerge = components.size();
        mergeAdjacentUniforms(components, weights);
        boolean mergedAdjacent = components.size() < beforeMerge;

        // Step 4: Check for full-range uniform coverage
        if (isFiniteBounds(knownLower, knownUpper)) {
            ScalarModel fullRange = checkFullRangeUniform(components, weights, knownLower, knownUpper);
            if (fullRange != null) {
                return fullRange;
            }
        }

        // Step 5: Merge near-identical parametric components
        beforeMerge = components.size();
        mergeNearIdenticalComponents(components, weights);
        boolean mergedNearIdentical = components.size() < beforeMerge;

        // Step 6: Unwrap single component
        if (components.size() == 1) {
            return components.get(0);
        }

        // Step 7: Check if all remaining components are effectively the same
        if (components.size() > 1 && allComponentsEquivalent(components)) {
            return components.get(0);
        }

        // Rebuild composite if we made any changes
        boolean anyChanges = anyConversions || mergedIdentical || mergedAdjacent || mergedNearIdentical;
        if (anyChanges || components.size() != composite.getComponentCount()) {
            return new CompositeScalarModel(components, toDoubleArray(weights));
        }

        return composite;
    }

    /// Converts Beta(1,1) distributions to equivalent Uniform.
    ///
    /// @return true if any conversions were made
    private static boolean convertBetaToUniform(CompositeScalarModel composite,
                                                 List<ScalarModel> outComponents,
                                                 List<Double> outWeights) {
        ScalarModel[] origComponents = composite.getScalarModels();
        double[] origWeights = composite.getWeights();
        boolean anyConversions = false;

        for (int i = 0; i < origComponents.length; i++) {
            ScalarModel comp = origComponents[i];

            if (comp instanceof BetaScalarModel beta && beta.isEffectivelyUniform()) {
                // Convert to Uniform
                outComponents.add(new UniformScalarModel(beta.getLower(), beta.getUpper()));
                anyConversions = true;
            } else if (comp instanceof CompositeScalarModel nested) {
                // Recursively reduce nested composites
                outComponents.add(reduce(nested));
                anyConversions = true;  // May have changed
            } else {
                outComponents.add(comp);
            }
            outWeights.add(origWeights[i]);
        }

        return anyConversions;
    }

    /// Merges uniform distributions with identical bounds.
    private static void mergeIdenticalUniforms(List<ScalarModel> components,
                                                List<Double> weights) {
        for (int i = 0; i < components.size(); i++) {
            if (!(components.get(i) instanceof UniformScalarModel u1)) continue;

            for (int j = components.size() - 1; j > i; j--) {
                if (!(components.get(j) instanceof UniformScalarModel u2)) continue;

                if (boundsMatch(u1.getLower(), u2.getLower()) &&
                    boundsMatch(u1.getUpper(), u2.getUpper())) {
                    // Merge: add weights, remove duplicate
                    weights.set(i, weights.get(i) + weights.get(j));
                    components.remove(j);
                    weights.remove(j);
                }
            }
        }
    }

    /// Merges adjacent uniform distributions with matching density.
    private static void mergeAdjacentUniforms(List<ScalarModel> components,
                                               List<Double> weights) {
        // Sort uniforms by lower bound for adjacency detection
        List<Integer> uniformIndices = new ArrayList<>();
        for (int i = 0; i < components.size(); i++) {
            if (components.get(i) instanceof UniformScalarModel) {
                uniformIndices.add(i);
            }
        }

        if (uniformIndices.size() < 2) return;

        // Sort by lower bound
        uniformIndices.sort((a, b) -> {
            UniformScalarModel ua = (UniformScalarModel) components.get(a);
            UniformScalarModel ub = (UniformScalarModel) components.get(b);
            return Double.compare(ua.getLower(), ub.getLower());
        });

        // Check for adjacent pairs that can be merged
        List<Integer> toRemove = new ArrayList<>();
        for (int k = 0; k < uniformIndices.size() - 1; k++) {
            int i = uniformIndices.get(k);
            int j = uniformIndices.get(k + 1);

            if (toRemove.contains(i) || toRemove.contains(j)) continue;

            UniformScalarModel u1 = (UniformScalarModel) components.get(i);
            UniformScalarModel u2 = (UniformScalarModel) components.get(j);

            // Check if adjacent (u1.upper ≈ u2.lower)
            if (!boundsMatch(u1.getUpper(), u2.getLower())) continue;

            // Check if densities match: w1/(b-a) ≈ w2/(d-c)
            double density1 = weights.get(i) / (u1.getUpper() - u1.getLower());
            double density2 = weights.get(j) / (u2.getUpper() - u2.getLower());

            if (Math.abs(density1 - density2) / Math.max(density1, density2) < DENSITY_TOLERANCE) {
                // Merge: extend u1 to cover u2's range
                double newWeight = weights.get(i) + weights.get(j);
                UniformScalarModel merged = new UniformScalarModel(u1.getLower(), u2.getUpper());
                components.set(i, merged);
                weights.set(i, newWeight);
                toRemove.add(j);
            }
        }

        // Remove merged components (in reverse order to preserve indices)
        toRemove.sort((a, b) -> b - a);
        for (int idx : toRemove) {
            components.remove(idx);
            weights.remove(idx);
        }
    }

    /// Checks if uniforms collectively cover the full known range uniformly.
    private static ScalarModel checkFullRangeUniform(List<ScalarModel> components,
                                                      List<Double> weights,
                                                      double knownLower,
                                                      double knownUpper) {
        // Collect all uniform components
        List<UniformScalarModel> uniforms = new ArrayList<>();
        List<Double> uniformWeights = new ArrayList<>();
        double nonUniformWeight = 0;

        for (int i = 0; i < components.size(); i++) {
            if (components.get(i) instanceof UniformScalarModel u) {
                uniforms.add(u);
                uniformWeights.add(weights.get(i));
            } else {
                nonUniformWeight += weights.get(i);
            }
        }

        // If there are non-uniform components, can't simplify to single uniform
        if (nonUniformWeight > 0.01) return null;
        if (uniforms.isEmpty()) return null;

        // Check if uniforms cover [knownLower, knownUpper] completely
        double totalWeight = uniformWeights.stream().mapToDouble(d -> d).sum();

        // Sort by lower bound
        uniforms.sort((a, b) -> Double.compare(a.getLower(), b.getLower()));

        // Check coverage
        double coverage = knownLower;
        boolean fullCoverage = true;
        double expectedDensity = totalWeight / (knownUpper - knownLower);

        for (int i = 0; i < uniforms.size(); i++) {
            UniformScalarModel u = uniforms.get(i);

            // Gap in coverage?
            if (u.getLower() > coverage + BOUNDS_TOLERANCE * (knownUpper - knownLower)) {
                fullCoverage = false;
                break;
            }

            // Density matches?
            double density = uniformWeights.get(i) / (u.getUpper() - u.getLower());
            if (Math.abs(density - expectedDensity) / expectedDensity > DENSITY_TOLERANCE) {
                fullCoverage = false;
                break;
            }

            coverage = Math.max(coverage, u.getUpper());
        }

        // Did we cover the full range?
        if (fullCoverage && coverage >= knownUpper - BOUNDS_TOLERANCE * (knownUpper - knownLower)) {
            return new UniformScalarModel(knownLower, knownUpper);
        }

        return null;
    }

    /// Merges near-identical parametric components.
    private static void mergeNearIdenticalComponents(List<ScalarModel> components,
                                                      List<Double> weights) {
        for (int i = 0; i < components.size(); i++) {
            for (int j = components.size() - 1; j > i; j--) {
                if (areNearIdentical(components.get(i), components.get(j))) {
                    // Merge weights, keep first component
                    weights.set(i, weights.get(i) + weights.get(j));
                    components.remove(j);
                    weights.remove(j);
                }
            }
        }
    }

    /// Checks if two components are near-identical.
    private static boolean areNearIdentical(ScalarModel a, ScalarModel b) {
        if (!a.getClass().equals(b.getClass())) return false;

        if (a instanceof NormalScalarModel n1 && b instanceof NormalScalarModel n2) {
            return relativeClose(n1.getMean(), n2.getMean(), PARAM_MERGE_TOLERANCE) &&
                   relativeClose(n1.getStdDev(), n2.getStdDev(), PARAM_MERGE_TOLERANCE);
        }

        if (a instanceof UniformScalarModel u1 && b instanceof UniformScalarModel u2) {
            return boundsMatch(u1.getLower(), u2.getLower()) &&
                   boundsMatch(u1.getUpper(), u2.getUpper());
        }

        if (a instanceof BetaScalarModel b1 && b instanceof BetaScalarModel b2) {
            return relativeClose(b1.getAlpha(), b2.getAlpha(), PARAM_MERGE_TOLERANCE) &&
                   relativeClose(b1.getBeta(), b2.getBeta(), PARAM_MERGE_TOLERANCE) &&
                   boundsMatch(b1.getLower(), b2.getLower()) &&
                   boundsMatch(b1.getUpper(), b2.getUpper());
        }

        if (a instanceof GammaScalarModel g1 && b instanceof GammaScalarModel g2) {
            return relativeClose(g1.getShape(), g2.getShape(), PARAM_MERGE_TOLERANCE) &&
                   relativeClose(g1.getScale(), g2.getScale(), PARAM_MERGE_TOLERANCE);
        }

        if (a instanceof StudentTScalarModel t1 && b instanceof StudentTScalarModel t2) {
            return relativeClose(t1.getDegreesOfFreedom(), t2.getDegreesOfFreedom(), PARAM_MERGE_TOLERANCE) &&
                   relativeClose(t1.getLocation(), t2.getLocation(), PARAM_MERGE_TOLERANCE) &&
                   relativeClose(t1.getScale(), t2.getScale(), PARAM_MERGE_TOLERANCE);
        }

        return false;
    }

    /// Checks if all components are equivalent (can be reduced to single model).
    private static boolean allComponentsEquivalent(List<ScalarModel> components) {
        if (components.size() < 2) return true;

        ScalarModel first = components.get(0);
        for (int i = 1; i < components.size(); i++) {
            if (!areNearIdentical(first, components.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean boundsMatch(double a, double b) {
        if (a == b) return true;
        double range = Math.max(Math.abs(a), Math.abs(b));
        if (range < 1e-10) return Math.abs(a - b) < 1e-10;
        return Math.abs(a - b) / range < BOUNDS_TOLERANCE;
    }

    private static boolean relativeClose(double a, double b, double tolerance) {
        if (a == b) return true;
        double max = Math.max(Math.abs(a), Math.abs(b));
        if (max < 1e-10) return Math.abs(a - b) < 1e-10;
        return Math.abs(a - b) / max < tolerance;
    }

    private static boolean isFiniteBounds(double lower, double upper) {
        return Double.isFinite(lower) && Double.isFinite(upper) && lower < upper;
    }

    private static double[] toDoubleArray(List<Double> list) {
        double[] arr = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }

    /// Result of reduction analysis (for debugging/reporting).
    ///
    /// @param originalComponents number of components before reduction
    /// @param reducedComponents number of components after reduction
    /// @param reductionsApplied list of reduction types applied
    public record ReductionResult(
        int originalComponents,
        int reducedComponents,
        List<String> reductionsApplied
    ) {
        /// Returns true if any reductions were made.
        public boolean wasReduced() {
            return reducedComponents < originalComponents;
        }
    }

    /// Analyzes what reductions would be applied without actually reducing.
    ///
    /// @param composite the composite model to analyze
    /// @param knownLower known lower bound (or -Infinity)
    /// @param knownUpper known upper bound (or +Infinity)
    /// @return analysis result describing potential reductions
    public static ReductionResult analyze(CompositeScalarModel composite,
                                           double knownLower, double knownUpper) {
        List<String> reductions = new ArrayList<>();
        int original = composite.getComponentCount();

        // Check for Beta(1,1) -> Uniform
        for (ScalarModel comp : composite.getScalarModels()) {
            if (comp instanceof BetaScalarModel beta && beta.isEffectivelyUniform()) {
                reductions.add("Beta(1,1)->Uniform");
                break;
            }
        }

        // Check for identical uniforms
        ScalarModel[] comps = composite.getScalarModels();
        for (int i = 0; i < comps.length; i++) {
            if (!(comps[i] instanceof UniformScalarModel u1)) continue;
            for (int j = i + 1; j < comps.length; j++) {
                if (!(comps[j] instanceof UniformScalarModel u2)) continue;
                if (boundsMatch(u1.getLower(), u2.getLower()) &&
                    boundsMatch(u1.getUpper(), u2.getUpper())) {
                    reductions.add("Identical uniforms merged");
                    break;
                }
            }
        }

        ScalarModel reduced = reduce(composite, knownLower, knownUpper);
        int reducedCount = reduced instanceof CompositeScalarModel c ? c.getComponentCount() : 1;

        return new ReductionResult(original, reducedCount, reductions);
    }
}
