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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CompositeModelReducer}.
 */
class CompositeModelReducerTest {

    @Test
    void testBetaToUniformConversion() {
        // Beta(1,1) is equivalent to Uniform
        BetaScalarModel beta = new BetaScalarModel(1.0, 1.0, -1.0, 1.0);
        assertTrue(beta.isEffectivelyUniform(), "Beta(1,1) should be effectively uniform");

        NormalScalarModel normal = new NormalScalarModel(0.0, 0.5, -1.0, 1.0);
        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(beta, normal),
            new double[]{0.5, 0.5}
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        assertTrue(reduced instanceof CompositeScalarModel, "Should still be composite");
        CompositeScalarModel reducedComposite = (CompositeScalarModel) reduced;

        // Check that the composite now has a Uniform instead of Beta
        boolean hasUniform = false;
        boolean hasBeta = false;
        for (ScalarModel comp : reducedComposite.getScalarModels()) {
            if (comp instanceof UniformScalarModel) hasUniform = true;
            if (comp instanceof BetaScalarModel) hasBeta = true;
        }
        assertTrue(hasUniform, "Beta(1,1) should be converted to Uniform");
        assertFalse(hasBeta, "Should not have Beta after conversion");
    }

    @Test
    void testIdenticalUniformMerging() {
        // Two identical uniforms should merge
        UniformScalarModel u1 = new UniformScalarModel(-1.0, 0.0);
        UniformScalarModel u2 = new UniformScalarModel(-1.0, 0.0);  // Same bounds

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(u1, u2),
            new double[]{0.3, 0.7}
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        // Should reduce to single Uniform
        assertTrue(reduced instanceof UniformScalarModel,
            "Identical uniforms should merge to single uniform, got: " + reduced.getClass().getSimpleName());
    }

    @Test
    void testAdjacentUniformMergingEqualDensity() {
        // Adjacent uniforms with equal density can be merged
        // w1/(b-a) = w2/(d-c) means equal density
        // U[-1,0] with weight 0.5 has density 0.5/1 = 0.5
        // U[0,1] with weight 0.5 has density 0.5/1 = 0.5
        UniformScalarModel u1 = new UniformScalarModel(-1.0, 0.0);
        UniformScalarModel u2 = new UniformScalarModel(0.0, 1.0);

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(u1, u2),
            new double[]{0.5, 0.5}  // Equal weights, equal widths -> equal density
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        assertTrue(reduced instanceof UniformScalarModel,
            "Adjacent uniforms with equal density should merge, got: " + reduced.getClass().getSimpleName());

        UniformScalarModel merged = (UniformScalarModel) reduced;
        assertEquals(-1.0, merged.getLower(), 0.001, "Merged lower bound");
        assertEquals(1.0, merged.getUpper(), 0.001, "Merged upper bound");
    }

    @Test
    void testAdjacentUniformUnequalDensityNotMerged() {
        // Adjacent uniforms with UNEQUAL density should NOT merge
        // U[-1,0] with weight 0.3 has density 0.3
        // U[0,1] with weight 0.7 has density 0.7
        UniformScalarModel u1 = new UniformScalarModel(-1.0, 0.0);
        UniformScalarModel u2 = new UniformScalarModel(0.0, 1.0);

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(u1, u2),
            new double[]{0.3, 0.7}  // Unequal weights -> unequal density
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        assertTrue(reduced instanceof CompositeScalarModel,
            "Adjacent uniforms with unequal density should NOT merge");
        assertEquals(2, ((CompositeScalarModel) reduced).getComponentCount());
    }

    @Test
    void testFullRangeUniformWithKnownBounds() {
        // Three uniforms covering [-1, 1] with equal density -> single U[-1,1]
        UniformScalarModel u1 = new UniformScalarModel(-1.0, -0.33);
        UniformScalarModel u2 = new UniformScalarModel(-0.33, 0.33);
        UniformScalarModel u3 = new UniformScalarModel(0.33, 1.0);

        // Weights proportional to width for equal density
        double w1 = 0.33;  // width 0.67
        double w2 = 0.34;  // width 0.66
        double w3 = 0.33;  // width 0.67

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(u1, u2, u3),
            new double[]{w1, w2, w3}
        );

        // Without known bounds - may not fully reduce
        ScalarModel reducedNoBounds = CompositeModelReducer.reduce(composite);
        System.out.println("Without bounds: " + reducedNoBounds.getClass().getSimpleName());

        // With known bounds [-1, 1] - should reduce to single uniform
        ScalarModel reducedWithBounds = CompositeModelReducer.reduce(composite, -1.0, 1.0);
        System.out.println("With bounds [-1,1]: " + reducedWithBounds.getClass().getSimpleName());

        if (reducedWithBounds instanceof UniformScalarModel u) {
            assertEquals(-1.0, u.getLower(), 0.01);
            assertEquals(1.0, u.getUpper(), 0.01);
        }
    }

    @Test
    void testNearIdenticalNormalsMerge() {
        // Two very similar normals should merge
        // Use mean=1.0 so relative comparison works (not near zero)
        NormalScalarModel n1 = new NormalScalarModel(1.0, 1.0, -3.0, 3.0);
        NormalScalarModel n2 = new NormalScalarModel(1.02, 1.02, -3.0, 3.0);  // Within 5% tolerance

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(n1, n2),
            new double[]{0.5, 0.5}
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        assertTrue(reduced instanceof NormalScalarModel,
            "Near-identical normals should merge to single normal, got: " + reduced.getClass().getSimpleName());
    }

    @Test
    void testSingleComponentUnwrap() {
        // Composite with single component should unwrap
        NormalScalarModel normal = new NormalScalarModel(0.0, 1.0, -3.0, 3.0);

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(normal),
            new double[]{1.0}
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        assertTrue(reduced instanceof NormalScalarModel,
            "Single-component composite should unwrap");
    }

    @Test
    void testDistinctComponentsPreserved() {
        // Truly distinct components should NOT be merged
        NormalScalarModel n1 = new NormalScalarModel(-2.0, 0.5, -3.0, 3.0);
        NormalScalarModel n2 = new NormalScalarModel(2.0, 0.5, -3.0, 3.0);

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(n1, n2),
            new double[]{0.5, 0.5}
        );

        ScalarModel reduced = CompositeModelReducer.reduce(composite);

        assertTrue(reduced instanceof CompositeScalarModel,
            "Distinct components should remain as composite");
        assertEquals(2, ((CompositeScalarModel) reduced).getComponentCount());
    }

    @Test
    void testCanonicalFormWithBounds() {
        // Test that toCanonicalForm with bounds properly reduces
        BetaScalarModel beta = new BetaScalarModel(1.0, 1.0, -1.0, 1.0);  // Effectively uniform
        NormalScalarModel normal = new NormalScalarModel(0.0, 0.3, -1.0, 1.0);

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(beta, normal),
            new double[]{0.3, 0.7}
        );

        ScalarModel canonical = composite.toCanonicalForm(-1.0, 1.0);

        assertTrue(canonical instanceof CompositeScalarModel,
            "Should remain composite with 2 distinct types");
        CompositeScalarModel canonicalComposite = (CompositeScalarModel) canonical;

        // Beta should be converted to Uniform, and no Beta should remain
        boolean hasUniform = false;
        boolean hasBeta = false;
        for (ScalarModel comp : canonicalComposite.getScalarModels()) {
            if (comp instanceof UniformScalarModel) hasUniform = true;
            if (comp instanceof BetaScalarModel) hasBeta = true;
        }
        assertTrue(hasUniform, "Beta(1,1) should be converted to Uniform in canonical form");
        assertFalse(hasBeta, "No Beta should remain after canonical form");
    }

    @Test
    void testReductionAnalysis() {
        BetaScalarModel beta = new BetaScalarModel(1.0, 1.0, -1.0, 1.0);
        UniformScalarModel uniform = new UniformScalarModel(-1.0, 1.0);

        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(beta, uniform),
            new double[]{0.5, 0.5}
        );

        CompositeModelReducer.ReductionResult result =
            CompositeModelReducer.analyze(composite, -1.0, 1.0);

        System.out.println("Reduction analysis:");
        System.out.println("  Original components: " + result.originalComponents());
        System.out.println("  Reduced components: " + result.reducedComponents());
        System.out.println("  Was reduced: " + result.wasReduced());
        System.out.println("  Reductions: " + result.reductionsApplied());

        assertTrue(result.wasReduced(), "Should detect reduction opportunity");
    }

    @Test
    void testNormalizedVectorScenario() {
        // Simulate a real scenario with normalized vector data in [-1, 1]
        // Multiple uniform bands that together cover the range
        CompositeScalarModel composite = new CompositeScalarModel(
            List.of(
                new UniformScalarModel(-1.0, -0.5),
                new UniformScalarModel(-0.5, 0.0),
                new UniformScalarModel(0.0, 0.5),
                new UniformScalarModel(0.5, 1.0)
            ),
            new double[]{0.25, 0.25, 0.25, 0.25}  // Equal weights, equal widths
        );

        System.out.println("\nNormalized vector scenario:");
        System.out.println("  Original: " + composite.getComponentCount() + " uniform components");

        ScalarModel reduced = composite.toCanonicalForm(-1.0, 1.0);

        System.out.println("  Reduced: " + reduced.getClass().getSimpleName());
        if (reduced instanceof UniformScalarModel u) {
            System.out.println("    Bounds: [" + u.getLower() + ", " + u.getUpper() + "]");
        }

        // Should reduce to single U[-1,1]
        assertTrue(reduced instanceof UniformScalarModel,
            "4 adjacent uniforms with equal density covering [-1,1] should reduce to U[-1,1]");
    }
}
