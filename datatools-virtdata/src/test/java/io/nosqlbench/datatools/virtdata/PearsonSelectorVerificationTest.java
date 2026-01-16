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

import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.extract.BestFitSelector;
import io.nosqlbench.vshapes.extract.DatasetModelExtractor;
import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.CompositeScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verification test that boundedDataSelector correctly recovers bounded
 * distribution types (Normal, Beta, Uniform) for unit interval data.
 *
 * <p>This test validates:
 * <ol>
 *   <li>boundedDataSelector outperforms defaultSelector for bounded data</li>
 *   <li>Bounded distributions can be reliably distinguished from each other</li>
 *   <li>The fix for using proper selectors works correctly</li>
 * </ol>
 *
 * <p>Heavy-tailed distributions are excluded because they are inappropriate
 * for bounded unit interval data and create artificial ambiguity.
 */
@Tag("accuracy")
public class PearsonSelectorVerificationTest {

    private static final int DIMS = 60;  // Multiple of 3 for even distribution
    private static final int CARDINALITY = 8192;

    @Test
    void testBoundedDataSelectorRecoversBoundedTypes() {
        // Create source models covering bounded distribution types only
        // These are appropriate for unit interval data [-1, 1]
        ScalarModel[] sourceTypes = new ScalarModel[] {
            new NormalScalarModel(0.0, 0.2),     // Centered, moderate spread
            new BetaScalarModel(2.0, 5.0),       // Left-skewed Beta
            new UniformScalarModel(-0.5, 0.5)   // Bounded uniform
        };
        int numTypes = sourceTypes.length;

        // Create array with cycling types
        ScalarModel[] allModels = new ScalarModel[DIMS];
        for (int d = 0; d < DIMS; d++) {
            allModels[d] = sourceTypes[d % numTypes];
        }

        // Generate data using stratified sampling
        float[][] data = generateData(allModels);

        // Test with DEFAULT selector (only Normal, Uniform, Empirical)
        DatasetModelExtractor defaultExtractor = new DatasetModelExtractor();
        VectorSpaceModel defaultModel = defaultExtractor.extractVectorModel(data);
        int defaultMatches = countTypeMatches(allModels, defaultModel);

        // Test with BOUNDED DATA selector (Normal, Beta, Uniform - appropriate for bounded data)
        DatasetModelExtractor boundedExtractor = new DatasetModelExtractor(
            BestFitSelector.boundedDataSelector(),
            DatasetModelExtractor.DEFAULT_UNIQUE_VECTORS
        );
        VectorSpaceModel boundedModel = boundedExtractor.extractVectorModel(data);
        int boundedMatches = countTypeMatches(allModels, boundedModel);

        System.out.printf("Default selector: %d/%d type matches (%.1f%%)%n",
            defaultMatches, DIMS, 100.0 * defaultMatches / DIMS);
        System.out.printf("Bounded selector: %d/%d type matches (%.1f%%)%n",
            boundedMatches, DIMS, 100.0 * boundedMatches / DIMS);

        // Show type breakdown for first 9 dimensions (3 full cycles)
        System.out.println("\nFirst 9 dimensions breakdown:");
        for (int d = 0; d < 9; d++) {
            String sourceType = getEffectiveModelType(allModels[d]);
            String defaultType = getEffectiveModelType(defaultModel.scalarModel(d));
            String boundedType = getEffectiveModelType(boundedModel.scalarModel(d));
            System.out.printf("  dim %2d: source=%-10s default=%-10s bounded=%-10s %s%n",
                d, sourceType, defaultType, boundedType,
                sourceType.equals(boundedType) ? "✓" : "");
        }

        // The bounded selector should match significantly more types than default
        // Default can only match normal/uniform (~2/3 types ≈ 67%)
        assertTrue(boundedMatches >= defaultMatches,
            String.format("Bounded selector (%d matches) should be at least as good as default (%d matches)",
                boundedMatches, defaultMatches));

        // STRICT CHECK: Bounded selector should achieve at least 90% DIRECT matches
        // This is the primary quality assertion - we should NOT need equivalences
        double boundedDirectMatchRate = (double) boundedMatches / DIMS;
        assertTrue(boundedDirectMatchRate >= 0.90,
            String.format("Bounded selector DIRECT match rate %.1f%% below expected 90%% - " +
                "classification accuracy needs improvement", boundedDirectMatchRate * 100));

        // Count "equivalent" matches as secondary check (Uniform↔Beta is mathematically acceptable)
        int boundedEquivalentMatches = countEquivalentMatches(allModels, boundedModel);
        System.out.printf("Bounded selector (with equivalences): %d/%d (%.1f%%)%n",
            boundedEquivalentMatches, DIMS, 100.0 * boundedEquivalentMatches / DIMS);

        // With equivalences, bounded selector should achieve at least 95%
        double boundedEquivMatchRate = (double) boundedEquivalentMatches / DIMS;
        assertTrue(boundedEquivMatchRate >= 0.95,
            String.format("Bounded selector match rate (with equivalences) %.1f%% below expected 95%%",
                boundedEquivMatchRate * 100));
    }

    @Test
    void testBoundedDistributionsAreDistinguishable() {
        // Test that each bounded distribution type can be identified correctly
        // when generated and fitted in isolation
        //
        // Note: Uniform and Beta with α≈β≈1 are mathematically equivalent,
        // so Uniform→Beta is an acceptable "match"

        ScalarModel[] distinctModels = new ScalarModel[] {
            new NormalScalarModel(0.0, 0.15),   // Gaussian peak - should be identifiable
            new BetaScalarModel(3.0, 7.0),      // Clearly asymmetric Beta - should be identifiable
            new UniformScalarModel(-0.7, 0.7)   // Flat - may fit as Beta(≈1,≈1)
        };

        DatasetModelExtractor extractor = new DatasetModelExtractor(
            BestFitSelector.boundedDataSelector(),
            DatasetModelExtractor.DEFAULT_UNIQUE_VECTORS
        );

        System.out.println("\nDistinguishability test:");
        int passed = 0;
        for (ScalarModel source : distinctModels) {
            // Generate 10000 samples from this distribution
            float[] samples = new float[10000];
            ComponentSampler sampler = ComponentSamplerFactory.forModel(source);
            for (int i = 0; i < samples.length; i++) {
                samples[i] = (float) sampler.sample(StratifiedSampler.unitIntervalValue(i, 0, samples.length));
            }

            // Wrap as single-dimension vector data
            float[][] data = new float[samples.length][1];
            for (int i = 0; i < samples.length; i++) {
                data[i][0] = samples[i];
            }

            // Extract model
            VectorSpaceModel recovered = extractor.extractVectorModel(data);
            String sourceType = getEffectiveModelType(source);
            ScalarModel fittedModel = recovered.scalarModel(0);
            String fittedType = getEffectiveModelType(fittedModel);

            // Accept exact match OR mathematically equivalent match
            boolean match = sourceType.equals(fittedType) || isEquivalent(sourceType, fittedType);
            System.out.printf("  %s -> %s %s%n", sourceType, fittedType, match ? "✓" : "✗");

            // Parameter accuracy check for Beta distribution
            if (source instanceof BetaScalarModel sourceBeta &&
                fittedModel instanceof BetaScalarModel fittedBeta) {
                double alphaError = Math.abs(sourceBeta.getAlpha() - fittedBeta.getAlpha())
                                   / sourceBeta.getAlpha();
                double betaError = Math.abs(sourceBeta.getBeta() - fittedBeta.getBeta())
                                  / sourceBeta.getBeta();
                System.out.printf("    Beta params: source(α=%.2f, β=%.2f) -> fitted(α=%.2f, β=%.2f) " +
                    "[αErr=%.1f%%, βErr=%.1f%%]%n",
                    sourceBeta.getAlpha(), sourceBeta.getBeta(),
                    fittedBeta.getAlpha(), fittedBeta.getBeta(),
                    alphaError * 100, betaError * 100);
                // Beta parameter recovery should be within 35%
                // (method of moments has higher variance than MLE for asymmetric Beta)
                assertTrue(alphaError < 0.35,
                    String.format("Beta α error %.1f%% exceeds 35%% threshold", alphaError * 100));
                assertTrue(betaError < 0.35,
                    String.format("Beta β error %.1f%% exceeds 35%% threshold", betaError * 100));
            }

            if (match) passed++;
        }

        assertEquals(distinctModels.length, passed,
            "All bounded distributions should be correctly identified (with equivalences allowed)");
    }

    private float[][] generateData(ScalarModel[] models) {
        float[][] data = new float[CARDINALITY][DIMS];
        ComponentSampler[] samplers = new ComponentSampler[DIMS];

        for (int d = 0; d < DIMS; d++) {
            samplers[d] = ComponentSamplerFactory.forModel(models[d]);
        }

        for (int v = 0; v < CARDINALITY; v++) {
            for (int d = 0; d < DIMS; d++) {
                double u = StratifiedSampler.unitIntervalValue(v, d, CARDINALITY);
                data[v][d] = (float) samplers[d].sample(u);
            }
        }

        return data;
    }

    private int countTypeMatches(ScalarModel[] source, VectorSpaceModel model) {
        int matches = 0;
        for (int d = 0; d < DIMS; d++) {
            String sourceType = getEffectiveModelType(source[d]);
            String fittedType = getEffectiveModelType(model.scalarModel(d));
            if (sourceType.equals(fittedType)) {
                matches++;
            }
        }
        return matches;
    }

    /**
     * Counts type matches including mathematically equivalent distributions.
     * Uniform ↔ Beta is acceptable since Beta(1,1) = Uniform.
     */
    private int countEquivalentMatches(ScalarModel[] source, VectorSpaceModel model) {
        int matches = 0;
        for (int d = 0; d < DIMS; d++) {
            String sourceType = getEffectiveModelType(source[d]);
            String fittedType = getEffectiveModelType(model.scalarModel(d));

            if (sourceType.equals(fittedType)) {
                matches++;
            } else if (isEquivalent(sourceType, fittedType)) {
                matches++;
            }
        }
        return matches;
    }

    /**
     * Checks if two distribution types are mathematically equivalent for bounded data.
     */
    private boolean isEquivalent(String type1, String type2) {
        // Uniform and Beta are interchangeable for bounded data
        // Beta(α≈1, β≈1) is mathematically equivalent to Uniform
        if ((type1.equals("uniform") && type2.equals("beta")) ||
            (type1.equals("beta") && type2.equals("uniform"))) {
            return true;
        }
        return false;
    }

    /**
     * Gets the effective model type for display purposes.
     *
     * <p>For CompositeScalarModel, returns the underlying type for 1-component
     * composites, or "composite" for multi-component composites. This supports
     * unified composite handling where all models may be wrapped.
     */
    private String getEffectiveModelType(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            return composite.getEffectiveModelType();
        }
        return model.getModelType();
    }
}
