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

import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class BestFitSelectorTest {

    @Test
    void testDefaultSelector() {
        BestFitSelector selector = BestFitSelector.defaultSelector();
        assertNotNull(selector);
        assertEquals(3, selector.getFitters().size());
    }

    @Test
    void testParametricOnlySelector() {
        BestFitSelector selector = BestFitSelector.parametricOnly();
        assertNotNull(selector);
        assertEquals(2, selector.getFitters().size());

        // Should not include empirical
        for (ComponentModelFitter fitter : selector.getFitters()) {
            assertNotEquals("empirical", fitter.getModelType());
        }
    }

    @Test
    void testSelectBestForNormalData() {
        Random random = new Random(12345);
        float[] values = new float[10000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) random.nextGaussian();
        }

        BestFitSelector selector = BestFitSelector.parametricOnly();
        ScalarModel best = selector.selectBest(values);

        assertNotNull(best);
        // Normal data should fit normal model best
        assertTrue(best instanceof NormalScalarModel,
            "Normal data should select normal model, got: " + best.getModelType());
    }

    @Test
    void testSelectBestForUniformData() {
        Random random = new Random(12345);
        float[] values = new float[10000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) random.nextDouble();
        }

        BestFitSelector selector = BestFitSelector.parametricOnly();
        ScalarModel best = selector.selectBest(values);

        assertNotNull(best);
        // Uniform data should fit Uniform model best
        assertTrue(best instanceof UniformScalarModel,
            "Uniform data should select Uniform model, got: " + best.getModelType());
    }

    @Test
    void testFitAllReturnsAllResults() {
        float[] values = new float[1000];
        Random random = new Random(12345);
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) random.nextGaussian();
        }

        BestFitSelector selector = BestFitSelector.defaultSelector();
        List<ComponentModelFitter.FitResult> results = selector.fitAll(values);

        assertEquals(3, results.size());

        // All should have valid scores
        for (ComponentModelFitter.FitResult result : results) {
            assertNotNull(result.model());
            assertTrue(Double.isFinite(result.goodnessOfFit()));
            assertNotNull(result.modelType());
        }
    }

    @Test
    void testSelectBestResultIncludesMetadata() {
        float[] values = new float[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i / 1000f;
        }

        BestFitSelector selector = BestFitSelector.defaultSelector();
        ComponentModelFitter.FitResult result = selector.selectBestResult(values);

        assertNotNull(result);
        assertNotNull(result.model());
        assertNotNull(result.modelType());
        assertTrue(Double.isFinite(result.goodnessOfFit()));
    }

    @Test
    void testSummarizeFits() {
        float[] values = new float[1000];
        Random random = new Random(12345);
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) random.nextGaussian();
        }

        BestFitSelector selector = BestFitSelector.defaultSelector();
        String summary = selector.summarizeFits(values);

        assertNotNull(summary);
        assertTrue(summary.contains("Fit Summary:"));
        assertTrue(summary.contains("normal"));
        assertTrue(summary.contains("uniform"));
        assertTrue(summary.contains("empirical"));
        assertTrue(summary.contains("(BEST)"));
    }

    @Test
    void testEmptyFittersThrows() {
        assertThrows(IllegalArgumentException.class, () -> new BestFitSelector(List.of()));
    }

    @Test
    void testNullFittersThrows() {
        assertThrows(NullPointerException.class, () -> new BestFitSelector(null));
    }

    @Test
    void testSelectBestNullValuesThrows() {
        BestFitSelector selector = BestFitSelector.defaultSelector();
        assertThrows(NullPointerException.class, () -> selector.selectBest(null));
    }

    @Test
    void testSelectBestEmptyValuesThrows() {
        BestFitSelector selector = BestFitSelector.defaultSelector();
        assertThrows(IllegalArgumentException.class, () -> selector.selectBest(new float[0]));
    }

    @Test
    void testEmpiricalPenaltyPrefersParametric() {
        // Create data that could fit either uniform or empirical well
        float[] values = new float[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i / 1000f;
        }

        // With default penalty, parametric should be preferred when fits are close
        BestFitSelector withPenalty = new BestFitSelector(List.of(
            new UniformModelFitter(),
            new EmpiricalModelFitter()
        ), 0.5);  // High penalty

        ScalarModel best = withPenalty.selectBest(values);
        // With high penalty, uniform should be preferred
        assertEquals("uniform", best.getModelType());
    }
}
