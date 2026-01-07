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

import io.nosqlbench.vshapes.model.EmpiricalScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
public class ModelFitterTest {

    @Test
    void testNormalFitterOnNormalData() {
        Random random = new Random(12345);
        float[] values = new float[10000];
        double trueMean = 5.0;
        double trueStdDev = 2.0;

        for (int i = 0; i < values.length; i++) {
            values[i] = (float) (random.nextGaussian() * trueStdDev + trueMean);
        }

        NormalModelFitter fitter = new NormalModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(values);

        assertNotNull(result);
        assertEquals("normal", result.modelType());
        assertTrue(result.model() instanceof NormalScalarModel);

        NormalScalarModel model = (NormalScalarModel) result.model();
        assertEquals(trueMean, model.getMean(), 0.1);
        assertEquals(trueStdDev, model.getStdDev(), 0.1);

        // Good fit should have low goodness-of-fit value
        assertTrue(result.goodnessOfFit() < 2.0, "Normal data should have good fit to normal model");
    }

    @Test
    void testUniformFitterOnUniformData() {
        float[] values = new float[10000];
        double trueLower = 2.0;
        double trueUpper = 8.0;

        Random random = new Random(12345);
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) (random.nextDouble() * (trueUpper - trueLower) + trueLower);
        }

        UniformModelFitter fitter = new UniformModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(values);

        assertNotNull(result);
        assertEquals("uniform", result.modelType());
        assertTrue(result.model() instanceof UniformScalarModel);

        UniformScalarModel model = (UniformScalarModel) result.model();
        assertEquals(trueLower, model.getLower(), 0.1);
        assertEquals(trueUpper, model.getUpper(), 0.1);

        // Good fit should have low goodness-of-fit value
        assertTrue(result.goodnessOfFit() < 2.0, "Uniform data should have good fit to Uniform model");
    }

    @Test
    void testEmpiricalFitter() {
        Random random = new Random(12345);
        float[] values = new float[10000];

        // Generate bimodal data that's hard to fit with parametric models
        for (int i = 0; i < values.length; i++) {
            if (random.nextBoolean()) {
                values[i] = (float) (random.nextGaussian() * 0.5 - 2.0);
            } else {
                values[i] = (float) (random.nextGaussian() * 0.5 + 2.0);
            }
        }

        EmpiricalModelFitter fitter = new EmpiricalModelFitter();
        ComponentModelFitter.FitResult result = fitter.fit(values);

        assertNotNull(result);
        assertEquals("empirical", result.modelType());
        assertTrue(result.model() instanceof EmpiricalScalarModel);

        // Verify model properties (sampling is tested in virtdata module)
        EmpiricalScalarModel model = (EmpiricalScalarModel) result.model();
        assertTrue(model.getBinCount() > 0, "Should have bins");
        assertTrue(Double.isFinite(model.getMean()), "Mean should be finite");
        assertTrue(Double.isFinite(model.getStdDev()), "StdDev should be finite");
        assertTrue(Double.isFinite(model.getMin()), "Should have finite min");
        assertTrue(Double.isFinite(model.getMax()), "Should have finite max");
    }

    @Test
    void testEmpiricalFitterWithBinCount() {
        float[] values = new float[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) i / values.length;
        }

        EmpiricalModelFitter fitter = new EmpiricalModelFitter(50);
        ComponentModelFitter.FitResult result = fitter.fit(values);

        assertNotNull(result);
        assertEquals("empirical", result.modelType());
    }

    @Test
    void testNormalFitterModelType() {
        NormalModelFitter fitter = new NormalModelFitter();
        assertEquals("normal", fitter.getModelType());
        assertTrue(fitter.supportsBoundedData()); // With truncation detection enabled
    }

    @Test
    void testUniformFitterModelType() {
        UniformModelFitter fitter = new UniformModelFitter();
        assertEquals("uniform", fitter.getModelType());
        assertTrue(fitter.supportsBoundedData());
    }

    @Test
    void testEmpiricalFitterModelType() {
        EmpiricalModelFitter fitter = new EmpiricalModelFitter();
        assertEquals("empirical", fitter.getModelType());
        assertTrue(fitter.supportsBoundedData());
        assertTrue(fitter.requiresRawData());
    }

    @Test
    void testFittersHandleNullValues() {
        assertThrows(NullPointerException.class, () -> new NormalModelFitter().fit(null));
        assertThrows(NullPointerException.class, () -> new UniformModelFitter().fit(null));
        assertThrows(NullPointerException.class, () -> new EmpiricalModelFitter().fit(null));
    }

    @Test
    void testFittersHandleEmptyValues() {
        assertThrows(IllegalArgumentException.class, () -> new NormalModelFitter().fit(new float[0]));
        assertThrows(IllegalArgumentException.class, () -> new UniformModelFitter().fit(new float[0]));
        assertThrows(IllegalArgumentException.class, () -> new EmpiricalModelFitter().fit(new float[0]));
    }

    @Test
    void testEmpiricalFitterInvalidBinCount() {
        assertThrows(IllegalArgumentException.class, () -> new EmpiricalModelFitter(1));
        assertThrows(IllegalArgumentException.class, () -> new EmpiricalModelFitter(0));
        assertThrows(IllegalArgumentException.class, () -> new EmpiricalModelFitter(-1));
    }

    @Test
    void testUniformFitterInvalidBoundaryExtension() {
        assertThrows(IllegalArgumentException.class, () -> new UniformModelFitter(-0.1));
        assertThrows(IllegalArgumentException.class, () -> new UniformModelFitter(0.6));
    }
}
