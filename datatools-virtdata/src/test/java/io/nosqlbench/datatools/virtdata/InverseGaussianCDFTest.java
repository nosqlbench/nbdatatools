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

import io.nosqlbench.vshapes.model.GaussianComponentModel;
import io.nosqlbench.datatools.virtdata.sampling.InverseGaussianCDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class InverseGaussianCDFTest {

    // Tolerance for approximation accuracy
    private static final double TOLERANCE = 0.001;

    @Test
    void testMedian() {
        // P(X <= 0) = 0.5 for standard normal
        double result = InverseGaussianCDF.standardNormalQuantile(0.5);
        assertEquals(0.0, result, TOLERANCE);
    }

    @ParameterizedTest
    @CsvSource({
        "0.1, -1.282",
        "0.25, -0.674",
        "0.5, 0.0",
        "0.75, 0.674",
        "0.9, 1.282",
        "0.95, 1.645",
        "0.99, 2.326"
    })
    void testKnownQuantiles(double p, double expectedQuantile) {
        double result = InverseGaussianCDF.standardNormalQuantile(p);
        assertEquals(expectedQuantile, result, TOLERANCE);
    }

    @Test
    void testSymmetry() {
        // standardNormalQuantile(p) should equal -standardNormalQuantile(1-p)
        double[] probs = {0.1, 0.2, 0.3, 0.4};
        for (double p : probs) {
            double lower = InverseGaussianCDF.standardNormalQuantile(p);
            double upper = InverseGaussianCDF.standardNormalQuantile(1.0 - p);
            assertEquals(-lower, upper, TOLERANCE, "Symmetry failed for p=" + p);
        }
    }

    @Test
    void testCustomDistribution() {
        // For N(5, 2): quantile(0.5) should be 5 (the mean)
        double result = InverseGaussianCDF.quantile(0.5, 5.0, 2.0);
        assertEquals(5.0, result, TOLERANCE);

        // Standard normal quantile at 0.84 ≈ 1.0, so N(5,2) quantile ≈ 5 + 2*1 = 7
        double result84 = InverseGaussianCDF.quantile(0.84, 5.0, 2.0);
        assertEquals(7.0, result84, 0.1);
    }

    @Test
    void testWithComponentModel() {
        GaussianComponentModel model = new GaussianComponentModel(10.0, 3.0);
        double result = InverseGaussianCDF.quantile(0.5, model);
        assertEquals(10.0, result, TOLERANCE);
    }

    @Test
    void testBoundaryValues() {
        // Very small p (close to 0)
        double smallP = InverseGaussianCDF.standardNormalQuantile(0.001);
        assertTrue(smallP < -3.0, "Very small p should give very negative quantile");

        // Very large p (close to 1)
        double largeP = InverseGaussianCDF.standardNormalQuantile(0.999);
        assertTrue(largeP > 3.0, "Very large p should give very positive quantile");
    }

    @Test
    void testInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> InverseGaussianCDF.standardNormalQuantile(0.0));
        assertThrows(IllegalArgumentException.class, () -> InverseGaussianCDF.standardNormalQuantile(1.0));
        assertThrows(IllegalArgumentException.class, () -> InverseGaussianCDF.standardNormalQuantile(-0.1));
        assertThrows(IllegalArgumentException.class, () -> InverseGaussianCDF.standardNormalQuantile(1.1));
    }
}
