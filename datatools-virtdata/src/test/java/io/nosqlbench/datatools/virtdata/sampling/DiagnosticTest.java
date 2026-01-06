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

package io.nosqlbench.datatools.virtdata.sampling;

import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Test;

public class DiagnosticTest {

    @Test
    void diagnoseBetaPrime() {
        BetaPrimeScalarModel model = new BetaPrimeScalarModel(3.0, 5.0);
        BetaPrimeSampler sampler = new BetaPrimeSampler(model);

        System.out.println("Testing BetaPrime(3, 5)...");
        System.out.println("Expected mean: " + (3.0 / (5.0 - 1.0)));

        double[] testU = {0.1, 0.25, 0.5, 0.75, 0.9};
        for (double u : testU) {
            double sample = sampler.sample(u);
            System.out.printf("  u=%.2f -> %.6f%n", u, sample);
        }

        // Test with random samples like the accuracy test
        System.out.println("\nTesting with random samples (first 10)...");
        java.util.Random random = new java.util.Random(42L);
        double sum = 0;
        int outliers = 0;
        for (int i = 0; i < 100000; i++) {
            double u = random.nextDouble();
            u = Math.max(1e-10, Math.min(1 - 1e-10, u));
            double sample = sampler.sample(u);
            sum += sample;
            if (sample > 100) {
                outliers++;
                System.out.printf("  OUTLIER at i=%d: u=%.15f -> %.6e%n", i, u, sample);
            }
            if (i < 10) {
                System.out.printf("  u=%.10f -> %.6f%n", u, sample);
            }
        }
        System.out.printf("Mean of 100K samples: %.6f%n", sum / 100000);
        System.out.printf("Outliers (>100): %d%n", outliers);

        // Test extreme u values explicitly
        System.out.println("\nTesting extreme u values...");
        double[] extremeU = {0.99, 0.999, 0.9999, 0.99999, 0.999999, 1 - 1e-10};
        for (double u : extremeU) {
            double sample = sampler.sample(u);
            System.out.printf("  u=%.15f -> %.6e%n", u, sample);
        }
    }

    @Test
    void diagnoseBeta() {
        BetaScalarModel betaModel = new BetaScalarModel(3.0, 5.0);
        BetaSampler betaSampler = new BetaSampler(betaModel);

        System.out.println("\nTesting Beta(3, 5) for comparison...");
        System.out.println("Expected mean: " + (3.0 / 8.0));
        double[] testU = {0.1, 0.25, 0.5, 0.75, 0.9};
        for (double u : testU) {
            double sample = betaSampler.sample(u);
            System.out.printf("  u=%.2f -> %.6f%n", u, sample);
        }
    }

    @Test
    void diagnoseStudentT() {
        StudentTScalarModel model = new StudentTScalarModel(10.0);
        StudentTSampler sampler = new StudentTSampler(model);

        System.out.println("\nTesting Student-t(10)...");
        System.out.println("Expected mean: 0");
        double[] testU = {0.1, 0.25, 0.5, 0.75, 0.9};
        for (double u : testU) {
            double sample = sampler.sample(u);
            System.out.printf("  u=%.2f -> %.6f%n", u, sample);
        }
    }
}
