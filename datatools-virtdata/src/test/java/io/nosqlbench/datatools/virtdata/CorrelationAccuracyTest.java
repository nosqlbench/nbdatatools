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
import io.nosqlbench.vshapes.extract.CorrelationAnalysis;
import io.nosqlbench.vshapes.extract.DatasetModelExtractor;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accuracy tests for correlation structure validation.
 *
 * <h2>Purpose</h2>
 *
 * <p>Since the VectorSpaceModel treats dimensions independently, this test suite
 * documents how correlation structure is affected by the independence assumption.
 *
 * <h2>Key Metrics</h2>
 *
 * <ul>
 *   <li><b>Frobenius norm</b>: Overall matrix difference</li>
 *   <li><b>Significant correlations</b>: Count where |r| > 0.1</li>
 *   <li><b>Correlation loss rate</b>: Fraction of correlations lost</li>
 * </ul>
 *
 * @see CorrelationAnalysis
 */
@Tag("accuracy")
public class CorrelationAccuracyTest {

    private static final long SEED = 42L;
    private static final int SAMPLES = 100_000;

    @Test
    void testCorrelationMatrixComputation() {
        int dims = 16;
        int n = 10000;
        Random rng = new Random(SEED);

        // Generate correlated data
        float[][] data = generateCorrelatedData(n, dims, rng, 0.5);

        // Compute correlation matrix
        double[][] corr = CorrelationAnalysis.computeCorrelationMatrix(data);

        // Verify diagonal is 1.0
        for (int d = 0; d < dims; d++) {
            assertEquals(1.0, corr[d][d], 1e-10, "Diagonal should be 1.0");
        }

        // Verify symmetry
        for (int i = 0; i < dims; i++) {
            for (int j = i + 1; j < dims; j++) {
                assertEquals(corr[i][j], corr[j][i], 1e-10, "Matrix should be symmetric");
            }
        }

        // Verify correlations are in [-1, 1]
        for (int i = 0; i < dims; i++) {
            for (int j = 0; j < dims; j++) {
                assertTrue(corr[i][j] >= -1.0 && corr[i][j] <= 1.0,
                    "Correlation should be in [-1, 1]");
            }
        }
    }

    @Test
    void testCorrelationComparisonIdenticalData() {
        int dims = 32;
        int n = 10000;
        Random rng = new Random(SEED);

        float[][] data = new float[n][dims];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dims; d++) {
                data[i][d] = (float) rng.nextGaussian();
            }
        }

        double[][] corr1 = CorrelationAnalysis.computeCorrelationMatrix(data);
        double[][] corr2 = CorrelationAnalysis.computeCorrelationMatrix(data);

        CorrelationAnalysis.CorrelationComparison comparison =
            CorrelationAnalysis.compareCorrelationMatrices(corr1, corr2);

        assertEquals(0.0, comparison.frobeniusNormDiff(), 1e-10,
            "Identical matrices should have zero Frobenius difference");
        assertEquals(0.0, comparison.maxAbsDiff(), 1e-10,
            "Identical matrices should have zero max difference");
        assertEquals(1.0, comparison.preservationRate(), 1e-10,
            "All correlations should be preserved");
    }

    @Test
    void testIndependentDataPreservesCorrelation() {
        int dims = 32;
        int n = 50000;
        Random rng = new Random(SEED);

        // Generate independent data (no correlation between dimensions)
        float[][] original = new float[n][dims];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dims; d++) {
                original[i][d] = (float) rng.nextGaussian();
            }
        }

        // Extract model and regenerate
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel model = extractor.extractVectorModel(original);
        float[][] synthetic = generateFromModel(model, n, SEED + 1);

        // Compare correlations
        CorrelationAnalysis.CorrelationComparison comparison =
            CorrelationAnalysis.compareCorrelationStructure(original, synthetic);

        System.out.println("\n=== Independent Data Correlation Test ===");
        System.out.println(CorrelationAnalysis.formatCorrelationReport(comparison));

        // For independent data, there should be few significant correlations
        assertTrue(comparison.frobeniusNormDiff() < dims * 0.5,
            "Frobenius norm should be moderate for independent data");
    }

    @Test
    void testCorrelatedDataShowsLoss() {
        int dims = 32;
        int n = 50000;
        Random rng = new Random(SEED);

        // Generate data with strong correlations
        float[][] correlated = generateCorrelatedData(n, dims, rng, 0.7);

        // Extract model and regenerate (model assumes independence)
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel model = extractor.extractVectorModel(correlated);
        float[][] synthetic = generateFromModel(model, n, SEED + 1);

        // Compare correlations
        CorrelationAnalysis.CorrelationComparison comparison =
            CorrelationAnalysis.compareCorrelationStructure(correlated, synthetic);

        System.out.println("\n=== Correlated Data Correlation Test ===");
        System.out.println(CorrelationAnalysis.formatCorrelationReport(comparison));

        // Document that correlation loss is expected due to independence assumption
        assertTrue(comparison.significantCorrelations() > 0,
            "Correlated data should have significant correlations");
        assertTrue(comparison.lossRate() > 0,
            "Some correlation loss is expected due to independence assumption");

        System.out.println("Note: Correlation loss is expected behavior when modeling dimensions independently.");
        System.out.println("This is a known limitation of the independence assumption in VectorSpaceModel.");
    }

    @Test
    void testExplainedVarianceComparison() {
        int dims = 64;
        int n = 50000;
        Random rng = new Random(SEED);

        float[][] original = new float[n][dims];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dims; d++) {
                original[i][d] = (float) rng.nextGaussian();
            }
        }

        // Extract model and regenerate
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel model = extractor.extractVectorModel(original);
        float[][] synthetic = generateFromModel(model, n, SEED + 1);

        // Compare explained variance
        int numComponents = 10;
        double[] origVar = CorrelationAnalysis.computeExplainedVariance(original, numComponents);
        double[] synthVar = CorrelationAnalysis.computeExplainedVariance(synthetic, numComponents);

        System.out.println("\n=== Explained Variance Comparison ===");
        System.out.println("Component  Original    Synthetic   Diff");
        for (int i = 0; i < numComponents; i++) {
            double diff = Math.abs(origVar[i] - synthVar[i]);
            System.out.printf("    %2d     %.4f      %.4f     %.4f%n", i + 1, origVar[i], synthVar[i], diff);
        }

        double meanDiff = CorrelationAnalysis.compareExplainedVariance(original, synthetic, numComponents);
        System.out.printf("Mean explained variance difference: %.4f%n", meanDiff);

        // For independent data, explained variance should be similar
        assertTrue(meanDiff < 0.05,
            "Explained variance should be similar for independent data");
    }

    @Test
    void testCorrelationReportFormatting() {
        int dims = 16;
        int n = 10000;
        Random rng = new Random(SEED);

        float[][] data1 = new float[n][dims];
        float[][] data2 = new float[n][dims];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dims; d++) {
                data1[i][d] = (float) rng.nextGaussian();
                data2[i][d] = (float) rng.nextGaussian();
            }
        }

        CorrelationAnalysis.CorrelationComparison comparison =
            CorrelationAnalysis.compareCorrelationStructure(data1, data2);

        String report = CorrelationAnalysis.formatCorrelationReport(comparison);

        // Verify report contains expected sections
        assertTrue(report.contains("CORRELATION STRUCTURE ANALYSIS"),
            "Report should have header");
        assertTrue(report.contains("Dimensions:"),
            "Report should show dimensions");
        assertTrue(report.contains("Frobenius norm"),
            "Report should show Frobenius norm");

        System.out.println("\n=== Formatted Correlation Report ===");
        System.out.println(report);
    }

    @Test
    void testCorrelationWithVaryingDimensions() {
        int[] dimSizes = {8, 32, 64, 128};
        int n = 10000;
        Random rng = new Random(SEED);

        System.out.println("\n=== Correlation Scaling with Dimensions ===");
        System.out.println("Dims   Frobenius   Max Diff   Significant");

        for (int dims : dimSizes) {
            float[][] data = new float[n][dims];
            for (int i = 0; i < n; i++) {
                for (int d = 0; d < dims; d++) {
                    data[i][d] = (float) rng.nextGaussian();
                }
            }

            // Compare two independent samples from same distribution
            float[][] data2 = new float[n][dims];
            for (int i = 0; i < n; i++) {
                for (int d = 0; d < dims; d++) {
                    data2[i][d] = (float) rng.nextGaussian();
                }
            }

            CorrelationAnalysis.CorrelationComparison comparison =
                CorrelationAnalysis.compareCorrelationStructure(data, data2);

            System.out.printf(" %3d    %.4f      %.4f       %d%n",
                dims, comparison.frobeniusNormDiff(), comparison.maxAbsDiff(),
                comparison.significantCorrelations());
        }
    }

    // ========== Helper Methods ==========

    private float[][] generateCorrelatedData(int n, int dims, Random rng, double baseCorrelation) {
        float[][] data = new float[n][dims];

        // Generate base dimension
        for (int i = 0; i < n; i++) {
            data[i][0] = (float) rng.nextGaussian();
        }

        // Generate other dimensions with correlation to first dimension
        for (int d = 1; d < dims; d++) {
            double corrFactor = baseCorrelation * (1.0 - (d % 4) * 0.2);  // Varying correlation
            for (int i = 0; i < n; i++) {
                double noise = rng.nextGaussian();
                data[i][d] = (float) (data[i][0] * corrFactor + noise * Math.sqrt(1 - corrFactor * corrFactor));
            }
        }

        return data;
    }

    private float[][] generateFromModel(VectorSpaceModel model, int samples, long seed) {
        int dims = model.dimensions();
        float[][] data = new float[samples][dims];

        ComponentSampler[] samplers = new ComponentSampler[dims];
        for (int d = 0; d < dims; d++) {
            samplers[d] = ComponentSamplerFactory.forModel(model.scalarModel(d));
        }

        for (int v = 0; v < samples; v++) {
            for (int d = 0; d < dims; d++) {
                double u = StratifiedSampler.unitIntervalValue(v, d, samples);
                data[v][d] = (float) samplers[d].sample(u);
            }
        }

        return data;
    }
}
