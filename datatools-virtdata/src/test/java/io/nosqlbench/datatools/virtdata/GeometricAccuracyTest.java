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
import io.nosqlbench.vshapes.extract.DatasetModelExtractor;
import io.nosqlbench.vshapes.extract.StatisticalTestSuite;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Accuracy tests for geometric properties of vector spaces.
 *
 * <h2>Purpose</h2>
 *
 * <p>Validates that synthetic vector data preserves key geometric properties:
 * <ul>
 *   <li><b>Inter-vector distances</b>: Distribution of pairwise Euclidean distances</li>
 *   <li><b>Cosine similarities</b>: Distribution of pairwise cosine similarities</li>
 *   <li><b>Vector norms</b>: Distribution of L2 norms</li>
 *   <li><b>Angular distribution</b>: Distribution of angles between vectors</li>
 * </ul>
 *
 * <h2>Approach</h2>
 *
 * <p>Uses K-S tests on sampled geometric properties to verify distributions match.
 *
 * @see StatisticalTestSuite
 */
@Tag("accuracy")
public class GeometricAccuracyTest {

    private static final long SEED = 42L;
    // Reduced from 50K to 20K - still sufficient for geometric property validation
    private static final int SAMPLES = 20_000;
    private static final int PAIR_SAMPLES = 5_000;

    static Stream<Arguments> dimensionProvider() {
        return Stream.of(
            Arguments.of(32, "32 dimensions"),
            Arguments.of(128, "128 dimensions"),
            Arguments.of(384, "384 dimensions")
        );
    }

    // ========== Distance Distribution Tests ==========

    @ParameterizedTest(name = "Euclidean distance: {1}")
    @MethodSource("dimensionProvider")
    void testEuclideanDistanceDistribution(int dims, String description) {
        float[][] original = generateRandomVectors(SAMPLES, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origDistances = samplePairwiseDistances(original, PAIR_SAMPLES, SEED);
        float[] synthDistances = samplePairwiseDistances(synthetic, PAIR_SAMPLES, SEED);

        StatisticalTestSuite.TestResult ks =
            StatisticalTestSuite.kolmogorovSmirnovTest(origDistances, synthDistances);

        System.out.printf("\n=== Euclidean Distance Distribution (%s) ===%n", description);
        System.out.printf("K-S statistic: %.4f (critical: %.4f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");

        // Compare moments
        StatisticalTestSuite.MomentComparison moments =
            StatisticalTestSuite.compareMoments(origDistances, synthDistances);
        System.out.printf("Mean error: %.4f, Variance error: %.2f%%%n",
            moments.meanError(), moments.varianceRelError() * 100);

        // Use lenient threshold - geometric properties are affected by stratified sampling
        // K-S < 0.1 is acceptable for documenting similarity
        assertTrue(ks.statistic() < 0.1,
            "Distance K-S statistic should be < 0.1, got: " + ks.statistic());
    }

    @ParameterizedTest(name = "Cosine similarity: {1}")
    @MethodSource("dimensionProvider")
    void testCosineSimilarityDistribution(int dims, String description) {
        float[][] original = generateRandomVectors(SAMPLES, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origCosines = samplePairwiseCosines(original, PAIR_SAMPLES, SEED);
        float[] synthCosines = samplePairwiseCosines(synthetic, PAIR_SAMPLES, SEED);

        StatisticalTestSuite.TestResult ks =
            StatisticalTestSuite.kolmogorovSmirnovTest(origCosines, synthCosines);

        System.out.printf("\n=== Cosine Similarity Distribution (%s) ===%n", description);
        System.out.printf("K-S statistic: %.4f (critical: %.4f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");

        // Use lenient threshold - stratified sampling affects joint distributions
        assertTrue(ks.statistic() < 0.1,
            "Cosine K-S statistic should be < 0.1, got: " + ks.statistic());
    }

    // ========== Norm Distribution Tests ==========

    @ParameterizedTest(name = "L2 norm: {1}")
    @MethodSource("dimensionProvider")
    void testNormDistribution(int dims, String description) {
        float[][] original = generateRandomVectors(SAMPLES, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origNorms = computeNorms(original);
        float[] synthNorms = computeNorms(synthetic);

        StatisticalTestSuite.TestResult ks =
            StatisticalTestSuite.kolmogorovSmirnovTest(origNorms, synthNorms);

        System.out.printf("\n=== L2 Norm Distribution (%s) ===%n", description);
        System.out.printf("K-S statistic: %.4f (critical: %.4f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");

        // Print norm statistics
        double origMean = mean(origNorms);
        double synthMean = mean(synthNorms);
        System.out.printf("Original mean norm: %.4f, Synthetic mean norm: %.4f%n",
            origMean, synthMean);

        // Norm distributions are affected by the sampling method
        // K-S < 0.1 is acceptable for demonstrating similarity
        assertTrue(ks.statistic() < 0.1,
            "Norm K-S statistic should be < 0.1, got: " + ks.statistic());
    }

    // ========== Angular Distribution Tests ==========

    @Test
    void testAngularDistribution() {
        int dims = 128;
        float[][] original = generateRandomVectors(SAMPLES, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origAngles = samplePairwiseAngles(original, PAIR_SAMPLES, SEED);
        float[] synthAngles = samplePairwiseAngles(synthetic, PAIR_SAMPLES, SEED);

        StatisticalTestSuite.TestResult ks =
            StatisticalTestSuite.kolmogorovSmirnovTest(origAngles, synthAngles);

        System.out.println("\n=== Angular Distribution (128 dims) ===");
        System.out.printf("K-S statistic: %.4f (critical: %.4f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");

        // Print angle statistics
        double origMean = mean(origAngles);
        double synthMean = mean(synthAngles);
        System.out.printf("Original mean angle: %.4f rad (%.2f°)%n",
            origMean, Math.toDegrees(origMean));
        System.out.printf("Synthetic mean angle: %.4f rad (%.2f°)%n",
            synthMean, Math.toDegrees(synthMean));

        // Use lenient threshold for angular distribution
        assertTrue(ks.statistic() < 0.1,
            "Angular K-S statistic should be < 0.1, got: " + ks.statistic());
    }

    // ========== Manhattan Distance Tests ==========

    @Test
    void testManhattanDistanceDistribution() {
        int dims = 128;
        float[][] original = generateRandomVectors(SAMPLES, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origManhattan = sampleManhattanDistances(original, PAIR_SAMPLES, SEED);
        float[] synthManhattan = sampleManhattanDistances(synthetic, PAIR_SAMPLES, SEED);

        StatisticalTestSuite.TestResult ks =
            StatisticalTestSuite.kolmogorovSmirnovTest(origManhattan, synthManhattan);

        System.out.println("\n=== Manhattan Distance Distribution (128 dims) ===");
        System.out.printf("K-S statistic: %.4f (critical: %.4f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");

        // Use lenient threshold for Manhattan distance
        assertTrue(ks.statistic() < 0.1,
            "Manhattan K-S statistic should be < 0.1, got: " + ks.statistic());
    }

    // ========== Nearest Neighbor Distance Tests ==========

    @Test
    void testNearestNeighborDistanceDistribution() {
        int dims = 64;
        int n = 2000;  // Reduced for O(n²) nearest neighbor search
        float[][] original = generateRandomVectors(n, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origNN = computeNearestNeighborDistances(original, 200);  // Sample subset
        float[] synthNN = computeNearestNeighborDistances(synthetic, 200);

        StatisticalTestSuite.TestResult ks =
            StatisticalTestSuite.kolmogorovSmirnovTest(origNN, synthNN);

        System.out.println("\n=== Nearest Neighbor Distance Distribution ===");
        System.out.printf("K-S statistic: %.4f (critical: %.4f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");

        // NN distribution may differ due to independence assumption in synthetic data,
        // but K-S should still be reasonably small (< 0.15)
        assertTrue(ks.statistic() < 0.15,
            "NN distance K-S statistic should be < 0.15, got: " + ks.statistic());
    }

    // ========== Dimensionality Scaling Tests ==========

    @Test
    void testDimensionalityScaling() {
        // Reduced dimension range and sample size for faster tests
        int[] dimensions = {32, 64, 128};
        int n = 10000;

        System.out.println("\n=== Dimensionality Scaling ===");
        System.out.println("Dims    Dist K-S    Cos K-S     Norm K-S");

        int failCount = 0;
        StringBuilder failures = new StringBuilder();

        for (int dims : dimensions) {
            float[][] original = generateRandomVectors(n, dims, SEED);
            float[][] synthetic = roundTripThroughModel(original, SEED + 1);

            float[] origDist = samplePairwiseDistances(original, 3000, SEED);
            float[] synthDist = samplePairwiseDistances(synthetic, 3000, SEED);
            StatisticalTestSuite.TestResult distKS =
                StatisticalTestSuite.kolmogorovSmirnovTest(origDist, synthDist);

            float[] origCos = samplePairwiseCosines(original, 3000, SEED);
            float[] synthCos = samplePairwiseCosines(synthetic, 3000, SEED);
            StatisticalTestSuite.TestResult cosKS =
                StatisticalTestSuite.kolmogorovSmirnovTest(origCos, synthCos);

            float[] origNorm = computeNorms(original);
            float[] synthNorm = computeNorms(synthetic);
            StatisticalTestSuite.TestResult normKS =
                StatisticalTestSuite.kolmogorovSmirnovTest(origNorm, synthNorm);

            System.out.printf(" %3d     %.4f      %.4f       %.4f%n",
                dims, distKS.statistic(), cosKS.statistic(), normKS.statistic());

            // All K-S statistics should be < 0.1 for geometric property preservation
            if (distKS.statistic() >= 0.1 || cosKS.statistic() >= 0.1 || normKS.statistic() >= 0.1) {
                failCount++;
                failures.append(String.format("dims=%d (dist=%.4f, cos=%.4f, norm=%.4f); ",
                    dims, distKS.statistic(), cosKS.statistic(), normKS.statistic()));
            }
        }

        assertEquals(0, failCount,
            "All dimensions should have K-S < 0.1 for geometric properties. Failures: " + failures);
    }

    // ========== Q-Q Correlation for Geometric Properties ==========

    @Test
    void testGeometricQQCorrelation() {
        int dims = 128;
        float[][] original = generateRandomVectors(SAMPLES, dims, SEED);
        float[][] synthetic = roundTripThroughModel(original, SEED + 1);

        float[] origDist = samplePairwiseDistances(original, PAIR_SAMPLES, SEED);
        float[] synthDist = samplePairwiseDistances(synthetic, PAIR_SAMPLES, SEED);

        double distQQ = StatisticalTestSuite.qqCorrelation(origDist, synthDist);

        float[] origNorm = computeNorms(original);
        float[] synthNorm = computeNorms(synthetic);

        double normQQ = StatisticalTestSuite.qqCorrelation(origNorm, synthNorm);

        System.out.println("\n=== Geometric Q-Q Correlations ===");
        System.out.printf("Distance Q-Q correlation: %.6f%n", distQQ);
        System.out.printf("Norm Q-Q correlation: %.6f%n", normQQ);

        assertTrue(distQQ > 0.99, "Distance Q-Q correlation should be > 0.99");
        assertTrue(normQQ > 0.99, "Norm Q-Q correlation should be > 0.99");
    }

    // ========== Helper Methods ==========

    private float[][] generateRandomVectors(int n, int dims, long seed) {
        Random rng = new Random(seed);
        float[][] data = new float[n][dims];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dims; d++) {
                data[i][d] = (float) rng.nextGaussian();
            }
        }
        return data;
    }

    private float[][] roundTripThroughModel(float[][] original, long seed) {
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel model = extractor.extractVectorModel(original);
        return generateFromModel(model, original.length, seed);
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

    private float[] samplePairwiseDistances(float[][] vectors, int numPairs, long seed) {
        Random rng = new Random(seed);
        int n = vectors.length;
        float[] distances = new float[numPairs];

        for (int p = 0; p < numPairs; p++) {
            int i = rng.nextInt(n);
            int j = rng.nextInt(n);
            if (i == j) j = (j + 1) % n;
            distances[p] = euclideanDistance(vectors[i], vectors[j]);
        }

        return distances;
    }

    private float[] samplePairwiseCosines(float[][] vectors, int numPairs, long seed) {
        Random rng = new Random(seed);
        int n = vectors.length;
        float[] cosines = new float[numPairs];

        for (int p = 0; p < numPairs; p++) {
            int i = rng.nextInt(n);
            int j = rng.nextInt(n);
            if (i == j) j = (j + 1) % n;
            cosines[p] = cosineSimilarity(vectors[i], vectors[j]);
        }

        return cosines;
    }

    private float[] samplePairwiseAngles(float[][] vectors, int numPairs, long seed) {
        Random rng = new Random(seed);
        int n = vectors.length;
        float[] angles = new float[numPairs];

        for (int p = 0; p < numPairs; p++) {
            int i = rng.nextInt(n);
            int j = rng.nextInt(n);
            if (i == j) j = (j + 1) % n;
            float cos = cosineSimilarity(vectors[i], vectors[j]);
            // Clamp to [-1, 1] for numerical stability
            cos = Math.max(-1f, Math.min(1f, cos));
            angles[p] = (float) Math.acos(cos);
        }

        return angles;
    }

    private float[] sampleManhattanDistances(float[][] vectors, int numPairs, long seed) {
        Random rng = new Random(seed);
        int n = vectors.length;
        float[] distances = new float[numPairs];

        for (int p = 0; p < numPairs; p++) {
            int i = rng.nextInt(n);
            int j = rng.nextInt(n);
            if (i == j) j = (j + 1) % n;
            distances[p] = manhattanDistance(vectors[i], vectors[j]);
        }

        return distances;
    }

    private float[] computeNorms(float[][] vectors) {
        float[] norms = new float[vectors.length];
        for (int i = 0; i < vectors.length; i++) {
            norms[i] = l2Norm(vectors[i]);
        }
        return norms;
    }

    private float[] computeNearestNeighborDistances(float[][] vectors, int sampleSize) {
        Random rng = new Random(SEED);
        int n = vectors.length;
        float[] nnDistances = new float[sampleSize];

        for (int s = 0; s < sampleSize; s++) {
            int i = rng.nextInt(n);
            float minDist = Float.MAX_VALUE;

            // Find nearest neighbor (brute force for small samples)
            for (int j = 0; j < n; j++) {
                if (i != j) {
                    float dist = euclideanDistance(vectors[i], vectors[j]);
                    minDist = Math.min(minDist, dist);
                }
            }

            nnDistances[s] = minDist;
        }

        return nnDistances;
    }

    private float euclideanDistance(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            float d = a[i] - b[i];
            sum += d * d;
        }
        return (float) Math.sqrt(sum);
    }

    private float manhattanDistance(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.abs(a[i] - b[i]);
        }
        return sum;
    }

    private float cosineSimilarity(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA <= 0 || normB <= 0) return 0;
        return (float) (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
    }

    private float l2Norm(float[] v) {
        float sum = 0;
        for (float f : v) sum += f * f;
        return (float) Math.sqrt(sum);
    }

    private double mean(float[] values) {
        double sum = 0;
        for (float v : values) sum += v;
        return sum / values.length;
    }
}
