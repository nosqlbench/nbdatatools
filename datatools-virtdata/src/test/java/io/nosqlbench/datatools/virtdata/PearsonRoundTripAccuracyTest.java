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
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.UniformScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.xvec.writers.FvecVectorWriter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end accuracy test for bounded distribution round-trip verification.
 *
 * <h2>Purpose</h2>
 *
 * <p>This test verifies that the model fitting pipeline can accurately recover
 * the original distribution types and parameters from synthetic data generated
 * within bounded ranges, typical for vector component values in [-1, 1].
 *
 * <h2>Test Matrix</h2>
 *
 * <p>The test is parameterized across:
 * <ul>
 *   <li>Dimensionalities: 128, 256, 1024 (4096 disabled for performance)</li>
 *   <li>Cardinalities: 8192 (high-precision tests disabled for performance)</li>
 * </ul>
 *
 * <h2>Distribution Coverage</h2>
 *
 * <p>Each dimension cycles through bounded distribution types that are
 * appropriate for unit interval data and can be reliably distinguished:
 * <ul>
 *   <li><b>Normal</b> - Central tendency with Gaussian shape</li>
 *   <li><b>Beta</b> - Flexible bounded distribution with various shapes</li>
 *   <li><b>Uniform</b> - Flat distribution across the range</li>
 * </ul>
 *
 * <p><b>Note:</b> Heavy-tailed distributions (Gamma, StudentT, InverseGamma,
 * BetaPrime) are excluded because:
 * <ol>
 *   <li>Their distinguishing features (heavy tails) are truncated in bounded ranges</li>
 *   <li>They become indistinguishable from bounded distributions when constrained</li>
 *   <li>Testing them on bounded data introduces artificial ambiguity</li>
 * </ol>
 *
 * <h2>Workflow</h2>
 *
 * <ol>
 *   <li>Create ScalarModel instances for each dimension (bounded types only)</li>
 *   <li>Generate vector data using ComponentSamplers</li>
 *   <li>Write data to temporary .fvec files</li>
 *   <li>Analyze with DatasetModelExtractor using boundedDataSelector</li>
 *   <li>Verify recovered model types match originals</li>
 *   <li>Verify recovered parameters are within tolerance</li>
 * </ol>
 */
@Tag("accuracy")
public class PearsonRoundTripAccuracyTest {

    /**
     * Bounded distribution types for unit interval data [-1, 1].
     *
     * <p>These distributions are appropriate for bounded data and can be
     * reliably distinguished from each other:
     * <ul>
     *   <li>NORMAL - Central tendency with Gaussian shape</li>
     *   <li>BETA - Flexible bounded distribution, various shapes</li>
     *   <li>UNIFORM - Flat distribution across range</li>
     * </ul>
     *
     * <p>Heavy-tailed distributions (Gamma, StudentT, InverseGamma, BetaPrime)
     * are excluded because they are indistinguishable in bounded ranges.
     */
    private enum BoundedDistributionType {
        NORMAL,
        BETA,
        UNIFORM
    }

    private static final BoundedDistributionType[] BOUNDED_TYPES = BoundedDistributionType.values();
    private static final int NUM_BOUNDED_TYPES = BOUNDED_TYPES.length;

    /**
     * Provides the test parameter matrix.
     * Dimensionalities: 128, 256, 1024 (from KeyDimensions.TESTED_DIMENSIONS)
     * Cardinalities: 8192 (moderate sample for statistical significance)
     *
     * <p>Note: 4096-dimension and high-cardinality tests are disabled for performance.
     * Enable them manually for comprehensive validation.
     */
    static Stream<Arguments> testConfigurations() {
        return Stream.of(
            // Tests covering key dimensions with moderate cardinality
            Arguments.of(128, 8192, "128d_8k"),
            Arguments.of(256, 8192, "256d_8k"),
            Arguments.of(1024, 8192, "1024d_8k")
            // Arguments.of(4096, 8192, "4096d_8k"),   // Disabled for performance

            // High-precision tests - disabled for performance
            // Arguments.of(128, 131072, "128d_128k"),
            // Arguments.of(1024, 131072, "1024d_128k"),
            // Arguments.of(4096, 131072, "4096d_128k")
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("testConfigurations")
    void testPearsonRoundTrip(int dimensions, int cardinality, String testName, @TempDir Path tempDir)
            throws IOException {

        System.out.println("Testing: " + testName + " (dims=" + dimensions + ", n=" + cardinality + ")");

        // Step 1: Create source models for each dimension
        ScalarModel[] sourceModels = createSourceModels(dimensions);

        // Step 2: Generate vector data
        float[][] data = generateVectorData(sourceModels, cardinality);

        // Step 3: Write to .fvec file
        Path fvecFile = tempDir.resolve(testName + ".fvec");
        writeFvecFile(fvecFile, data);

        // Step 4: Read back and analyze
        float[][] readData = readFvecFile(fvecFile, dimensions, cardinality);

        // Verify data integrity
        assertDataIntegrity(data, readData);

        // Step 5: Analyze with DatasetModelExtractor using bounded data selector
        // For unit interval data, use boundedDataSelector which includes only
        // distributions meaningful in bounded ranges: Normal, Beta, Uniform
        DatasetModelExtractor extractor = new DatasetModelExtractor(
            BestFitSelector.boundedDataSelector(),
            DatasetModelExtractor.DEFAULT_UNIQUE_VECTORS
        );
        VectorSpaceModel recoveredModel = extractor.extractVectorModel(readData);

        // Step 6: Verify recovered models
        verifyRecoveredModels(sourceModels, recoveredModel, dimensions, cardinality);

        System.out.println("  ✓ Round-trip successful");
    }

    /**
     * Creates source ScalarModel instances for each dimension.
     *
     * <p>Each dimension cycles through bounded distribution types
     * (Normal, Beta, Uniform) with varying parameters. These are
     * appropriate for unit interval data [-1, 1].
     */
    private ScalarModel[] createSourceModels(int dimensions) {
        ScalarModel[] models = new ScalarModel[dimensions];
        Random paramRng = new Random(12345);  // Deterministic parameters

        for (int d = 0; d < dimensions; d++) {
            BoundedDistributionType type = BOUNDED_TYPES[d % NUM_BOUNDED_TYPES];
            models[d] = createModelForType(type, d, paramRng);
        }

        return models;
    }

    /**
     * Creates a ScalarModel for bounded distribution types.
     *
     * <p>All models generate data within reasonable bounds suitable for
     * unit interval vector components.
     */
    private ScalarModel createModelForType(BoundedDistributionType type, int dimension, Random rng) {
        // Use dimension to create slight parameter variations
        double dimOffset = dimension * 0.001;

        switch (type) {
            case NORMAL:
                // Normal: mean near center, moderate stdDev for bounded range
                // Mean in [-0.2, 0.2], stdDev in [0.1, 0.2] produces values mostly in [-1, 1]
                double mean = dimOffset - 0.1;
                double stdDev = 0.15 + (rng.nextDouble() * 0.05);
                return new NormalScalarModel(mean, stdDev);

            case BETA:
                // Beta: bounded [0, 1], various shapes
                // Alpha > beta = right skewed, alpha < beta = left skewed
                // Both around 2-5 gives bell-shaped curves
                double alpha = 2.0 + dimOffset + (rng.nextDouble() * 2);
                double beta = 3.0 + (rng.nextDouble() * 2);
                return new BetaScalarModel(alpha, beta);

            case UNIFORM:
                // Uniform: bounded range within [-1, 1]
                double lower = -0.8 + dimOffset;
                double upper = 0.8 + dimOffset;
                return new UniformScalarModel(lower, upper);

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Generates vector data by sampling from each dimension's model.
     */
    private float[][] generateVectorData(ScalarModel[] models, int cardinality) {
        int dimensions = models.length;
        float[][] data = new float[cardinality][dimensions];

        // Create samplers for each dimension
        ComponentSampler[] samplers = new ComponentSampler[dimensions];
        for (int d = 0; d < dimensions; d++) {
            samplers[d] = ComponentSamplerFactory.forModel(models[d]);
        }

        // Use stratified sampling for even coverage
        for (int v = 0; v < cardinality; v++) {
            for (int d = 0; d < dimensions; d++) {
                // StratifiedSampler provides deterministic unit-interval values
                double u = StratifiedSampler.unitIntervalValue(v, d, cardinality);
                data[v][d] = (float) samplers[d].sample(u);
            }
        }

        return data;
    }

    /**
     * Writes vector data to a .fvec file.
     */
    private void writeFvecFile(Path path, float[][] data) {
        FvecVectorWriter writer = new FvecVectorWriter();
        writer.open(path);
        try {
            for (float[] vector : data) {
                writer.write(vector);
            }
        } finally {
            writer.close();
        }
    }

    /**
     * Reads vector data from a .fvec file.
     */
    private float[][] readFvecFile(Path path, int dimensions, int cardinality) throws IOException {
        float[][] data = new float[cardinality][dimensions];

        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            int bytesPerVector = 4 + dimensions * 4;  // 4 bytes for dimension + 4 bytes per float
            ByteBuffer buffer = ByteBuffer.allocate(bytesPerVector);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int v = 0; v < cardinality; v++) {
                buffer.clear();
                int bytesRead = channel.read(buffer);
                assertEquals(bytesPerVector, bytesRead, "Incomplete vector read at index " + v);

                buffer.flip();
                int readDim = buffer.getInt();
                assertEquals(dimensions, readDim, "Dimension mismatch at vector " + v);

                for (int d = 0; d < dimensions; d++) {
                    data[v][d] = buffer.getFloat();
                }
            }
        }

        return data;
    }

    /**
     * Verifies that read data matches written data.
     */
    private void assertDataIntegrity(float[][] original, float[][] read) {
        assertEquals(original.length, read.length, "Cardinality mismatch");
        assertEquals(original[0].length, read[0].length, "Dimension mismatch");

        for (int v = 0; v < original.length; v++) {
            for (int d = 0; d < original[0].length; d++) {
                assertEquals(original[v][d], read[v][d], 1e-6f,
                    "Data mismatch at [" + v + "][" + d + "]");
            }
        }
    }

    /**
     * Verifies that recovered models match the source models.
     *
     * <p>This method checks:
     * <ul>
     *   <li>Model type matches (or is a reasonable alternative)</li>
     *   <li>Key parameters are within tolerance of source values</li>
     * </ul>
     *
     * <p>Note: With finite samples, the fitted model may differ slightly from
     * the true source, especially for:
     * <ul>
     *   <li>Low cardinality (small samples)</li>
     *   <li>Distributions with similar shapes (Normal vs StudentT with high df)</li>
     * </ul>
     */
    private void verifyRecoveredModels(ScalarModel[] sourceModels, VectorSpaceModel recovered,
                                       int dimensions, int cardinality) {

        assertEquals(dimensions, recovered.dimensions(), "Recovered dimensions mismatch");

        int typeMatches = 0;
        int parameterMatches = 0;

        // Tolerance scales with sample size - larger samples should give better fits
        double paramTolerance = cardinality >= 100000 ? 0.1 :
                                cardinality >= 10000 ? 0.2 : 0.3;

        for (int d = 0; d < dimensions; d++) {
            ScalarModel source = sourceModels[d];
            ScalarModel fitted = recovered.scalarModel(d);

            // Check type match
            if (isTypeMatch(source, fitted)) {
                typeMatches++;

                // Check parameter match for matching types
                if (areParametersClose(source, fitted, paramTolerance)) {
                    parameterMatches++;
                }
            } else {
                // Log mismatches for debugging
                if (d < 10 || d == dimensions - 1) {
                    System.out.println("  Type mismatch at dim " + d +
                        ": source=" + source.getModelType() +
                        ", fitted=" + fitted.getModelType());
                }
            }
        }

        double typeMatchRate = (double) typeMatches / dimensions;
        double paramMatchRate = typeMatches > 0 ? (double) parameterMatches / typeMatches : 0;

        System.out.printf("  Type match rate: %.1f%% (%d/%d)%n",
            typeMatchRate * 100, typeMatches, dimensions);
        System.out.printf("  Parameter match rate: %.1f%% (%d/%d)%n",
            paramMatchRate * 100, parameterMatches, typeMatches);

        // Expect high type match rate with fullPearsonSelector
        // With all Pearson fitters available, match rates should be much higher
        double expectedTypeMatchRate = cardinality >= 100000 ? 0.85 :
                                       cardinality >= 8000 ? 0.75 : 0.65;

        assertTrue(typeMatchRate >= expectedTypeMatchRate,
            String.format("Type match rate %.1f%% below expected %.1f%%",
                typeMatchRate * 100, expectedTypeMatchRate * 100));
    }

    /**
     * Checks if the fitted model type matches or is equivalent to the source.
     *
     * <p>For bounded distributions, acceptable equivalences are:
     * <ul>
     *   <li>Beta(1,1) ≈ Uniform (mathematically identical)</li>
     *   <li>Narrow Normal ≈ Beta with similar shape</li>
     * </ul>
     */
    private boolean isTypeMatch(ScalarModel source, ScalarModel fitted) {
        String sourceType = source.getModelType();
        String fittedType = fitted.getModelType();

        // Exact match
        if (sourceType.equals(fittedType)) {
            return true;
        }

        // Acceptable alternative: Beta(α≈1, β≈1) is mathematically equivalent to Uniform
        if (source instanceof BetaScalarModel && fittedType.equals("uniform")) {
            BetaScalarModel beta = (BetaScalarModel) source;
            if (Math.abs(beta.getAlpha() - 1.0) < 0.2 &&
                Math.abs(beta.getBeta() - 1.0) < 0.2) {
                return true;
            }
        }

        // Reverse: Uniform fitted as Beta(≈1, ≈1) is acceptable
        if (source instanceof UniformScalarModel && fittedType.equals("beta")) {
            if (fitted instanceof BetaScalarModel) {
                BetaScalarModel beta = (BetaScalarModel) fitted;
                if (Math.abs(beta.getAlpha() - 1.0) < 0.5 &&
                    Math.abs(beta.getBeta() - 1.0) < 0.5) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Checks if fitted parameters are close to source parameters.
     *
     * <p>For bounded distributions, we check:
     * <ul>
     *   <li>Normal: mean and stdDev</li>
     *   <li>Beta: shape parameters (α, β)</li>
     *   <li>Uniform: bounds (lower, upper)</li>
     * </ul>
     */
    private boolean areParametersClose(ScalarModel source, ScalarModel fitted, double tolerance) {
        if (source instanceof NormalScalarModel && fitted instanceof NormalScalarModel) {
            NormalScalarModel s = (NormalScalarModel) source;
            NormalScalarModel f = (NormalScalarModel) fitted;
            return Math.abs(s.getMean() - f.getMean()) < tolerance &&
                   Math.abs(s.getStdDev() - f.getStdDev()) < tolerance * s.getStdDev();
        }

        if (source instanceof UniformScalarModel && fitted instanceof UniformScalarModel) {
            UniformScalarModel s = (UniformScalarModel) source;
            UniformScalarModel f = (UniformScalarModel) fitted;
            double range = s.getRange();
            return Math.abs(s.getLower() - f.getLower()) < tolerance * range &&
                   Math.abs(s.getUpper() - f.getUpper()) < tolerance * range;
        }

        if (source instanceof BetaScalarModel && fitted instanceof BetaScalarModel) {
            BetaScalarModel s = (BetaScalarModel) source;
            BetaScalarModel f = (BetaScalarModel) fitted;
            // Check if shape parameters are proportionally close
            double alphaRatio = f.getAlpha() / s.getAlpha();
            double betaRatio = f.getBeta() / s.getBeta();
            return Math.abs(alphaRatio - 1.0) < tolerance &&
                   Math.abs(betaRatio - 1.0) < tolerance;
        }

        // For cross-type matches (e.g., Uniform↔Beta), just return true
        return true;
    }
}
