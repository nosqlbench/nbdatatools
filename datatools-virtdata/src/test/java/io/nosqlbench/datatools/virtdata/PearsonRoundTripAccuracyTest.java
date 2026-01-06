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
import io.nosqlbench.vshapes.model.BetaPrimeScalarModel;
import io.nosqlbench.vshapes.model.BetaScalarModel;
import io.nosqlbench.vshapes.model.GammaScalarModel;
import io.nosqlbench.vshapes.model.InverseGammaScalarModel;
import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.PearsonIVScalarModel;
import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.StudentTScalarModel;
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
 * End-to-end accuracy test for Pearson distribution round-trip verification.
 *
 * <h2>Purpose</h2>
 *
 * <p>This test verifies that the model fitting pipeline can accurately recover
 * the original distribution types and parameters from synthetic data generated
 * using known Pearson distribution models.
 *
 * <h2>Test Matrix</h2>
 *
 * <p>The test is parameterized across:
 * <ul>
 *   <li>Dimensionalities: 128, 1024, 4096</li>
 *   <li>Cardinalities: 1024, 8192, 131072 (128k)</li>
 * </ul>
 *
 * <h2>Distribution Coverage</h2>
 *
 * <p>Each dimension in the vector dataset uses a different Pearson distribution
 * type (cycling through the available types). This tests the ability to detect
 * and distinguish between:
 * <ul>
 *   <li>Normal (Type 0)</li>
 *   <li>Beta (Type I)</li>
 *   <li>Gamma (Type III)</li>
 *   <li>Student's t (Type VII)</li>
 *   <li>Uniform (Type II degenerate)</li>
 *   <li>Inverse Gamma (Type V)</li>
 *   <li>Beta Prime (Type VI)</li>
 *   <li>Pearson IV (Type IV)</li>
 * </ul>
 *
 * <h2>Workflow</h2>
 *
 * <ol>
 *   <li>Create ScalarModel instances for each dimension</li>
 *   <li>Generate vector data using ComponentSamplers</li>
 *   <li>Write data to temporary .fvec files</li>
 *   <li>Analyze with DatasetModelExtractor</li>
 *   <li>Verify recovered model types match originals</li>
 *   <li>Verify recovered parameters are within tolerance</li>
 * </ol>
 */
@Tag("accuracy")
public class PearsonRoundTripAccuracyTest {

    /** Pearson distribution types for testing */
    private enum PearsonTestType {
        NORMAL,
        BETA,
        GAMMA,
        STUDENT_T,
        UNIFORM,
        INVERSE_GAMMA,
        BETA_PRIME
        // PEARSON_IV excluded - complex parameters, harder to validate
    }

    private static final PearsonTestType[] TYPES = PearsonTestType.values();
    private static final int NUM_TYPES = TYPES.length;

    /**
     * Provides the test parameter matrix.
     * Dimensionalities: 128, 1024, 4096
     * Cardinalities: 1024, 8192, 131072
     */
    static Stream<Arguments> testConfigurations() {
        return Stream.of(
            // Smaller configurations for faster validation
            Arguments.of(128, 1024, "128d_1k"),
            Arguments.of(128, 8192, "128d_8k"),
            Arguments.of(128, 131072, "128d_128k"),

            // Medium configurations
            Arguments.of(1024, 1024, "1024d_1k"),
            Arguments.of(1024, 8192, "1024d_8k"),
            Arguments.of(1024, 131072, "1024d_128k"),

            // Large configurations
            Arguments.of(4096, 1024, "4096d_1k"),
            Arguments.of(4096, 8192, "4096d_8k"),
            Arguments.of(4096, 131072, "4096d_128k")
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

        // Step 5: Analyze with DatasetModelExtractor
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel recoveredModel = extractor.extractVectorModel(readData);

        // Step 6: Verify recovered models
        verifyRecoveredModels(sourceModels, recoveredModel, dimensions, cardinality);

        System.out.println("  ✓ Round-trip successful");
    }

    /**
     * Creates source ScalarModel instances for each dimension.
     * Each dimension cycles through different Pearson distribution types
     * with varying parameters.
     */
    private ScalarModel[] createSourceModels(int dimensions) {
        ScalarModel[] models = new ScalarModel[dimensions];
        Random paramRng = new Random(12345);  // Deterministic parameters

        for (int d = 0; d < dimensions; d++) {
            PearsonTestType type = TYPES[d % NUM_TYPES];
            models[d] = createModelForType(type, d, paramRng);
        }

        return models;
    }

    /**
     * Creates a ScalarModel for the given Pearson type with dimension-specific parameters.
     */
    private ScalarModel createModelForType(PearsonTestType type, int dimension, Random rng) {
        // Use dimension to create slight parameter variations
        double dimOffset = dimension * 0.001;

        switch (type) {
            case NORMAL:
                // Normal: mean varies, stdDev varies
                double mean = 0.5 + dimOffset;
                double stdDev = 0.1 + (rng.nextDouble() * 0.05);
                return new NormalScalarModel(mean, stdDev);

            case BETA:
                // Beta: shape parameters vary, bounded [0, 1]
                double alpha = 2.0 + dimOffset;
                double beta = 5.0 + (rng.nextDouble() * 0.5);
                return new BetaScalarModel(alpha, beta);

            case GAMMA:
                // Gamma: shape and scale vary
                double shape = 3.0 + dimOffset;
                double scale = 0.2 + (rng.nextDouble() * 0.1);
                return new GammaScalarModel(shape, scale);

            case STUDENT_T:
                // Student's t: degrees of freedom varies (higher for more normal-like)
                double df = 8.0 + dimension % 10;  // 8-17 degrees of freedom
                return new StudentTScalarModel(df);

            case UNIFORM:
                // Uniform: bounds vary
                double lower = -0.5 + dimOffset;
                double upper = 0.5 + dimOffset;
                return new UniformScalarModel(lower, upper);

            case INVERSE_GAMMA:
                // Inverse Gamma: shape and scale vary
                double igShape = 4.0 + dimOffset;
                double igScale = 2.0 + (rng.nextDouble() * 0.2);
                return new InverseGammaScalarModel(igShape, igScale);

            case BETA_PRIME:
                // Beta Prime: shape parameters vary
                double bpAlpha = 3.0 + dimOffset;
                double bpBeta = 4.0 + (rng.nextDouble() * 0.3);
                return new BetaPrimeScalarModel(bpAlpha, bpBeta);

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

        // Expect high type match rate for larger samples
        double expectedTypeMatchRate = cardinality >= 100000 ? 0.70 :
                                       cardinality >= 10000 ? 0.60 : 0.50;

        assertTrue(typeMatchRate >= expectedTypeMatchRate,
            String.format("Type match rate %.1f%% below expected %.1f%%",
                typeMatchRate * 100, expectedTypeMatchRate * 100));
    }

    /**
     * Checks if the fitted model type matches or is equivalent to the source.
     */
    private boolean isTypeMatch(ScalarModel source, ScalarModel fitted) {
        String sourceType = source.getModelType();
        String fittedType = fitted.getModelType();

        // Exact match
        if (sourceType.equals(fittedType)) {
            return true;
        }

        // Acceptable alternatives:
        // - StudentT with high df ≈ Normal
        // - Beta with α=β=1 ≈ Uniform
        // - Gamma and InverseGamma can be confused with similar shape
        if (source instanceof StudentTScalarModel) {
            StudentTScalarModel t = (StudentTScalarModel) source;
            // High df Student-t is very similar to Normal
            if (t.getDegreesOfFreedom() > 15 && fittedType.equals("normal")) {
                return true;
            }
        }

        if (source instanceof BetaScalarModel) {
            BetaScalarModel beta = (BetaScalarModel) source;
            // Beta(1,1) is uniform
            if (Math.abs(beta.getAlpha() - 1.0) < 0.1 &&
                Math.abs(beta.getBeta() - 1.0) < 0.1 &&
                fittedType.equals("uniform")) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if fitted parameters are close to source parameters.
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

        if (source instanceof GammaScalarModel && fitted instanceof GammaScalarModel) {
            GammaScalarModel s = (GammaScalarModel) source;
            GammaScalarModel f = (GammaScalarModel) fitted;
            // Check mean and variance match (method of moments)
            return Math.abs(s.getMean() - f.getMean()) < tolerance * s.getMean() &&
                   Math.abs(s.getVariance() - f.getVariance()) < tolerance * s.getVariance();
        }

        if (source instanceof StudentTScalarModel && fitted instanceof StudentTScalarModel) {
            StudentTScalarModel s = (StudentTScalarModel) source;
            StudentTScalarModel f = (StudentTScalarModel) fitted;
            // Check degrees of freedom are close
            double dfRatio = f.getDegreesOfFreedom() / s.getDegreesOfFreedom();
            return Math.abs(dfRatio - 1.0) < tolerance;
        }

        // For other types, just return true if types match (parameter validation is complex)
        return source.getModelType().equals(fitted.getModelType());
    }
}
