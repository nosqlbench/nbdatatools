package io.nosqlbench.command.analyze.subcommands;

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

import io.nosqlbench.vshapes.model.NormalScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CMD_analyze_profile}.
 *
 * <p>This test is tagged as "long-running" because it performs actual
 * profile analysis which can take significant time. Run with:
 * <pre>
 * mvn test -pl datatools-commands -Plong-running
 * </pre>
 */
@Tag("long-running")
class CMD_analyze_profileTest {

    @Test
    void testProfileVectorFile(@TempDir Path tempDir) throws Exception {
        // Use the test fvec file from resources
        Path testFile = Path.of("../datatools-vectordata/src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");

        if (!Files.exists(testFile)) {
            // Skip if test file doesn't exist in this environment
            System.out.println("Skipping test - test file not found: " + testFile);
            return;
        }

        Path outputFile = tempDir.resolve("test_model.json");

        // Run the command
        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        int exitCode = commandLine.execute(
            "-b", testFile.toString(),
            "-o", outputFile.toString()
        );

        assertEquals(0, exitCode, "Command should succeed");
        assertTrue(Files.exists(outputFile), "Output file should be created");

        // Verify the JSON can be parsed back
        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(outputFile);
        assertNotNull(model);
        assertTrue(model.uniqueVectors() > 0);
        assertTrue(model.dimensions() > 0);

        System.out.println("Created model with " + model.uniqueVectors() + " vectors, " +
            model.dimensions() + " dimensions");
    }

    @Test
    void testProfileWithTruncation(@TempDir Path tempDir) throws Exception {
        Path testFile = Path.of("../datatools-vectordata/src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");

        if (!Files.exists(testFile)) {
            System.out.println("Skipping test - test file not found: " + testFile);
            return;
        }

        Path outputFile = tempDir.resolve("test_model_truncated.json");

        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        int exitCode = commandLine.execute(
            "-b", testFile.toString(),
            "-o", outputFile.toString(),
            "--truncated",
            "--model-type", "normal"
        );

        assertEquals(0, exitCode, "Command should succeed");

        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(outputFile);
        assertNotNull(model);

        // Verify truncation bounds are set
        NormalScalarModel normal = (NormalScalarModel) model.scalarModel(0);
        assertTrue(normal.isTruncated(),
            "Component should be truncated when --truncated is specified");

        System.out.println("Truncated model bounds: [" +
            normal.lower() + ", " +
            normal.upper() + "]");
    }

    @Test
    void testProfileWithSampling(@TempDir Path tempDir) throws Exception {
        Path testFile = Path.of("../datatools-vectordata/src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");

        if (!Files.exists(testFile)) {
            System.out.println("Skipping test - test file not found: " + testFile);
            return;
        }

        Path outputFile = tempDir.resolve("test_model_sampled.json");

        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        int exitCode = commandLine.execute(
            "-b", testFile.toString(),
            "-o", outputFile.toString(),
            "--sample", "10"
        );

        assertEquals(0, exitCode, "Command should succeed with sampling");

        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(outputFile);
        assertNotNull(model);
        assertTrue(model.uniqueVectors() > 0);
    }

    @Test
    void testProfileWithCustomUniqueVectors(@TempDir Path tempDir) throws Exception {
        Path testFile = Path.of("../datatools-vectordata/src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");

        if (!Files.exists(testFile)) {
            System.out.println("Skipping test - test file not found: " + testFile);
            return;
        }

        Path outputFile = tempDir.resolve("test_model_custom_n.json");

        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        int exitCode = commandLine.execute(
            "-b", testFile.toString(),
            "-o", outputFile.toString(),
            "-n", "1000000"
        );

        assertEquals(0, exitCode, "Command should succeed");

        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(outputFile);
        assertEquals(1_000_000, model.uniqueVectors(),
            "Model should use specified unique vectors count");
    }

    @Test
    void testProfileFileNotFound(@TempDir Path tempDir) {
        Path nonExistentFile = tempDir.resolve("nonexistent.fvec");
        Path outputFile = tempDir.resolve("test_model.json");

        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        // Capture stderr
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(errContent));

        try {
            int exitCode = commandLine.execute(
                "-b", nonExistentFile.toString(),
                "-o", outputFile.toString()
            );

            assertEquals(1, exitCode, "Command should fail for non-existent file");
            assertFalse(Files.exists(outputFile), "Output file should not be created");
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    void testProfileWithInlineRange(@TempDir Path tempDir) throws Exception {
        Path testFile = Path.of("../datatools-vectordata/src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");

        if (!Files.exists(testFile)) {
            System.out.println("Skipping test - test file not found: " + testFile);
            return;
        }

        Path outputFile = tempDir.resolve("test_model_range.json");

        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        // Use inline range specification to profile first 100 vectors
        int exitCode = commandLine.execute(
            "-b", testFile.toString() + ":100",
            "-o", outputFile.toString()
        );

        assertEquals(0, exitCode, "Command should succeed with inline range");
        assertTrue(Files.exists(outputFile), "Output file should be created");

        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(outputFile);
        assertNotNull(model);
        // The unique vectors count should reflect the range size (100)
        // or whatever was specified via -n option
        assertEquals(100, model.uniqueVectors(),
            "Model unique vectors should match the range size");
    }

    @Test
    void testProfileWithHalfOpenIntervalRange(@TempDir Path tempDir) throws Exception {
        Path testFile = Path.of("../datatools-vectordata/src/test/resources/testserver/rawdatasets/testxvec/testxvec_base.fvec");

        if (!Files.exists(testFile)) {
            System.out.println("Skipping test - test file not found: " + testFile);
            return;
        }

        Path outputFile = tempDir.resolve("test_model_interval.json");

        CMD_analyze_profile cmd = new CMD_analyze_profile();
        CommandLine commandLine = new CommandLine(cmd);

        // Use half-open interval notation [10,60) to profile 50 vectors
        int exitCode = commandLine.execute(
            "-b", testFile.toString() + ":[10,60)",
            "-o", outputFile.toString()
        );

        assertEquals(0, exitCode, "Command should succeed with interval range");
        assertTrue(Files.exists(outputFile), "Output file should be created");

        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(outputFile);
        assertNotNull(model);
        assertEquals(50, model.uniqueVectors(),
            "Model unique vectors should match the range size (60-10=50)");
    }
}
