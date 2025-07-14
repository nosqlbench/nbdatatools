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

package io.nosqlbench.command.generate;

import io.nosqlbench.command.generate.subcommands.CMD_generate_ivecExtract;
import io.nosqlbench.nbdatatools.api.services.VectorFileIO;
import io.nosqlbench.nbdatatools.api.fileio.VectorFileArray;
import io.nosqlbench.nbdatatools.api.services.FileType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/// Test class for IvecExtract command
/// Tests extraction of indices from an ivec file using a range specification
public class IvecExtractTest {

  @TempDir
  Path tempDir;

  private Path ivecFile;
  private Path outputFile;

  private static final int NUM_INDICES = 20;

  /// Set up test files before each test
  @BeforeEach
  void setup() throws IOException {
    // Create a temporary ivec file with indices
    ivecFile = tempDir.resolve("indices.ivec");
    TestUtils.createUniformIvecFile(ivecFile, NUM_INDICES, 1, 42);

    // Define output path
    outputFile = tempDir.resolve("output.ivec");
  }

  /// Test successful extraction of indices
  @Test
  void testSuccessfulExtraction() throws Exception {
    // Arrange
    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (NUM_INDICES - 1),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();

    // Verify the content of the output file using try-with-resources
    try (VectorFileArray<int[]> reader = VectorFileIO.randomAccess(
        FileType.xvec,
        int[].class,
        outputFile
    );
         VectorFileArray<int[]> originalReader = VectorFileIO.randomAccess(
             FileType.xvec,
             int[].class,
             ivecFile
         ))
    {
      assertThat(reader.size()).isEqualTo(NUM_INDICES);

      // Verify that the extracted indices match those from the source file
      for (int i = 0; i < NUM_INDICES; i++) {
        int[] originalIndex = originalReader.get(i);
        int[] extractedIndex = reader.get(i);

        assertThat(extractedIndex).isEqualTo(originalIndex);
      }
    }
  }

  /// Test extraction with partial count
  @Test
  void testPartialExtraction() throws Exception {
    // Arrange
    int partialCount = 5;
    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (partialCount - 1),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);

    // Verify the content of the output file using try-with-resources
    try (VectorFileArray<int[]> reader = VectorFileIO.randomAccess(
        FileType.xvec,
        int[].class,
        outputFile
    ))
    {
      assertThat(reader.size()).isEqualTo(partialCount);
    }
  }

  /// Test handling of nonexistent source files
  @Test
  void testNonexistentSourceFiles() {
    // Arrange - ivec file doesn't exist
    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        "nonexistent.ivec",
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (NUM_INDICES - 1),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();
  }

  /// Test overwrite protection (--force flag)
  @Test
  void testOverwriteProtection() throws Exception {
    // Arrange - create the output file first with dummy content
    Files.createFile(outputFile);
    try (var dos = new java.io.DataOutputStream(Files.newOutputStream(outputFile))) {
      dos.writeInt(1); // Dimension of 1
      dos.writeInt(999); // Dummy data
    }

    // Verify the initial file size is non-zero
    assertThat(Files.size(outputFile)).isGreaterThan(0);

    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act - run without force flag
    int exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (NUM_INDICES - 1)
    );

    // Assert - command should fail without force flag
    assertThat(exitCode).isEqualTo(1);
    // File should still exist with original content
    assertThat(Files.exists(outputFile)).isTrue();

    // Act - run with force flag
    command = new CMD_generate_ivecExtract();
    cmd = new CommandLine(command);
    exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (NUM_INDICES - 1),
        "--force"
    );

    // Assert - command should succeed with force flag
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();

    // Verify the content of the output file after successful overwrite using nested try-with-resources
    try (VectorFileArray<int[]> reader = VectorFileIO.randomAccess(
        FileType.xvec,
        int[].class,
        outputFile
    );
         VectorFileArray<int[]> originalReader = VectorFileIO.randomAccess(
             FileType.xvec,
             int[].class,
             ivecFile
         ))
    {
      assertThat(reader.size()).isEqualTo(NUM_INDICES);
      assertThat(reader.get(0).length).isEqualTo(1);

      // Verify that the extracted indices match those from the source file
      for (int i = 0; i < NUM_INDICES; i++) {
        int[] originalIndex = originalReader.get(i);
        int[] extractedIndex = reader.get(i);

        assertThat(extractedIndex).isEqualTo(originalIndex);
      }
    }
  }

  /// Test with range exceeding available indices
  @Test
  void testRangeExceedingIndices() {
    // Arrange
    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (NUM_INDICES + 5),
        "--force"
    );

    // Assert
    // The command should succeed but with a warning about truncating the range
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();

    // Verify the content has only NUM_INDICES vectors
    try (VectorFileArray<int[]> reader = VectorFileIO.randomAccess(
        FileType.xvec,
        int[].class,
        outputFile
    ))
    {
      assertThat(reader.size()).isEqualTo(NUM_INDICES);
    } catch (Exception e) {
      fail("Could not read output file", e);
    }
  }

  /// Test with negative range value
  @Test
  void testNegativeRange() {
    // Arrange
    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "-5..5",
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();
  }

  /// Test with custom range values including non-zero starting indices
  @Test
  void testCustomRanges() throws Exception {
    // Arrange - create a command with a non-zero starting range
    CMD_generate_ivecExtract command = new CMD_generate_ivecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act - extract indices 3 to 7 (5 indices)
    int exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "3..7",
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();

    // Verify the file contains 5 indices (indices 3,4,5,6,7)
    try (VectorFileArray<int[]> reader = VectorFileIO.randomAccess(
        FileType.xvec,
        int[].class,
        outputFile
    ))
    {
      assertThat(reader.size()).isEqualTo(5);
    }

    // Test with a range that exceeds the available indices
    command = new CMD_generate_ivecExtract();
    cmd = new CommandLine(command);
    Path largeRangeOutput = tempDir.resolve("large_range.ivec");

    // Define a range that is definitely larger than our test file
    exitCode = cmd.execute(
        "--ivec-file",
        ivecFile.toString(),
        "--output",
        largeRangeOutput.toString(),
        "--range",
        "0..100",
        // NUM_INDICES is 20, so this exceeds it
        "--force"
    );

    // The command should succeed with warnings about truncating the range
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(largeRangeOutput)).isTrue();

    // Verify the output contains only NUM_INDICES indices
    try (VectorFileArray<int[]> reader = VectorFileIO.randomAccess(
        FileType.xvec,
        int[].class,
        largeRangeOutput
    ))
    {
      assertThat(reader.size()).isEqualTo(NUM_INDICES);
    }
  }
}