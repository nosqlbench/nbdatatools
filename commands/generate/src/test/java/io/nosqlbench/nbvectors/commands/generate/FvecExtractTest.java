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

package io.nosqlbench.nbvectors.commands.generate;

import io.nosqlbench.command.generate.FvecExtract;
import io.nosqlbench.nbvectors.api.commands.TestUtils;
import io.nosqlbench.nbvectors.api.commands.VectorFileIO;
import io.nosqlbench.nbvectors.api.fileio.VectorFileArray;
import io.nosqlbench.nbvectors.api.services.FileType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/// Test class for FvecExtract command
/// Tests extraction of vectors from an fvec file using indices from an ivec file
public class FvecExtractTest {

  @TempDir
  Path tempDir;

  private Path indexFile;
  private Path fvecFile;
  private Path outputFile;

  private static final int NUM_INDICES = 10;
  private static final int NUM_VECTORS = 20;
  private static final int DIMENSION = 5;

  /// Set up test files before each test
  @BeforeEach
  void setup() throws IOException {
    // Create a temporary ivec file with indices
    indexFile = tempDir.resolve("indices.ivec");
    TestUtils.createUniformIvecFile(indexFile, NUM_INDICES, 1, 42);

    // Create a temporary fvec file with vectors to extract from
    fvecFile = tempDir.resolve("vectors.fvec");
    TestUtils.createUniformFvecFile(fvecFile, NUM_VECTORS, DIMENSION, 43);

    // Define output path
    outputFile = tempDir.resolve("output.fvec");
  }

  /// Test successful extraction of vectors
  @Test
  void testSuccessfulExtraction() throws Exception {
    // Arrange
    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
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
    try (VectorFileArray<float[]> reader = VectorFileIO.vectorFileArray(
        FileType.xvec,
        float[].class,
        outputFile
    );
         VectorFileArray<int[]> ivecReader = VectorFileIO.vectorFileArray(
             FileType.xvec,
             int[].class,
             indexFile
         );
         VectorFileArray<float[]> originalReader = VectorFileIO.vectorFileArray(
             FileType.xvec,
             float[].class,
             fvecFile
         ))
    {

      assertThat(reader.getSize()).isEqualTo(NUM_INDICES);

      // Verify that the extracted vectors match those from the source file
      for (int i = 0; i < NUM_INDICES; i++) {
        int index = ivecReader.get(i)[0];
        float[] originalVector = originalReader.get(index);
        float[] extractedVector = reader.get(i);

        assertThat(extractedVector).isEqualTo(originalVector);
      }
    }
  }

  /// Test extraction with partial count
  @Test
  void testPartialExtraction() throws Exception {
    // Arrange
    int partialCount = 5;
    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (partialCount - 1),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);

    // Verify the content of the output file using try-with-resources
    try (VectorFileArray reader = VectorFileIO.vectorFileArray(
        FileType.xvec,
        float[].class,
        outputFile
    ))
    {
      assertThat(reader.getSize()).isEqualTo(partialCount);
    }
  }

  /// Test handling of nonexistent source files
  @Test
  void testNonexistentSourceFiles() {
    // Arrange - ivec file doesn't exist
    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        "nonexistent.ivec",
        "--fvec-file",
        fvecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0.." + (NUM_INDICES - 1),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();

    // Arrange - fvec file doesn't exist
    command = new FvecExtract();
    cmd = new CommandLine(command);

    // Act
    exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        "nonexistent.fvec",
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
      dos.writeFloat(999.0f); // Dummy data
    }

    // Verify the initial file size is non-zero
    assertThat(Files.size(outputFile)).isGreaterThan(0);

    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act - run without force flag
    int exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
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
    command = new FvecExtract();
    cmd = new CommandLine(command);
    exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
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
    try (VectorFileArray<float[]> reader = VectorFileIO.vectorFileArray(
        FileType.xvec,
        float[].class,
        outputFile
    );
         VectorFileArray<float[]> originalReader = VectorFileIO.vectorFileArray(
             FileType.xvec,
             float[].class,
             fvecFile
         );
         VectorFileArray<int[]> ivecReader = VectorFileIO.vectorFileArray(
             FileType.xvec,
             int[].class,
             indexFile
         ))
    {

      assertThat(reader.getSize()).isEqualTo(NUM_INDICES);
      assertThat(reader.get(0).length).isEqualTo(DIMENSION);

      // Verify that the extracted vectors match those from the source file
      for (int i = 0; i < NUM_INDICES; i++) {
        int index = ivecReader.get(i)[0];
        float[] originalVector = originalReader.get(index);
        float[] extractedVector = reader.get(i);

        assertThat(extractedVector).isEqualTo(originalVector);
      }
    }
  }

  /// Test with range exceeding available indices
  @Test
  void testRangeExceedingIndices() {
    // Arrange
    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
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
    try (VectorFileArray<float[]> reader = VectorFileIO.vectorFileArray(
        FileType.xvec,
        float[].class,
        outputFile
    ))
    {
      assertThat(reader.getSize()).isEqualTo(NUM_INDICES);
    } catch (Exception e) {
      fail("Could not read output file", e);
    }
  }

  /// Test with negative range value
  @Test
  void testNegativeRange() {
    // Arrange
    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
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

  /// Test with indices that are out of bounds for the fvec file
  @Test
  void testOutOfBoundsIndices() throws IOException {
    // Arrange - create an ivec file with indices beyond the fvec size
    Path largeIndicesFile = tempDir.resolve("large_indices.ivec");
    try (FileOutputStream fos = new FileOutputStream(largeIndicesFile.toFile())) {
      // Write vectors with out-of-bounds indices
      for (int i = 0; i < 5; i++) {
        // Allocate a ByteBuffer with little-endian byte order
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4).order(ByteOrder.LITTLE_ENDIAN);

        // Write dimension (1)
        buffer.putInt(1);

        // Write index that exceeds the fvec size
        buffer.putInt(i + NUM_VECTORS);

        // Write buffer to file
        fos.write(buffer.array());
      }
    }

    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act
    int exitCode = cmd.execute(
        "--ivec-file",
        largeIndicesFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "0..4",
        "--force"
    );

    // Assert - Command should fail with error code when encountering out-of-bounds indices
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();
  }

  /// Test with custom range values including non-zero starting indices
  @Test
  void testCustomRanges() throws Exception {
    // Arrange - create a command with a non-zero starting range
    FvecExtract command = new FvecExtract();
    CommandLine cmd = new CommandLine(command);

    // Act - extract indices 3 to 7 (5 vectors)
    int exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
        "--output",
        outputFile.toString(),
        "--range",
        "3..7",
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();

    // Verify the file contains 5 vectors (indices 3,4,5,6,7)
    try (VectorFileArray<float[]> reader = VectorFileIO.vectorFileArray(
        FileType.xvec,
        float[].class,
        outputFile
    ))
    {
      // Each vector is stored with its dimension header (4 bytes) + dimension*4 bytes
      // so the size should be 5 vectors
      assertThat(reader.getSize()).isEqualTo(5);
    }

    // Test with a range that exceeds the available indices
    command = new FvecExtract();
    cmd = new CommandLine(command);
    Path largeRangeOutput = tempDir.resolve("large_range.fvec");

    // Define a range that is definitely larger than our test file
    exitCode = cmd.execute(
        "--ivec-file",
        indexFile.toString(),
        "--fvec-file",
        fvecFile.toString(),
        "--output",
        largeRangeOutput.toString(),
        "--range",
        "0..100",
        // NUM_INDICES is 10, so this exceeds it
        "--force"
    );

    // The command should succeed with warnings about truncating the range
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(largeRangeOutput)).isTrue();

    // Verify the output contains only NUM_INDICES vectors
    try (VectorFileArray<float[]> reader = VectorFileIO.vectorFileArray(
        FileType.xvec,
        float[].class,
        largeRangeOutput
    ))
    {
      assertThat(reader.getSize()).isEqualTo(NUM_INDICES);
    }
  }
}
