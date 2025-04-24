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

package io.nosqlbench.nbvectors.commands.generate.commands;

import io.nosqlbench.readers.TestUtils;
import io.nosqlbench.readers.UniformFvecReader;
import io.nosqlbench.readers.UniformIvecReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

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
  void testSuccessfulExtraction() throws IOException {
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
        "--count",
        String.valueOf(NUM_INDICES),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();

    // Verify the content of the output file using try-with-resources
    try (UniformFvecReader reader = new UniformFvecReader(outputFile);
         UniformIvecReader ivecReader = new UniformIvecReader(indexFile);
         UniformFvecReader originalReader = new UniformFvecReader(fvecFile)) {
        
      assertThat(reader.getSize()).isEqualTo(NUM_INDICES);
      assertThat(reader.getDimension()).isEqualTo(DIMENSION);
  
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
  void testPartialExtraction() throws IOException {
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
        "--count",
        String.valueOf(partialCount),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(0);

    // Verify the content of the output file using try-with-resources
    try (UniformFvecReader reader = new UniformFvecReader(outputFile)) {
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
        "--count",
        String.valueOf(NUM_INDICES),
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
        "--count",
        String.valueOf(NUM_INDICES),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();
  }

  /// Test overwrite protection (--force flag)
  @Test
  void testOverwriteProtection() throws IOException {
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
        "--count",
        String.valueOf(NUM_INDICES)
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
        "--count",
        String.valueOf(NUM_INDICES),
        "--force"
    );
  
    // Assert - command should succeed with force flag
    assertThat(exitCode).isEqualTo(0);
    assertThat(Files.exists(outputFile)).isTrue();
    
    // Verify the content of the output file after successful overwrite using nested try-with-resources
    try (UniformFvecReader reader = new UniformFvecReader(outputFile);
         UniformFvecReader originalReader = new UniformFvecReader(fvecFile);
         UniformIvecReader ivecReader = new UniformIvecReader(indexFile)) {
      
      assertThat(reader.getSize()).isEqualTo(NUM_INDICES);
      assertThat(reader.getDimension()).isEqualTo(DIMENSION);
      
      // Verify that the extracted vectors match those from the source file
      for (int i = 0; i < NUM_INDICES; i++) {
        int index = ivecReader.get(i)[0];
        float[] originalVector = originalReader.get(index);
        float[] extractedVector = reader.get(i);
        
        assertThat(extractedVector).isEqualTo(originalVector);
      }
    }
  }

  /// Test with count exceeding available indices
  @Test
  void testCountExceedingIndices() {
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
        "--count",
        String.valueOf(NUM_INDICES + 5),
        "--force"
    );

    // Assert
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();
  }

  /// Test with negative count value
  @Test
  void testNegativeCount() {
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
        "--count",
        "-5",
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
    try (var dos = new java.io.DataOutputStream(Files.newOutputStream(largeIndicesFile))) {
      // Write vectors with out-of-bounds indices
      for (int i = 0; i < 5; i++) {
        dos.writeInt(1);
        dos.writeInt(i + NUM_VECTORS); // These indices exceed the fvec size
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
        "--count",
        "5",
        "--force"
    );

    // Assert - Command should fail with error code when encountering out-of-bounds indices
    assertThat(exitCode).isEqualTo(1);
    assertThat(Files.exists(outputFile)).isFalse();
  }
}
