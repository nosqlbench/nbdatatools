package io.nosqlbench.nbvectors.commands.merkle;

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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class CMD_merkleTest {

  @TempDir
  Path tempDir;

  @Test
  public void testCreateMerkleFile() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 3 + 512); // 3.5MB (not a multiple of 1MB)

    // Create a Merkle file for the test file
    CMD_merkle cmd = new CMD_merkle();
    cmd.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");
  }

  /**
   * Creates a test file with the specified size
   * @param size Size of the file in bytes
   * @return Path to the created file
   * @throws IOException If an error occurs creating the file
   */
  private Path createTestFile(int size) throws IOException {
    Path testFile = tempDir.resolve("test-file.dat");
    byte[] data = new byte[size];
    // Fill with some pattern data
    for (int i = 0; i < size; i++) {
      data[i] = (byte)(i % 256);
    }
    Files.write(testFile, data);
    return testFile;
  }
}
