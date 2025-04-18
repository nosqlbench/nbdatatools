package io.nosqlbench.nbvectors.commands.merkle;

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