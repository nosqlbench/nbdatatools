package io.nosqlbench.command.merkle;

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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

  @Test
  public void testSkipUpToDateMerkleFile() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 2); // 2MB

    // Create a Merkle file for the test file
    CMD_merkle cmd = new CMD_merkle();
    cmd.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Ensure the Merkle file is newer than the source file
    // Set the last modified time of the Merkle file to be 1 second later than the source file
    long sourceLastModified = Files.getLastModifiedTime(testFile).toMillis();
    Files.setLastModifiedTime(merkleFile, java.nio.file.attribute.FileTime.fromMillis(sourceLastModified + 1000));

    // Create a MerkleCommand to test the CreateCommand implementation
    MerkleCommand createCommand = MerkleCommand.findByName("create");
    assertNotNull(createCommand, "Create command should be found");

    // Execute the command with force=false
    // The command should skip the file since the Merkle file is newer
    boolean success = createCommand.execute(java.util.List.of(testFile), 1048576, false, false);
    assertTrue(success, "Command should succeed even when skipping files");

    // The Merkle file's last modified time should not have changed
    long merkleLastModifiedAfter = Files.getLastModifiedTime(merkleFile).toMillis();
    assertEquals(sourceLastModified + 1000, merkleLastModifiedAfter,
                "Merkle file should not have been updated");
  }

  @Test
  public void testForceOverwriteUpToDateMerkleFile() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 2); // 2MB

    // Create a Merkle file for the test file
    CMD_merkle cmd = new CMD_merkle();
    cmd.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Ensure the Merkle file is newer than the source file
    // Set the last modified time of the Merkle file to be 1 second later than the source file
    long sourceLastModified = Files.getLastModifiedTime(testFile).toMillis();
    Files.setLastModifiedTime(merkleFile, java.nio.file.attribute.FileTime.fromMillis(sourceLastModified + 1000));
    long originalMerkleLastModified = Files.getLastModifiedTime(merkleFile).toMillis();

    // Create a MerkleCommand to test the CreateCommand implementation
    MerkleCommand createCommand = MerkleCommand.findByName("create");
    assertNotNull(createCommand, "Create command should be found");

    // Add a small delay to ensure the timestamp will be different
    Thread.sleep(1500);

    // Execute the command with force=true
    // The command should overwrite the file even though the Merkle file is newer
    boolean success = createCommand.execute(java.util.List.of(testFile), 1048576, true, false);
    assertTrue(success, "Command should succeed when forcing overwrite");

    // The Merkle file's last modified time should have changed
    long merkleLastModifiedAfter = Files.getLastModifiedTime(merkleFile).toMillis();
    assertTrue(merkleLastModifiedAfter > originalMerkleLastModified,
               "Merkle file should have been updated when using force option");
  }

  @Test
  public void testDryRunOption() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 2); // 2MB

    // Create a MerkleCommand to test the CreateCommand implementation
    MerkleCommand createCommand = MerkleCommand.findByName("create");
    assertNotNull(createCommand, "Create command should be found");

    // Execute the command with dryrun=true
    // The command should not create any Merkle files
    boolean success = createCommand.execute(java.util.List.of(testFile), 1048576, false, true);
    assertTrue(success, "Command should succeed in dry run mode");

    // Verify no Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertFalse(Files.exists(merkleFile), "Merkle file should not be created in dry run mode");

    // Now run without dryrun to create the file
    success = createCommand.execute(java.util.List.of(testFile), 1048576, false, false);
    assertTrue(success, "Command should succeed");

    // Verify the Merkle file was created
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Set the last modified time of the Merkle file to be 1 second later than the source file
    long sourceLastModified = Files.getLastModifiedTime(testFile).toMillis();
    Files.setLastModifiedTime(merkleFile, java.nio.file.attribute.FileTime.fromMillis(sourceLastModified + 1000));

    // Run with dryrun again - should report that it would skip the file
    success = createCommand.execute(java.util.List.of(testFile), 1048576, false, true);
    assertTrue(success, "Command should succeed in dry run mode");

    // The Merkle file's last modified time should not have changed
    long merkleLastModifiedAfter = Files.getLastModifiedTime(merkleFile).toMillis();
    assertEquals(sourceLastModified + 1000, merkleLastModifiedAfter,
                "Merkle file should not have been updated in dry run mode");
  }

  @Test
  public void testExpandDirectoriesWithDefaultExtensions() throws IOException {
    // Create test files with various extensions
    Path ivecFile = tempDir.resolve("test.ivec");
    Path ivecsFile = tempDir.resolve("test.ivecs");
    Path fvecFile = tempDir.resolve("test.fvec");
    Path fvecsFile = tempDir.resolve("test.fvecs");
    Path bvecFile = tempDir.resolve("test.bvec");
    Path bvecsFile = tempDir.resolve("test.bvecs");
    Path hdf5File = tempDir.resolve("test.hdf5");
    Path txtFile = tempDir.resolve("test.txt"); // Not in default extensions

    // Create all the files
    Files.createFile(ivecFile);
    Files.createFile(ivecsFile);
    Files.createFile(fvecFile);
    Files.createFile(fvecsFile);
    Files.createFile(bvecFile);
    Files.createFile(bvecsFile);
    Files.createFile(hdf5File);
    Files.createFile(txtFile);

    // Test with a single directory and no extensions
    List<Path> paths = new ArrayList<>();
    paths.add(tempDir);

    List<Path> expandedFiles = CMD_merkle.expandDirectoriesWithExtensions(paths);

    // Should find 7 files with default extensions
    assertEquals(7, expandedFiles.size(), "Should find 7 files with default extensions");

    // Verify all default extension files are included
    assertTrue(expandedFiles.contains(ivecFile), ".ivec file should be included");
    assertTrue(expandedFiles.contains(ivecsFile), ".ivecs file should be included");
    assertTrue(expandedFiles.contains(fvecFile), ".fvec file should be included");
    assertTrue(expandedFiles.contains(fvecsFile), ".fvecs file should be included");
    assertTrue(expandedFiles.contains(bvecFile), ".bvec file should be included");
    assertTrue(expandedFiles.contains(bvecsFile), ".bvecs file should be included");
    assertTrue(expandedFiles.contains(hdf5File), ".hdf5 file should be included");

    // Verify non-default extension file is not included
    assertFalse(expandedFiles.contains(txtFile), ".txt file should not be included");
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

  @Test
  public void testSummaryCommandWithMerkleFile() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 2); // 2MB

    // Create a Merkle file for the test file
    CMD_merkle cmd = new CMD_merkle();
    cmd.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Create a reference file by copying the Merkle file
    Path refFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MREF);
    Files.copy(merkleFile, refFile);
    assertTrue(Files.exists(refFile), "Reference file should be created");

    // Test summary command with the original file
    MerkleCommand summaryCommand = MerkleCommand.findByName("summary");
    assertNotNull(summaryCommand, "Summary command should be found");

    boolean success = summaryCommand.execute(List.of(testFile), 1048576, false, true);
    assertTrue(success, "Summary command should succeed with original file");

    // Test summary command with the Merkle file directly
    success = summaryCommand.execute(List.of(merkleFile), 1048576, false, true);
    assertTrue(success, "Summary command should succeed with Merkle file");

    // Test summary command with the reference file directly
    success = summaryCommand.execute(List.of(refFile), 1048576, false, true);
    assertTrue(success, "Summary command should succeed with reference file");
  }

  @Test
  public void testCreateAndVerifyMerkleFile() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 3 + 512); // 3.5MB (not a multiple of 1MB)

    // Create a Merkle file for the test file
    CMD_merkle cmd = new CMD_merkle();
    cmd.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Verify the Merkle file integrity
    boolean isValid = cmd.verifyMerkleFileIntegrity(merkleFile);
    assertTrue(isValid, "Merkle file should be valid immediately after creation");

    // Load the Merkle tree from the file to verify it can be loaded
    try {
      io.nosqlbench.vectordata.merkle.MerkleTree merkleTree = 
          io.nosqlbench.vectordata.merkle.MerkleTree.load(merkleFile);
      assertNotNull(merkleTree, "Merkle tree should be loaded successfully");
      assertEquals(1048576, merkleTree.getChunkSize(), "Chunk size should match");

      // Verify we can get the correct number of leaves
      int expectedLeaves = (int) Math.ceil((double) Files.size(testFile) / 1048576);
      assertEquals(expectedLeaves, merkleTree.getNumberOfLeaves(), 
                  "Number of leaves should match file size and chunk size");

      // Verify we can access leaf hashes
      for (int i = 0; i < merkleTree.getNumberOfLeaves(); i++) {
        byte[] leafHash = merkleTree.getHashForLeaf(i);
        assertNotNull(leafHash, "Leaf hash should be accessible");
        assertEquals(32, leafHash.length, "Leaf hash should be 32 bytes (SHA-256)");
      }

      // Verify the total size matches the file size
      assertEquals(Files.size(testFile), merkleTree.totalSize(), 
                  "Total size should match file size");
    } catch (Exception e) {
      fail("Failed to load and validate Merkle tree: " + e.getMessage());
    }
  }
  @Test
  public void testNonLeafNodeHashComputation() throws Exception {
    // Create a test file with known content
    Path testFile = createTestFile(1024 * 1024 * 3 + 512); // 3.5MB (not a multiple of 1MB)
    long chunkSize = 1048576; // 1MB chunk size

    // Create a Merkle file for the test file using CMD_merkle
    CMD_merkle cmd = new CMD_merkle();
    cmd.createMerkleFile(testFile, chunkSize);

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleCommand.MRKL);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Load the Merkle tree from the file
    io.nosqlbench.vectordata.merkle.MerkleTree merkleTreeFromFile = 
        io.nosqlbench.vectordata.merkle.MerkleTree.load(merkleFile);

    // Create a Merkle tree directly from the file data using fromData
    ByteBuffer fileData = ByteBuffer.allocate((int)Files.size(testFile));
    try (FileChannel channel = FileChannel.open(testFile, StandardOpenOption.READ)) {
      channel.read(fileData);
      fileData.flip();
    }

    io.nosqlbench.vectordata.merkle.MerkleRange fullRange = 
        new io.nosqlbench.vectordata.merkle.MerkleRange(0, Files.size(testFile));
    io.nosqlbench.vectordata.merkle.MerkleTree merkleTreeFromData = 
        io.nosqlbench.vectordata.merkle.MerkleTree.fromData(fileData, chunkSize, fullRange);

    // Compare the root hashes of both trees
    // We need to use reflection to access the getHash method since it's package-private
    Method getHashMethod = io.nosqlbench.vectordata.merkle.MerkleTree.class.getDeclaredMethod("getHash", int.class);
    getHashMethod.setAccessible(true);

    byte[] rootHashFromFile = (byte[]) getHashMethod.invoke(merkleTreeFromFile, 0);
    byte[] rootHashFromData = (byte[]) getHashMethod.invoke(merkleTreeFromData, 0);

    // Print out the root hashes for debugging
    System.out.println("[DEBUG_LOG] Root hash from file: " + bytesToHex(rootHashFromFile));
    System.out.println("[DEBUG_LOG] Root hash from data: " + bytesToHex(rootHashFromData));

    // Compare leaf hashes to see if they match
    int leafCount = merkleTreeFromFile.getNumberOfLeaves();
    System.out.println("[DEBUG_LOG] Number of leaves: " + leafCount);

    boolean allLeafHashesMatch = true;
    for (int i = 0; i < leafCount; i++) {
      byte[] leafHashFromFile = merkleTreeFromFile.getHashForLeaf(i);
      byte[] leafHashFromData = merkleTreeFromData.getHashForLeaf(i);

      if (!Arrays.equals(leafHashFromFile, leafHashFromData)) {
        System.out.println("[DEBUG_LOG] Leaf hash mismatch at index " + i);
        System.out.println("[DEBUG_LOG] Leaf hash from file: " + bytesToHex(leafHashFromFile));
        System.out.println("[DEBUG_LOG] Leaf hash from data: " + bytesToHex(leafHashFromData));
        allLeafHashesMatch = false;
      }
    }

    System.out.println("[DEBUG_LOG] All leaf hashes match: " + allLeafHashesMatch);

    // Print out internal node hashes to see where the divergence occurs
    // Get the offset (index of first leaf node)
    Field offsetField = io.nosqlbench.vectordata.merkle.MerkleTree.class.getDeclaredField("offset");
    offsetField.setAccessible(true);
    int offset = (int) offsetField.get(merkleTreeFromFile);
    System.out.println("[DEBUG_LOG] Offset (index of first leaf): " + offset);

    // Print out all internal node hashes
    for (int i = 0; i < offset; i++) {
      byte[] internalHashFromFile = (byte[]) getHashMethod.invoke(merkleTreeFromFile, i);
      byte[] internalHashFromData = (byte[]) getHashMethod.invoke(merkleTreeFromData, i);

      System.out.println("[DEBUG_LOG] Internal node " + i + " hash from file: " + bytesToHex(internalHashFromFile));
      System.out.println("[DEBUG_LOG] Internal node " + i + " hash from data: " + bytesToHex(internalHashFromData));
      System.out.println("[DEBUG_LOG] Internal node " + i + " hashes match: " + 
                         Arrays.equals(internalHashFromFile, internalHashFromData));
    }

    // Manually compute the root hash by combining the hashes of its children
    // Get the DIGEST field from MerkleTree
    Field digestField = io.nosqlbench.vectordata.merkle.MerkleTree.class.getDeclaredField("DIGEST");
    digestField.setAccessible(true);
    MessageDigest digest = (MessageDigest) digestField.get(null); // static field, so null is the instance

    // Get the hashes of the children of the root node
    byte[] leftChildHash = (byte[]) getHashMethod.invoke(merkleTreeFromFile, 1);
    byte[] rightChildHash = (byte[]) getHashMethod.invoke(merkleTreeFromFile, 2);

    // Compute the root hash manually
    digest.reset();
    digest.update(leftChildHash);
    digest.update(rightChildHash);
    byte[] manualRootHash = digest.digest();

    System.out.println("[DEBUG_LOG] Manually computed root hash: " + bytesToHex(manualRootHash));
    System.out.println("[DEBUG_LOG] Manually computed root hash matches file: " + 
                       Arrays.equals(manualRootHash, rootHashFromFile));
    System.out.println("[DEBUG_LOG] Manually computed root hash matches data: " + 
                       Arrays.equals(manualRootHash, rootHashFromData));

    // The root hashes should be equal if the non-leaf node hash computation is correct
    assertTrue(Arrays.equals(rootHashFromFile, rootHashFromData), 
               "Root hashes should match, indicating correct non-leaf node hash computation");

    // Also verify that there are no mismatched chunks between the trees
    List<io.nosqlbench.vectordata.merkle.MerkleMismatch> mismatches = 
        merkleTreeFromFile.findMismatchedChunks(merkleTreeFromData);
    assertTrue(mismatches.isEmpty(), 
               "There should be no mismatched chunks between the trees");
  }

  // Helper method to convert byte array to hex string
  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
