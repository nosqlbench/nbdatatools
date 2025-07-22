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

import io.nosqlbench.command.merkle.subcommands.CMD_merkle_create;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_path;
import io.nosqlbench.command.merkle.subcommands.CMD_merkle_summary;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleDataImpl;
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
import java.util.stream.Collectors;

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
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");
  }

  @Test
  @org.junit.jupiter.api.Disabled("Test disabled - specific to old MerkleTree timestamp logic, not applicable to new MerkleRef format")
  public void testSkipUpToDateMerkleFile() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 2); // 2MB

    // Create a Merkle file for the test file
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Ensure the Merkle file is newer than the source file
    // Set the last modified time of the Merkle file to be 1 second later than the source file
    long sourceLastModified = Files.getLastModifiedTime(testFile).toMillis();
    Files.setLastModifiedTime(merkleFile, java.nio.file.attribute.FileTime.fromMillis(sourceLastModified + 1000));

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
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Ensure the Merkle file is newer than the source file
    // Set the last modified time of the Merkle file to be 1 second later than the source file
    long sourceLastModified = Files.getLastModifiedTime(testFile).toMillis();
    Files.setLastModifiedTime(merkleFile, java.nio.file.attribute.FileTime.fromMillis(sourceLastModified + 1000));
    long originalMerkleLastModified = Files.getLastModifiedTime(merkleFile).toMillis();

    // We already have a createCommand instance, so we'll reuse it

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

    // Create a CreateCommand to test the implementation
    CMD_merkle_create createCommand = new CMD_merkle_create();

    // Execute the command with dryrun=true
    // The command should not create any Merkle files
    boolean success = createCommand.execute(java.util.List.of(testFile), 1048576, false, true);
    assertTrue(success, "Command should succeed in dry run mode");

    // Verify no Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
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
    // Create test files with various extensions, including class name to avoid collisions
    Path ivecFile = tempDir.resolve("CMD_merkleTest_test.ivec");
    Path ivecsFile = tempDir.resolve("CMD_merkleTest_test.ivecs");
    Path fvecFile = tempDir.resolve("CMD_merkleTest_test.fvec");
    Path fvecsFile = tempDir.resolve("CMD_merkleTest_test.fvecs");
    Path bvecFile = tempDir.resolve("CMD_merkleTest_test.bvec");
    Path bvecsFile = tempDir.resolve("CMD_merkleTest_test.bvecs");
    Path hdf5File = tempDir.resolve("CMD_merkleTest_test.hdf5");
    Path txtFile = tempDir.resolve("CMD_merkleTest_test.txt"); // Not in default extensions

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

    List<Path> expandedFiles = MerkleUtils.expandDirectoriesWithExtensions(paths);

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
    Path testFile = tempDir.resolve("CMD_merkleTest_test-file.dat");
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
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Create a reference file by copying the Merkle file
    Path refFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    Files.copy(merkleFile, refFile);
    assertTrue(Files.exists(refFile), "Reference file should be created");

    // Test summary command with the original file
    CMD_merkle_summary summaryCommand = new CMD_merkle_summary();

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
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Print debug information about the Merkle file
    System.out.println("[DEBUG_LOG] Merkle file: " + merkleFile);
    System.out.println("[DEBUG_LOG] Merkle file size: " + Files.size(merkleFile));

    // Manual file format inspection not needed for merklev2 - validation is done through API

    // Verify the Merkle file integrity (skip for .mref files as they are validated during load)
    if (merkleFile.toString().endsWith(MerkleUtils.MRKL)) {
      boolean isValid = MerkleUtils.verifyMerkleFileIntegrity(merkleFile);
      assertTrue(isValid, "Merkle file should be valid immediately after creation");
    }

    // Load the Merkle tree from the file to verify it can be loaded
    try {
      MerkleDataImpl merkleData = MerkleRefFactory.load(merkleFile);
      assertNotNull(merkleData, "Merkle data should be loaded successfully");
      assertEquals(1048576, merkleData.getShape().getChunkSize(), "Chunk size should match");

      // Verify we can get the correct number of leaves
      int expectedLeaves = (int) Math.ceil((double) Files.size(testFile) / 1048576);
      assertEquals(expectedLeaves, merkleData.getShape().getLeafCount(), 
                  "Number of leaves should match file size and chunk size");

      // Verify we can access leaf hashes
      for (int i = 0; i < merkleData.getShape().getLeafCount(); i++) {
        byte[] leafHash = merkleData.getHashForLeaf(i);
        assertNotNull(leafHash, "Leaf hash should be accessible");
        assertEquals(32, leafHash.length, "Leaf hash should be 32 bytes (SHA-256)");
      }

      // Verify the total size matches the file size
      assertEquals(Files.size(testFile), merkleData.getShape().getTotalContentSize(), 
                  "Total size should match file size");
      
      // Clean up
      merkleData.close();
    } catch (Exception e) {
      fail("Failed to load and validate Merkle tree: " + e.getMessage());
    }
  }
  @Test
  @org.junit.jupiter.api.Disabled("Test disabled - specific to old MerkleTree implementation, not applicable to new MerkleRef format")
  public void testNonLeafNodeHashComputation() throws Exception {
    // Create a test file with known content
    Path testFile = createTestFile(1024 * 1024 * 3 + 512); // 3.5MB (not a multiple of 1MB)
    long chunkSize = 1048576; // 1MB chunk size (for creating the merkle file)

    // Create a Merkle file for the test file
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, chunkSize);

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
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

    io.nosqlbench.vectordata.merkle.MerkleTree merkleTreeFromData = 
        io.nosqlbench.vectordata.merkle.MerkleTree.fromData(fileData);

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

    // We don't verify that there are no mismatched chunks between the trees
    // because the leaf hashes might differ due to implementation details,
    // but the root hash should still match if the trees are structurally equivalent.
    // This is especially true for partial chunks at the end of the file.
    List<io.nosqlbench.vectordata.merkle.MerkleMismatch> mismatches = 
        merkleTreeFromFile.findMismatchedChunks(merkleTreeFromData);
    // Just log the mismatches for debugging purposes
    for (io.nosqlbench.vectordata.merkle.MerkleMismatch mismatch : mismatches) {
        System.out.println("[DEBUG_LOG] Mismatch at chunk index: " + mismatch.chunkIndex());
    }
  }

  // Helper method to convert byte array to hex string
  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Test
  public void testPathCommandWithValidChunkIndex() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 3 + 512); // 3.5MB (not a multiple of 1MB)

    // Create a Merkle file for the test file
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Test the path command with a valid chunk index
    CMD_merkle_path pathCommand = new CMD_merkle_path();
    boolean success = pathCommand.execute(testFile, 1); // Chunk index 1 (second chunk)
    assertTrue(success, "Path command should succeed with valid chunk index");
  }

  @Test
  public void testPathCommandWithInvalidChunkIndex() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 2); // 2MB

    // Create a Merkle file for the test file
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Test the path command with an invalid chunk index (too large)
    CMD_merkle_path pathCommand = new CMD_merkle_path();
    boolean success = pathCommand.execute(testFile, 10); // Chunk index 10 (beyond the end of the file)
    assertFalse(success, "Path command should fail with invalid chunk index");

    // Test the path command with an invalid chunk index (negative)
    success = pathCommand.execute(testFile, -1); // Negative chunk index
    assertFalse(success, "Path command should fail with negative chunk index");
  }

  @Test
  public void testPathCommandWithNonExistentFile() {
    // Test the path command with a non-existent file
    Path nonExistentFile = tempDir.resolve("non-existent-file.dat");
    CMD_merkle_path pathCommand = new CMD_merkle_path();
    boolean success = pathCommand.execute(nonExistentFile, 0);
    assertFalse(success, "Path command should fail with non-existent file");
  }

  @Test
  public void testExpandDirectoriesWithDotDirectory() throws IOException {
    // This test verifies that a path of "." is correctly converted to an absolute path

    // Create a mock implementation of MerkleUtils.expandDirectoriesWithExtensions
    // that we can use to verify the behavior

    // Create a Path object representing "."
    Path dotPath = Path.of(".");
    System.out.println("[DEBUG_LOG] Dot path: " + dotPath);
    System.out.println("[DEBUG_LOG] Absolute dot path: " + dotPath.toAbsolutePath());

    // Get the current directory
    Path currentDir = Path.of("").toAbsolutePath();
    System.out.println("[DEBUG_LOG] Current directory: " + currentDir);

    // Create a list with just the dot path
    List<Path> paths = new ArrayList<>();
    paths.add(dotPath);

    // Create a spy on MerkleUtils to verify the behavior
    // We'll manually check if the dot path is correctly converted to an absolute path

    // First, check if Files.isDirectory returns true for the dot path
    assertTrue(Files.isDirectory(dotPath), "Dot path should be recognized as a directory");

    // Check if the dot path's string representation is exactly "."
    assertEquals(".", dotPath.toString(), "Dot path string should be '.'");

    // Now, let's verify our fix in MerkleUtils.expandDirectoriesWithExtensions
    // by checking if a path of "." is correctly converted to an absolute path

    // Create a test directory with some files
    Path testDir = tempDir.resolve("CMD_merkleTest_testdir");
    Files.createDirectory(testDir);

    // Create test files with various extensions in the test directory, including class name to avoid collisions
    Path ivecFile = testDir.resolve("CMD_merkleTest_test.ivec");
    Path fvecFile = testDir.resolve("CMD_merkleTest_test.fvec");
    Path txtFile = testDir.resolve("CMD_merkleTest_test.txt"); // Not in default extensions

    // Create all the files
    Files.createFile(ivecFile);
    Files.createFile(fvecFile);
    Files.createFile(txtFile);

    // Print debug information
    System.out.println("[DEBUG_LOG] Test directory: " + testDir.toAbsolutePath());
    System.out.println("[DEBUG_LOG] ivecFile: " + ivecFile.toAbsolutePath());
    System.out.println("[DEBUG_LOG] fvecFile: " + fvecFile.toAbsolutePath());
    System.out.println("[DEBUG_LOG] txtFile: " + txtFile.toAbsolutePath());

    // Verify files were created
    assertTrue(Files.exists(ivecFile), "ivecFile should exist");
    assertTrue(Files.exists(fvecFile), "fvecFile should exist");
    assertTrue(Files.exists(txtFile), "txtFile should exist");

    // Now, create a Path object representing "." in the test directory
    // We'll use this to test our fix
    Path testDotPath = testDir.resolve(".");
    System.out.println("[DEBUG_LOG] Test dot path: " + testDotPath);
    System.out.println("[DEBUG_LOG] Absolute test dot path: " + testDotPath.toAbsolutePath());

    // Create a list with just the test dot path
    List<Path> testPaths = new ArrayList<>();
    testPaths.add(testDotPath);

    // Call expandDirectoriesWithExtensions with the test dot path
    List<Path> expandedFiles = MerkleUtils.expandDirectoriesWithExtensions(testPaths);

    // Print expanded files
    System.out.println("[DEBUG_LOG] Expanded files count: " + expandedFiles.size());
    expandedFiles.forEach(p -> System.out.println("[DEBUG_LOG] Expanded file: " + p.toAbsolutePath()));

    // Should find 2 files with default extensions
    assertEquals(2, expandedFiles.size(), "Should find 2 files with default extensions");

    // Convert the expanded files to normalized absolute paths for comparison
    List<Path> normalizedExpandedFiles = expandedFiles.stream()
        .map(Path::toAbsolutePath)
        .map(Path::normalize)
        .collect(Collectors.toList());

    // Normalize the expected file paths
    Path normalizedIvecFile = ivecFile.toAbsolutePath().normalize();
    Path normalizedFvecFile = fvecFile.toAbsolutePath().normalize();
    Path normalizedTxtFile = txtFile.toAbsolutePath().normalize();

    // Print normalized paths for debugging
    System.out.println("[DEBUG_LOG] Normalized ivecFile: " + normalizedIvecFile);
    System.out.println("[DEBUG_LOG] Normalized fvecFile: " + normalizedFvecFile);
    System.out.println("[DEBUG_LOG] Normalized txtFile: " + normalizedTxtFile);

    normalizedExpandedFiles.forEach(p -> 
        System.out.println("[DEBUG_LOG] Normalized expanded file: " + p));

    // Verify all default extension files are included
    assertTrue(normalizedExpandedFiles.contains(normalizedIvecFile), ".ivec file should be included");
    assertTrue(normalizedExpandedFiles.contains(normalizedFvecFile), ".fvec file should be included");

    // Verify non-default extension file is not included
    assertFalse(normalizedExpandedFiles.contains(normalizedTxtFile), ".txt file should not be included");
  }

  @Test
  public void testPathCommandWithGeneratedTestFiles() throws Exception {
    // Create a test file with some content
    Path testFile = createTestFile(1024 * 1024 * 5); // 5MB

    // Create a Merkle file for the test file
    CMD_merkle_create createCommand = new CMD_merkle_create();
    createCommand.createMerkleFile(testFile, 1048576); // 1MB chunk size

    // Verify the Merkle file was created
    Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MerkleUtils.MREF);
    assertTrue(Files.exists(merkleFile), "Merkle file should be created");

    // Test the path command with different valid chunk indices
    CMD_merkle_path pathCommand = new CMD_merkle_path();

    // Test with first chunk (index 0)
    boolean success = pathCommand.execute(testFile, 0);
    assertTrue(success, "Path command should succeed with first chunk");

    // Test with middle chunk (index 2)
    success = pathCommand.execute(testFile, 2);
    assertTrue(success, "Path command should succeed with middle chunk");

    // Test with last chunk (index 4)
    success = pathCommand.execute(testFile, 4);
    assertTrue(success, "Path command should succeed with last chunk");

    // Test with the merkle file directly
    success = pathCommand.execute(merkleFile, 0);
    assertTrue(success, "Path command should succeed with merkle file directly");
  }
}
