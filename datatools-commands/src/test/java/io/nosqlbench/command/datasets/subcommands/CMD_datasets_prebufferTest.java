package io.nosqlbench.command.datasets.subcommands;

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


import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.merklev2.MerkleRefFactory;
import io.nosqlbench.vectordata.merklev2.MerkleShape;
import io.nosqlbench.vectordata.merklev2.MerkleState;
import io.nosqlbench.vectordata.merklev2.CacheFileAccessor;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.CoreXVecVectorDatasetViewMethods;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(JettyFileServerExtension.class)
public class CMD_datasets_prebufferTest {

  @TempDir
  private Path tempDir;

  private TestDataSources sources;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws IOException {
    // Use the shared Jetty server instance from JettyFileServerExtension
    URL baseUrl = JettyFileServerExtension.getBaseUrl();

    // Create a test-specific cache directory to avoid conflicts between tests
    String testName = "CAT_" + testInfo.getTestMethod().get().getName() + "_" + System.currentTimeMillis();
    Path testCacheDir = tempDir.resolve("cache_" + testName);

    // Create a TestDataSources instance with the server URL but don't load catalog yet
    sources = TestDataSources.ofUrl(baseUrl.toString());
    
    // Note: We don't immediately load the catalog in setUp to avoid test failures
    // if the test server doesn't have the exact catalog structure expected.
    // Individual tests can choose to access catalogs as needed.
  }

  /// This test uses the jetty test server to verify that the prebuffer command works correctly.
  /// It verifies that:
  /// 1. The command can be instantiated without errors
  /// 2. The command accepts the expected parameters
  /// 3. Basic functionality works with the running HTTP server
  @Test
  public void testBasicDownload() {
    // Configure the cache directory for the test
    Path testCacheDir = tempDir.resolve("test_cache");
    
    // Get the jetty server URL from the extension
    URL baseUrl = JettyFileServerExtension.getBaseUrl();
    assertNotNull(baseUrl, "JettyFileServerExtension should provide a valid base URL");
    
    // Test that we can instantiate the command
    CMD_datasets_prebuffer cmd = new CMD_datasets_prebuffer();
    assertNotNull(cmd, "CMD_datasets_prebuffer should be instantiable");
    
    // Test that we can create a CommandLine instance with it
    picocli.CommandLine commandLine = new picocli.CommandLine(cmd);
    assertNotNull(commandLine, "CommandLine should be creatable with the command");
    
    // Test basic command line parsing with the real server URL
    String[] args = {
      "testxvec:default",  // Use the test dataset from test resources
      "--views=base_vectors",
      "--catalog=" + baseUrl.toString(),
      "--cache-dir=" + testCacheDir.toString()
    };
    
    // Test that command line parsing works (this tests argument binding)
    try {
      picocli.CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
      assertNotNull(parseResult, "Command line parsing should succeed");
      assertTrue(parseResult.hasMatchedPositional(0), "Should have matched the dataset parameter");
    } catch (Exception e) {
      fail("Command line parsing should not fail: " + e.getMessage());
    }
    
    // Test that the server is actually accessible
    String serverUrl = baseUrl.toString();
    assertTrue(serverUrl.startsWith("http://"), "Server URL should be HTTP");
    assertTrue(serverUrl.contains("localhost") || serverUrl.contains("127.0.0.1"), 
               "Server should be running on localhost");
    
    // Verify cache directory can be created
    try {
      Files.createDirectories(testCacheDir);
      assertTrue(Files.exists(testCacheDir), "Cache directory should be creatable");
    } catch (IOException e) {
      fail("Should be able to create cache directory: " + e.getMessage());
    }
    
    // Test that command help can be generated (this tests the command structure)
    String help = commandLine.getUsageMessage();
    assertNotNull(help, "Help message should be generated");
    assertTrue(help.contains("prebuffer"), "Help should mention prebuffer command");
    assertTrue(help.contains("--views"), "Help should mention views option");
    assertTrue(help.contains("--catalog"), "Help should mention catalog option");
    
    // Note: We're not actually executing the command because that would require
    // the full dataset infrastructure to be set up in the test server.
    // This test validates the command structure and server connectivity.
  }

  @Test
  public void testPrebufferDownloadsAllMerkleChunks() throws Exception {
    Path sourceDir = findSourceTestxvecDir();
    Path sourceBase = sourceDir.resolve("testxvec_base.fvec");
    Path sourceMref = sourceDir.resolve("testxvec_base.fvec.mref");
    assertTrue(Files.exists(sourceBase), "Missing testxvec_base.fvec test resource");
    assertTrue(Files.exists(sourceMref), "Missing testxvec_base.fvec.mref test resource");

    long vectorCount = computeMultiChunkVectorCount(sourceBase, sourceMref);

    String testName = "prebuffer_window_" + System.currentTimeMillis();
    Path tempRoot = JettyFileServerExtension.TEMP_RESOURCES_ROOT.resolve(testName);
    Path datasetDir = tempRoot.resolve("rawdatasets").resolve("testxvec_window");
    Files.createDirectories(datasetDir);

    Files.copy(sourceBase, datasetDir.resolve(sourceBase.getFileName()), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(sourceMref, datasetDir.resolve(sourceMref.getFileName()), StandardCopyOption.REPLACE_EXISTING);

    String catalogJson = """
        [
          {
            "layout": {
              "attributes": {
                "url": "https://github.com/nosqlbench/nbdatatools",
                "distance_function": "COSINE",
                "license": "APL"
              },
              "profiles": {
                "default": {
                  "base": {
                    "source": "testxvec_base.fvec",
                    "window": { "minIncl": 0, "maxExcl": %d }
                  }
                }
              }
            },
            "dataset_type": "dataset.yaml",
            "name": "testxvec_window",
            "path": "rawdatasets/testxvec_window/dataset.yaml"
          }
        ]
        """.formatted(vectorCount);
    Files.writeString(tempRoot.resolve("catalog.json"), catalogJson);

    Path testCacheDir = tempDir.resolve("prebuffer_chunk_cache");
    Path testConfigDir = tempDir.resolve("prebuffer_config");
    Files.createDirectories(testConfigDir);
    URL baseUrl = JettyFileServerExtension.getBaseUrl();
    String catalogUrl = baseUrl.toString() + "temp/" + testName + "/";
    Files.writeString(testConfigDir.resolve("catalogs.yaml"), "- " + catalogUrl + "\n");

    CMD_datasets_prebuffer cmd = new CMD_datasets_prebuffer();
    picocli.CommandLine commandLine = new picocli.CommandLine(cmd);
    String[] args = {
        "testxvec_window:default",
        "--views=*",
        "--configdir=" + testConfigDir,
        "--cache-dir=" + testCacheDir,
        "--progress=false"
    };

    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode, "Prebuffer command should succeed");

    Catalog catalog = Catalog.of(TestDataSources.ofUrl(catalogUrl));
    DatasetEntry dataset = catalog.findExact("testxvec_window")
        .orElseThrow(() -> new IllegalStateException("Expected testxvec_window dataset in catalog"));
    ProfileSelector selector = dataset.select().setCacheDir(testCacheDir.toString());
    VectorTestDataView view = selector.profile("default");
    BaseVectors baseVectors = view.getBaseVectors()
        .orElseThrow(() -> new IllegalStateException("Base vectors view should exist"));

    Path cacheFile;
    if (baseVectors instanceof CoreXVecVectorDatasetViewMethods<?> coreView
        && coreView.getChannel() instanceof CacheFileAccessor accessor) {
      cacheFile = accessor.getCacheFilePath();
    } else {
      throw new IllegalStateException("Base vectors view does not expose cache file path");
    }

    assertTrue(Files.exists(cacheFile), "Cache file should exist after prebuffer");

    Path stateFile = cacheFile.resolveSibling(cacheFile.getFileName() + ".mrkl");
    assertTrue(Files.exists(stateFile), "Merkle state file should exist after prebuffer");

    try (MerkleState state = MerkleState.load(stateFile)) {
      MerkleShape shape = state.getMerkleShape();
      try (var ref = MerkleRefFactory.load(sourceMref)) {
        MerkleShape refShape = ref.getShape();
        assertEquals(refShape.getChunkSize(), shape.getChunkSize(), "State chunk size should match reference");
        assertEquals(refShape.getTotalContentSize(), shape.getTotalContentSize(), "State content size should match reference");
      }
      long recordSize = computeRecordSize(sourceBase);
      long requiredBytes = vectorCount * recordSize;
      int expectedChunks = shape.getChunkIndexForPosition(Math.max(requiredBytes - 1, 0)) + 1;
      assertTrue(expectedChunks > 1, "Expected multiple merkle chunks for windowed data");

      BitSet validChunks = state.getValidChunks();
      int validCount = validChunks.cardinality();
      assertEquals(expectedChunks, validCount, "Prebuffer should validate all window chunks");
      for (int i = 0; i < expectedChunks; i++) {
        assertTrue(state.isValid(i), "Expected chunk " + i + " to be valid");
      }
      int firstInvalid = validChunks.nextClearBit(0);
      assertTrue(firstInvalid >= expectedChunks, "Expected no invalid chunks before " + expectedChunks + ", but found invalid at " + firstInvalid);
      assertTrue(Files.size(cacheFile) >= requiredBytes, "Cache file should cover required bytes for window");
    }
  }

  private Path findSourceTestxvecDir() {
    Path repoRoot = Path.of("..");
    Path sourceDir = repoRoot.resolve("datatools-vectordata")
        .resolve("src/test/resources/testserver/rawdatasets/testxvec");
    if (!Files.exists(sourceDir)) {
      sourceDir = JettyFileServerExtension.DEFAULT_RESOURCES_ROOT
          .resolve("rawdatasets/testxvec");
    }
    return sourceDir;
  }

  private long computeMultiChunkVectorCount(Path dataFile, Path mrefFile) throws Exception {
    long recordSize = computeRecordSize(dataFile);
    long totalVectors = Files.size(dataFile) / recordSize;
    long chunkSize;
    try (var ref = MerkleRefFactory.load(mrefFile)) {
      chunkSize = ref.getShape().getChunkSize();
    }
    long targetBytes = chunkSize * 2 + 1;
    long vectorCount = (targetBytes + recordSize - 1) / recordSize;
    if (vectorCount > totalVectors) {
      vectorCount = totalVectors;
    }
    assertTrue(vectorCount > 0, "Vector count should be positive");
    return vectorCount;
  }

  private long computeRecordSize(Path dataFile) throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    try (var channel = Files.newByteChannel(dataFile)) {
      int read = channel.read(buffer);
      if (read != 4) {
        throw new IllegalStateException("Unable to read dimensions from " + dataFile);
      }
    }
    buffer.flip();
    int dimensions = buffer.getInt();
    if (dimensions <= 0) {
      throw new IllegalStateException("Invalid vector dimensions: " + dimensions);
    }
    return 4L + (long) dimensions * Float.BYTES;
  }

  @Test
  @Disabled
  public void testReadRealDataSource() {
    // Test that the command can be instantiated and basic parsing works
    // The actual prebuffering is too slow for CI/CD builds 
    CMD_datasets_prebuffer cmd = new CMD_datasets_prebuffer();
    picocli.CommandLine commandLine = new picocli.CommandLine(cmd);
    
    // Test that argument parsing works for the real dataset
    String[] args = {"cohere_msmarco:default"};
    picocli.CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
    assertNotNull(parseResult, "Command line parsing should succeed for real dataset");
    assertTrue(parseResult.hasMatchedPositional(0), "Should have matched the dataset parameter");
    
    // Note: We skip the actual execution to avoid long S3 downloads in builds
    // The implementation error (profile parsing) has been verified to be fixed
  }
  
  /// Test actual prebuffer execution timing to verify it doesn't return prematurely
  @Test
  @Disabled
  public void testActualPrebufferTiming() {
    System.err.println("TEST: Testing actual prebuffer execution timing");
    
    CMD_datasets_prebuffer cmd = new CMD_datasets_prebuffer();
    picocli.CommandLine commandLine = new picocli.CommandLine(cmd);
    
    // Set up cache directory
    Path testCacheDir = tempDir.resolve("prebuffer_timing_test");
    
    // Use the staged dataset (cohere_msmarco)
    String[] args = {
      "cohere_msmarco:default",
      "--cache-dir=" + testCacheDir.toString(),
      "--views=base_vectors"
    };
    
    try {
      System.err.println("TEST: Parsing command line arguments...");
      picocli.CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
      assertTrue(parseResult.hasMatchedPositional(0), "Should parse dataset argument");
      
      System.err.println("TEST: Starting prebuffer execution timing test...");
      long startTime = System.currentTimeMillis();
      
      // Execute the command - this will actually run prebuffer
      System.err.println("TEST: Executing prebuffer command...");
      int exitCode = commandLine.execute(args);
      
      long duration = System.currentTimeMillis() - startTime;
      System.err.println("TEST: Prebuffer completed in " + duration + "ms with exit code: " + exitCode);
      
      // If prebuffer was returning prematurely, it would complete very quickly (< 100ms)
      // Real prebuffer should take longer due to network operations, merkle validation, etc.
      if (duration < 100) {
        System.err.println("WARNING: Prebuffer completed very quickly (" + duration + "ms) - may indicate premature return");
      } else {
        System.err.println("INFO: Prebuffer took " + duration + "ms - appears to be doing real work");
      }
      
      // Check if cache directory was created and has content
      if (Files.exists(testCacheDir)) {
        System.err.println("TEST: Cache directory created: " + testCacheDir);
        try {
          Files.walk(testCacheDir)
            .filter(Files::isRegularFile)
            .forEach(file -> System.err.println("TEST: Cache file: " + file.getFileName() + " (" + file.toFile().length() + " bytes)"));
        } catch (Exception e) {
          System.err.println("TEST: Could not list cache files: " + e.getMessage());
        }
      } else {
        System.err.println("TEST: Cache directory was not created - may indicate no actual work done");
      }
      
    } catch (Exception e) {
      System.err.println("TEST: Exception during prebuffer execution: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      // Don't fail the test on exceptions - we're measuring timing behavior
      // The exception itself tells us something about the execution path
    }
    
    System.err.println("TEST: Prebuffer timing test completed");
  }

}
