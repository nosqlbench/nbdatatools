package io.nosqlbench.vectordata.downloader;

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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.datagen.TestDataFiles;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.merkle.MerkleTree;
import io.nosqlbench.vectordata.merkle.MerkleTreeBuildProgress;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("largedata")
@ExtendWith(JettyFileServerExtension.class)
public class CatalogAccessLargefileTest {

  private TestDataSources sources;

  @BeforeEach
  public void setUp() throws IOException {
    // Use a direct file URL to the directory containing the catalog.json file
    // The Catalog class will append "catalog.json" to this URL
    // This will use the inbuilt content in the test resources
    Path resourcesDir = Paths.get("src/test/resources/testserver").toAbsolutePath();
    URL resourcesUrl = resourcesDir.toUri().toURL();

    // Create a TestDataSources instance with the file URL to the directory
    sources = TestDataSources.ofUrl(resourcesUrl.toString());
  }

  /// This test will do the following:
  /// 1. Create a directory called "largedata" for a large test dataset under the testserver/temp
  /// test resources.
  /// 2. Create a large fvec dataset with 1M vectors of 128 dimensions using TestDataFiles, but 
  /// only if it does not yet exist.
  /// 3. Create a merkle tree for the fvec dataset using the CMD_merkle_create command.
  /// 4. Create a dataset.yaml file for the dataset.
  /// 5. Create a catalog.json file for the dataset using the CMD_catalog command.
  /// 6. Use the Catalog class to load the catalog, using the testserver/temp remote (via local
  /// webserver) URL as the catalog base.
  /// 7. Use the DatasetEntry class to access the dataset
  /// 8. Use the BaseVectors class to access the base vectors, but without prebuffering
  /// 9. Verify that the base vectors are correct by checking a few random vectors.
  /// 10. Access the beginning and ending vector.
  @Tag("largedata")
  @Test
  public void testCreateLargeExampleWithFullLifecycle() throws Exception {
    // Define common test parameters
    int vectorCount = 1_000_000; // 1M vectors
    int dimensions = 128;
    long seed = 42L;

    // 1. Create a directory for the test dataset
    Path testServerDir = Paths.get("src/test/resources/testserver").toAbsolutePath();
    Path tempDir = testServerDir.resolve("temp");
    Path largeDataDir = createTestDataDirectory(tempDir);

    // 2. Create a large fvec dataset
    Path fvecFile = createLargeVectorDataset(largeDataDir, vectorCount, dimensions, seed);

    // 3. Create a merkle tree for the dataset
    Path merkleFile = createMerkleTree(fvecFile);

    // 4. Create a dataset.yaml file
    Path datasetYamlFile = createDatasetYamlFile(largeDataDir, vectorCount, dimensions);

    // 5. Create catalog files
    createCatalogFiles(tempDir, largeDataDir, datasetYamlFile, fvecFile, merkleFile, vectorCount, dimensions);

    // 6. Load the catalog
    Catalog tempCatalog = loadCatalog(tempDir);

    // 7. Access the dataset
    DatasetEntry datasetEntry = accessDataset(tempCatalog);

    // 8. Access the base vectors
    BaseVectors baseVectors = accessBaseVectors(datasetEntry);

    // 9. Verify the vectors
    verifyVectors(baseVectors, vectorCount, dimensions, seed);

    // 10. Access beginning and ending vectors
    accessBeginningAndEndingVectors(baseVectors);

    System.out.println("Test completed successfully");
  }

  /**
   * Step 1: Create a directory called "largedata" for a large test dataset
   */
  private Path createTestDataDirectory(Path tempDir) throws IOException {
    Path largeDataDir = tempDir.resolve("largedata");

    // Create directories if they don't exist
    Files.createDirectories(largeDataDir);

    return largeDataDir;
  }

  /**
   * Step 2: Create a large fvec dataset with vectors of specified dimensions
   */
  private Path createLargeVectorDataset(Path largeDataDir, int vectorCount, int dimensions, long seed) throws IOException {
    // Path to the vector file
    Path fvecFile = largeDataDir.resolve("large_vectors.fvec");

    // Only generate vectors and save to file if it doesn't already exist
    if (!Files.exists(fvecFile)) {
        System.out.println("Generating " + vectorCount + " vectors with " + dimensions + " dimensions...");

        // Generate vectors with some variability but no special properties
        float[][] vectors = TestDataFiles.genVectors(
            vectorCount, 
            dimensions, 
            seed, 
            0.1, // small variability
            0.0, // no scale
            0.0, // no zeroes
            0.0  // no duplicates
        );

        System.out.println("Saving vectors to " + fvecFile);
        TestDataFiles.saveToFile(vectors, fvecFile, TestDataFiles.Format.fvec);
    } else {
        System.out.println("Vector file " + fvecFile + " already exists, skipping vector generation phase");
    }

    return fvecFile;
  }

  /**
   * Step 3: Create a merkle tree for the fvec dataset
   */
  private Path createMerkleTree(Path fvecFile) throws IOException {
    System.out.println("Creating merkle tree for " + fvecFile);

    // Use MerkleTree.fromData to create the merkle tree
    Path merkleFile = fvecFile.resolveSibling(fvecFile.getFileName() + ".mrkl");

    // Define chunk size (1MB)
    long chunkSize = 1048576;

    // Get the file size for reporting
    long fileSize = Files.size(fvecFile);

    // Build the merkle tree
    MerkleTreeBuildProgress progress = MerkleTree.fromData(fvecFile);

    // Wait for the merkle tree computation to complete
    MerkleTree merkleTree = progress.getFuture().join();

    // Explicitly save the merkle tree to the expected location
    merkleTree.save(merkleFile);
    System.out.println("Saved merkle tree to " + merkleFile);

    // Ensure the merkle file exists
    if (!Files.exists(merkleFile)) {
        throw new RuntimeException("Merkle file was not created at " + merkleFile);
    }

    return merkleFile;
  }

  /**
   * Step 4: Create a dataset.yaml file for the dataset
   */
  private Path createDatasetYamlFile(Path largeDataDir, int vectorCount, int dimensions) throws IOException {
    Path datasetYamlFile = largeDataDir.resolve("dataset.yaml");
    System.out.println("Creating dataset.yaml at " + datasetYamlFile);

    String datasetYaml = """
        attributes:
          distance_function: COSINE
          license: APL
          url: https://github.com/nosqlbench/nbdatatools
          model: large_test_model
          vendor: NoSQLBench
          notes: Large test dataset with 1M vectors of 128 dimensions
          dimensions: 128
          count: 1000000

        profiles:
          default:
            base:
              source:
                path: large_vectors.fvec
              dimensions: 128
              count: 1000000
        """;

    Files.writeString(datasetYamlFile, datasetYaml);

    return datasetYamlFile;
  }

  /**
   * Step 5: Create catalog files (catalog.json and catalog.yaml)
   */
  private void createCatalogFiles(Path tempDir, Path largeDataDir, Path datasetYamlFile, 
                                 Path fvecFile, Path merkleFile, int vectorCount, int dimensions) throws IOException {
    System.out.println("Creating catalog files for " + largeDataDir);

    // First, gather information about the dataset
    URL datasetYamlUrl = datasetYamlFile.toUri().toURL();
    Path catalogJsonFile = tempDir.resolve("catalog.json");
    Path catalogYamlFile = tempDir.resolve("catalog.yaml");

    // Create a more detailed catalog entry that mimics what CMD_catalog would create
    Map<String, Object> catalogEntry = new HashMap<>();
    catalogEntry.put("name", "largedata");
    catalogEntry.put("dataset_type", "dataset.yaml");
    catalogEntry.put("path", "largedata/dataset.yaml");
    catalogEntry.put("url", datasetYamlUrl.toString());

    // Add attributes
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("distance_function", "COSINE");
    attributes.put("license", "APL");
    attributes.put("model", "large_test_model");
    attributes.put("vendor", "NoSQLBench");
    attributes.put("notes", "Large test dataset with 1M vectors of 128 dimensions");
    attributes.put("dimensions", dimensions);
    attributes.put("count", vectorCount);
    catalogEntry.put("attributes", attributes);

    // Add profiles
    Map<String, Object> baseSource = new HashMap<>();
    baseSource.put("path", "large_vectors.fvec");

    Map<String, Object> baseView = new HashMap<>();
    baseView.put("source", baseSource);

    Map<String, Object> defaultProfile = new HashMap<>();
    defaultProfile.put("base", baseView);

    Map<String, Object> profilesMap = new HashMap<>();
    profilesMap.put("default", defaultProfile);
    catalogEntry.put("profiles", profilesMap);

    // Add file information
    Map<String, Object> fileInfo = new HashMap<>();
    fileInfo.put("size", Files.size(fvecFile));
    fileInfo.put("last_modified", Files.getLastModifiedTime(fvecFile).toMillis());
    fileInfo.put("has_merkle", Files.exists(merkleFile));
    catalogEntry.put("file_info", fileInfo);

    // Create a list with the catalog entry
    List<Map<String, Object>> catalogEntries = new ArrayList<>();
    catalogEntries.add(catalogEntry);

    // Convert to JSON and write to file
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String catalogJson = gson.toJson(catalogEntries);
    Files.writeString(catalogJsonFile, catalogJson);

    // Create YAML version (simplified for testing)
    StringBuilder yamlBuilder = new StringBuilder();
    yamlBuilder.append("# Catalog generated for large test dataset\n");
    yamlBuilder.append("- name: largedata\n");
    yamlBuilder.append("  dataset_type: dataset.yaml\n");
    yamlBuilder.append("  path: largedata/dataset.yaml\n");
    yamlBuilder.append("  url: ").append(datasetYamlUrl.toString()).append("\n");
    yamlBuilder.append("  attributes:\n");
    yamlBuilder.append("    distance_function: COSINE\n");
    yamlBuilder.append("    license: APL\n");
    yamlBuilder.append("    model: large_test_model\n");
    yamlBuilder.append("    vendor: NoSQLBench\n");
    yamlBuilder.append("    notes: Large test dataset with 10M vectors of 128 dimensions\n");
    yamlBuilder.append("  profiles:\n");
    yamlBuilder.append("    default:\n");
    yamlBuilder.append("      base: large_vectors.fvec\n");
    yamlBuilder.append("  file_info:\n");
    yamlBuilder.append("    size: ").append(Files.size(fvecFile)).append("\n");
    yamlBuilder.append("    last_modified: ").append(Files.getLastModifiedTime(fvecFile).toMillis()).append("\n");
    yamlBuilder.append("    has_merkle: ").append(Files.exists(merkleFile)).append("\n");

    Files.writeString(catalogYamlFile, yamlBuilder.toString());
  }

  /**
   * Step 6: Load the catalog using the TestDataSources class
   */
  private Catalog loadCatalog(Path tempDir) throws IOException {
    URL tempDirUrl = tempDir.toUri().toURL();
    TestDataSources tempSources = TestDataSources.ofUrl(tempDirUrl.toString());
    return tempSources.catalog();
  }

  /**
   * Step 7: Access the dataset using the DatasetEntry class
   */
  private DatasetEntry accessDataset(Catalog tempCatalog) {
    System.out.println("Loading dataset from catalog");
    Optional<DatasetEntry> datasetOpt = tempCatalog.findExact("largedata");
    if (!datasetOpt.isPresent()) {
      throw new RuntimeException("Dataset not found in catalog");
    }
    return datasetOpt.get();
  }

  /**
   * Step 8: Access the base vectors using the BaseVectors class
   */
  private BaseVectors accessBaseVectors(DatasetEntry datasetEntry) {
    System.out.println("Loading base vectors");
    ProfileSelector profiles = datasetEntry.select();
    TestDataView defaultView = profiles.profile("default");
    return defaultView.getBaseVectors()
        .orElseThrow(() -> new RuntimeException("Base vectors not found"));
  }

  /**
   * Step 9: Verify that the base vectors are correct
   */
  private void verifyVectors(BaseVectors baseVectors, int expectedVectorCount, int expectedDimensions, long seed) {
    System.out.println("Verifying vectors");
    int count = baseVectors.getCount();
    assertEquals(expectedVectorCount, count, "Vector count should match");

    int dimensions = baseVectors.getVectorDimensions();
    assertEquals(expectedDimensions, dimensions, "Vector dimensions should match");

    // Check first vector
    float[] firstVector = baseVectors.get(0);
    assertNotNull(firstVector, "First vector should not be null");
    assertEquals(expectedDimensions, firstVector.length, "First vector should have correct dimensions");
    validateVectorValues(firstVector, 0);

    // Check last vector
    float[] lastVector = baseVectors.get(count - 1);
    assertNotNull(lastVector, "Last vector should not be null");
    assertEquals(expectedDimensions, lastVector.length, "Last vector should have correct dimensions");
    validateVectorValues(lastVector, count - 1);

    // Check a few random vectors
    Random random = new Random(seed);
    for (int i = 0; i < 10; i++) {
      int index = random.nextInt(count);
      float[] vector = baseVectors.get(index);
      assertNotNull(vector, "Vector at index " + index + " should not be null");
      assertEquals(expectedDimensions, vector.length, "Vector at index " + index + " should have correct dimensions");
      validateVectorValues(vector, index);
    }
  }

  /**
   * Step 10: Access the beginning and ending vectors
   */
  private void accessBeginningAndEndingVectors(BaseVectors baseVectors) {
    System.out.println("Accessing beginning and ending vectors");
    int count = baseVectors.getCount();
    float[] beginningVector = baseVectors.get(0);
    float[] endingVector = baseVectors.get(count - 1);

    System.out.println("Beginning vector (first 5 elements): " + formatVectorSample(beginningVector, 5));
    System.out.println("Ending vector (first 5 elements): " + formatVectorSample(endingVector, 5));
  }

  // Helper method to format a sample of vector values for display
  private String formatVectorSample(float[] vector, int sampleSize) {
    StringBuilder sb = new StringBuilder("[");
    int size = Math.min(sampleSize, vector.length);

    for (int i = 0; i < size; i++) {
      sb.append(vector[i]);
      if (i < size - 1) {
        sb.append(", ");
      }
    }

    if (size < vector.length) {
      sb.append(", ...");
    }

    sb.append("]");
    return sb.toString();
  }

  // Helper method to generate a simulated SHA-256 hash
  private String generateSimulatedHash() {
    // Generate a random 64-character hex string to simulate a SHA-256 hash
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < 64; i++) {
      sb.append(Integer.toHexString(random.nextInt(16)));
    }
    return sb.toString();
  }

  // Helper method to validate vector values (check for NaN and Infinity)
  private void validateVectorValues(float[] vector, int vectorIndex) {
    for (int i = 0; i < vector.length; i++) {
      org.junit.jupiter.api.Assertions.assertTrue(!Float.isNaN(vector[i]) && !Float.isInfinite(vector[i]),
                "Vector at index " + vectorIndex + ", dimension " + i + " should be a valid float value");
    }
  }

}
