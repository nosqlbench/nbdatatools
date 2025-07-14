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


import io.nosqlbench.jetty.testserver.JettyFileServerExtension;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(JettyFileServerExtension.class)
public class CatalogAccessTest {

  private TestDataSources sources;

  @BeforeEach
  public void setUp() throws IOException {
    // Use the shared Jetty server instance from JettyFileServerExtension
    URL baseUrl = JettyFileServerExtension.getBaseUrl();

    // Create a TestDataSources instance with the server URL
    sources = TestDataSources.ofUrl(baseUrl.toString());
  }

  @Test
  public void testLayoutDownloadAndRealization() {
    Catalog catalog = sources.catalog();
    List<DatasetEntry> dsentries = catalog.datasets();
    dsentries.forEach(System.out::println);
    Optional<DatasetEntry> dsOpt = catalog.findExact("testxvec");
    if (!dsOpt.isPresent()) {
      throw new RuntimeException("Dataset not found");
    }
    DatasetEntry ds = dsOpt.get();

    System.out.println("Found dataset: " + ds.name());
    System.out.println("Attributes: " + ds.attributes());
    ProfileSelector profiles = ds.select();
    TestDataView defaultView = profiles.profile("default");
    BaseVectors basev =
        defaultView.getBaseVectors().orElseThrow(() -> new RuntimeException("base vectors not found"));
    int count = basev.getCount();
    System.out.println("count:" + count);

    // No need for large prebuffer with test data
    CompletableFuture<Void> pbfuture = basev.prebuffer(0, 1024);
    try {
      pbfuture.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testDatasetDownload() {
    Catalog catalog = sources.catalog();
    List<DatasetEntry> datasets = catalog.datasets();

    // Find the testxvec dataset
    Optional<DatasetEntry> datasetOpt = catalog.findExact("testxvec");
    if (!datasetOpt.isPresent()) {
      throw new RuntimeException("Dataset not found");
    }
    DatasetEntry datasetEntry = datasetOpt.get();

    try {
      Path testdir = Files.createTempDirectory("testdir");
      DownloadProgress progress = datasetEntry.download(testdir);
      DownloadResult result;
      try {
        // Poll for results with a shorter timeout for test data
        while ((result = progress.poll(100, TimeUnit.MILLISECONDS)) == null) {
          System.out.println(
              progress.getProgress() + "( " + progress.currentBytes() + "/" + progress.totalBytes()
              + " bytes)");
          System.out.println("progress:" + progress);
        }
        System.out.println("final progress:" + progress);
        System.out.println("final result:" + result);

        // Verify the download was successful
        assertNotNull(result, "Download result should not be null");
        assertTrue(result.isSuccess(), "Download should complete successfully");

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Extended version of testLayoutDownloadAndRealization that verifies the vector values
   * for all vectors in the dataset.
   */
  @Test
  public void testLayoutDownloadAndRealizationWithVectorVerification() {
    // First, do what the original method does
    Catalog catalog = sources.catalog();
    List<DatasetEntry> dsentries = catalog.datasets();
    dsentries.forEach(System.out::println);
    Optional<DatasetEntry> dsOpt = catalog.findExact("testxvec");
    if (!dsOpt.isPresent()) {
      throw new RuntimeException("Dataset not found");
    }
    DatasetEntry ds = dsOpt.get();

    System.out.println("Found dataset: " + ds.name());
    System.out.println("Attributes: " + ds.attributes());
    ProfileSelector profiles = ds.select();
    TestDataView defaultView = profiles.profile("default");
    BaseVectors basev =
        defaultView.getBaseVectors().orElseThrow(() -> new RuntimeException("base vectors not found"));
    int count = basev.getCount();
    System.out.println("count:" + count);

    // No need for large prebuffer with test data
    CompletableFuture<Void> pbfuture = basev.prebuffer(0, 1024);
    try {
      pbfuture.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    // Now, extend the functionality to verify all vector values
    System.out.println("Starting vector verification for all vectors in the dataset");

    // Get the dimensions of each vector
    int dimensions = basev.getVectorDimensions();
    System.out.println("Vector dimensions: " + dimensions);

    // Verify that the dimensions are greater than 0
    assertTrue(dimensions > 0, "Vector dimensions should be greater than 0");

    // Verify all vectors in the dataset
    for (int i = 0; i < count; i++) {
      // Get the vector at index i
      float[] vector = basev.get(i);

      // Verify that the vector is not null
      assertNotNull(vector, "Vector at index " + i + " should not be null");

      // Verify that the vector has the correct dimensions
      assertEquals(dimensions, vector.length, "Vector at index " + i + " should have " + dimensions + " dimensions");

      // Verify that the vector contains valid float values (not NaN or Infinity)
      for (int j = 0; j < dimensions; j++) {
        assertTrue(!Float.isNaN(vector[j]) && !Float.isInfinite(vector[j]), 
                  "Vector at index " + i + ", dimension " + j + " should be a valid float value");
      }

      // Print progress every 1000 vectors
      if (i % 1000 == 0) {
        System.out.println("Verified " + i + " vectors out of " + count);
        // Print a sample of the vector values
        System.out.println("Sample of vector " + i + ": " + formatVectorSample(vector, 5));
      }
    }

    System.out.println("Vector verification completed successfully for all " + count + " vectors");
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

}
