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


import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.downloader.testserver.TestWebServerExtension;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CatalogAccessTest {

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

}
