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
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CatalogAccessTest {

  private static TestDataSources sources = TestDataSources.ofUrl(
      "https://jvector-datasets-shared.s3.us-east-1.amazonaws.com/faed719b5520a075f2281efb8c820834/ANN_SIFT1B/");

  @Test
  @Disabled("Requires internet access and real data")
  public void testLayoutDownloadAndRealization() {
    Catalog catalog = sources.catalog();
    List<DatasetEntry> dsentries = catalog.datasets();
    dsentries.forEach(System.out::println);
    Optional<DatasetEntry> dsOpt = catalog.findExact("ANN_SIFT1B");
    if (!dsOpt.isPresent()) {
      throw new RuntimeException("Dataset not found");
    }
    DatasetEntry ds = dsOpt.get();

    System.out.println("Found dataset: " + ds.name());
    System.out.println("Attributes: " + ds.attributes());
    ProfileSelector profiles = ds.select();
    TestDataView d1m = profiles.profile("1M");
    BaseVectors basev =
        d1m.getBaseVectors().orElseThrow(() -> new RuntimeException("base vectors not found"));
    int count = basev.getCount();
    System.out.println("count:" + count);
    CompletableFuture<Void> pbfuture = basev.prebuffer(0, 1024 * 1024 * 100);

    CompletableFuture<Void> pbfuture2 = basev.prebuffer(1024 * 1024 * 100, 1024 * 1024 * 200);
    try {
      pbfuture2.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    ///    Object values = basev.get(0);
///    System.out.println("values:" + Arrays.toString(values));


  }

  @Disabled
  @Test
  public void testDatasetDownload() {
    Catalog catalog = sources.catalog();
    List<DatasetEntry> datasets = catalog.datasets();
    DatasetEntry datasetEntry = datasets.get(0);
    try {
      Path testdir = Files.createTempDirectory("testdir");
      DownloadProgress progress = datasetEntry.download(testdir);
      DownloadResult result;
      try {
        while ((result = progress.poll(1, TimeUnit.SECONDS)) == null) {
          System.out.println(
              progress.getProgress() + "( " + progress.currentBytes() + "/" + progress.totalBytes()
              + " bytes)");
          System.out.println("progress:" +progress);
          System.out.println("result:"+result);
        }
        System.out.println("final progress:" +progress);
        System.out.println("final result:"+result);

      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

}
