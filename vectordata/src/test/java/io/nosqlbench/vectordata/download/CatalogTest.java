package io.nosqlbench.vectordata.download;

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


import io.nosqlbench.vectordata.TestDataSources;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class CatalogTest {

  @Test
  public void testLayoutDownloadAndRealization() {
    TestDataSources sources = TestDataSources.ofUrl(
        "https://jvector-datasets-shared.s3.us-east-1.amazonaws.com/faed719b5520a075f2281efb8c820834/ANN_SIFT1B/");
    Catalog catalog = sources.catalog();
    List<DatasetEntry> dsentries = catalog.datasets();
    dsentries.forEach(System.out::println);
    Optional<DatasetEntry> dsOpt = catalog.findExact("ANN_SIFT1B");
    dsOpt.ifPresent(ds -> {
      System.out.println("Found dataset: " + ds.name());
      System.out.println("Attributes: " + ds.attributes());
    });


  }


  @Disabled
  @Test
  public void testDatasetDownload() {
    TestDataSources sources =
        TestDataSources.ofUrl("https://jvector-datasets-public.s3.us-east-1.amazonaws.com/");
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
