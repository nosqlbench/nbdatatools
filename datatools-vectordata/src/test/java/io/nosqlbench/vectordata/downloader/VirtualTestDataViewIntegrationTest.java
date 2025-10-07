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
import io.nosqlbench.jetty.testserver.JettyFileServerFixture;
import org.junit.jupiter.api.Assumptions;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// Integration tests for {@link VirtualTestDataView} ensuring multi-view prebuffer support.
public class VirtualTestDataViewIntegrationTest {

  @TempDir
  private Path tempDir;

  @Test
  public void shouldExposeAllStandardViewsWhenAvailable() throws Exception {
    JettyFileServerFixture server = null;
    try {
      server = new JettyFileServerFixture(JettyFileServerExtension.DEFAULT_RESOURCES_ROOT);
      server.setTempDirectory(JettyFileServerExtension.TEMP_RESOURCES_ROOT);
      server.start();
    } catch (RuntimeException e) {
      Assumptions.assumeTrue(false, "Skipping test: " + e.getMessage());
    }

    try {
      Catalog catalog = Catalog.of(TestDataSources.ofUrl(server.getBaseUrl().toString()));

      DatasetEntry dataset = catalog.datasets().stream()
          .filter(entry -> entry.name().equals("testxvec"))
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("Expected testxvec dataset in catalog"));

      ProfileSelector selector = dataset.select().setCacheDir(tempDir.toString());
      TestDataView view = selector.profile("default");

      assertTrue(view.getBaseVectors().isPresent(), "base vectors should be available");
      assertTrue(view.getQueryVectors().isPresent(), "query vectors should be available");
      assertTrue(view.getNeighborIndices().isPresent(), "neighbor indices should be available");
      assertTrue(view.getNeighborDistances().isPresent(), "neighbor distances should be available");
    } finally {
      if (server != null) {
        server.close();
      }
    }
  }
}
