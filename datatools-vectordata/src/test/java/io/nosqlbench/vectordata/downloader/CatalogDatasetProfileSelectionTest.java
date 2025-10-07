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
import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class CatalogDatasetProfileSelectionTest {

  @Test
  public void shouldParseDatasetProfileSpecVariants() {
    DatasetProfileSpec datasetOnly = DatasetProfileSpec.parse("mnist");
    assertEquals("mnist", datasetOnly.dataset());
    assertFalse(datasetOnly.profile().isPresent());

    DatasetProfileSpec datasetAndProfile = DatasetProfileSpec.parse("mnist:train");
    assertEquals("mnist", datasetAndProfile.dataset());
    assertEquals("train", datasetAndProfile.profile().orElseThrow());

    DatasetProfileSpec escaped = DatasetProfileSpec.parse("vector\\:set:default");
    assertEquals("vector:set", escaped.dataset());
    assertEquals("default", escaped.profile().orElseThrow());

    assertThrows(IllegalArgumentException.class, () -> DatasetProfileSpec.parse("\\"));
    assertThrows(IllegalArgumentException.class, () -> DatasetProfileSpec.parse(" :profile"));
  }

  @Test
  public void shouldSelectDatasetAndOptionalProfile() throws MalformedURLException {
    RecordingProfileSelector selector = new RecordingProfileSelector("default");
    DatasetEntry entry = new StubDatasetEntry("mnist", new URL("https://example.com"), selector);
    Catalog catalog = new Catalog(List.of(entry));

    ProfileSelector plainSelector = catalog.select("mnist");
    assertSame(selector, plainSelector);
    assertTrue(plainSelector.presetProfile().isEmpty());

    ProfileSelector presetSelector = catalog.select("mnist:default");
    assertNotSame(selector, presetSelector);
    assertEquals("default", presetSelector.presetProfile().orElseThrow());

    TestDataView view = presetSelector.profile("default");
    assertEquals("default", view.getName());
    assertEquals(1, selector.profileCallCount());

    // Cached call should not increment
    presetSelector.profile("default");
    assertEquals(1, selector.profileCallCount());

    // No-arg lookup uses preset profile
    assertSame(view, presetSelector.profile());
    assertEquals(1, selector.profileCallCount());

    assertThrows(IllegalArgumentException.class, () -> presetSelector.profile("alternate"));

    presetSelector.setCacheDir("/tmp/cache");
    assertEquals("/tmp/cache", selector.cacheDir());

    // Cache invalidated on cache-dir change
    presetSelector.profile("default");
    assertEquals(2, selector.profileCallCount());

    // Direct profile helper
    TestDataView directView = catalog.profile("mnist:default");
    assertEquals("default", directView.getName());
    assertEquals(3, selector.profileCallCount());

    // Preparsed spec support
    DatasetProfileSpec spec = DatasetProfileSpec.of("mnist", "default");
    catalog.profile(spec);
    assertEquals(4, selector.profileCallCount());
  }

  private static final class StubDatasetEntry extends DatasetEntry {
    private final ProfileSelector selector;

    private StubDatasetEntry(String name, URL url, ProfileSelector selector) {
      super(name, url, Map.of(), new DSProfileGroup(), Map.of());
      this.selector = selector;
    }

    @Override
    public ProfileSelector select() {
      return selector;
    }
  }

  private static final class RecordingProfileSelector implements ProfileSelector {
    private final String expectedProfile;
    private final TestDataView view;
    private int profileCallCount;
    private String cacheDir;

    private RecordingProfileSelector(String expectedProfile) {
      this.expectedProfile = expectedProfile;
      this.view = new StubTestDataView(expectedProfile);
    }

    @Override
    public TestDataView profile(String profileName) {
      if (!expectedProfile.equals(profileName)) {
        throw new IllegalArgumentException("Unexpected profile: " + profileName);
      }
      profileCallCount++;
      return view;
    }

    @Override
    public ProfileSelector setCacheDir(String cacheDir) {
      this.cacheDir = cacheDir;
      return this;
    }

    private int profileCallCount() {
      return profileCallCount;
    }

    private String cacheDir() {
      return cacheDir;
    }
  }

  private static final class StubTestDataView implements TestDataView {
    private final String name;

    private StubTestDataView(String name) {
      this.name = name;
    }

    @Override
    public Optional<BaseVectors> getBaseVectors() {
      return Optional.empty();
    }

    @Override
    public Optional<QueryVectors> getQueryVectors() {
      return Optional.empty();
    }

    @Override
    public Optional<NeighborIndices> getNeighborIndices() {
      return Optional.empty();
    }

    @Override
    public Optional<NeighborDistances> getNeighborDistances() {
      return Optional.empty();
    }

    @Override
    public DistanceFunction getDistanceFunction() {
      return null;
    }

    @Override
    public String getLicense() {
      return null;
    }

    @Override
    public URL getUrl() {
      return null;
    }

    @Override
    public String getModel() {
      return null;
    }

    @Override
    public String getVendor() {
      return null;
    }

    @Override
    public Optional<String> lookupToken(String tokenName) {
      return Optional.empty();
    }

    @Override
    public Optional<String> tokenize(String template) {
      return Optional.empty();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Map<String, String> getTokens() {
      return Map.of();
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
      return CompletableFuture.completedFuture(null);
    }
  }
}
