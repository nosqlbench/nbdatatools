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
import io.nosqlbench.vectordata.layoutv2.DSProfile;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;

/// Implementation of ProfileSelector for virtual profiles.
///
/// This class provides methods for selecting and configuring profiles from a dataset entry.
/// It handles downloading and caching of datasets as needed.
public class VirtualProfileSelector implements ProfileSelector {
  private final DatasetEntry datasetEntry;
private Path cacheDir = Path.of(System.getProperty("user.home"), ".cache", "vectordata");


  /// Creates a new VirtualProfileSelector for the given dataset entry.
  ///
  /// @param datasetEntry The dataset entry to select profiles from
  public VirtualProfileSelector(DatasetEntry datasetEntry) {
    this.datasetEntry = datasetEntry;
  }

  /// Gets the set of available profile names.
  ///
  /// @return A set of strings containing all available profile names
  public Set<String> profiles() {
    return datasetEntry.profiles().keySet();
  }

  /// Gets the set of available profile names (alias for profiles()).
  ///
  /// @return A set of strings containing all available profile names
  public Set<String> getProfileNames() {
    return profiles();
  }

  @Override
  public Set<String> profileNames() {
    return new LinkedHashSet<>(profiles());
  }

  /// Selects a specific profile by name.
  ///
  /// @param profileName The name of the profile to select
  /// @return A TestDataView for the selected profile
  @Override
  public TestDataView profile(String profileName) {
    // Extract effective profile name based on the documented rules
    String effectiveProfileName = profileName;
    
    if (profileName.contains(":")) {
      // If it has colons, take the last word after the last colon
      int lastColonIndex = profileName.lastIndexOf(':');
      effectiveProfileName = profileName.substring(lastColonIndex + 1);
    } else if (profileName.equals(datasetEntry.name())) {
      // If it's a single word matching the dataset name, use "default"
      effectiveProfileName = "default";
    }
    // Otherwise, use the profileName as-is (already set above)
    
    DSProfile profile = datasetEntry.profiles().get(effectiveProfileName);
    if (profile==null) {
      profile = datasetEntry.profiles().get(effectiveProfileName.toLowerCase());
    }
    if (profile==null) {
      profile = datasetEntry.profiles().get(effectiveProfileName.toUpperCase());
    }
    if (profile==null) {
      throw new RuntimeException("profile " + effectiveProfileName + "' not found. Available profiles: " + profiles() + ", but not " + effectiveProfileName);
    }
    VirtualTestDataView view = new VirtualTestDataView( cacheDir,datasetEntry, profile);
    return view;
  }

  /// Sets the cache directory for downloaded datasets.
  ///
  /// @param cacheDir The directory to use for caching
  /// @return This ProfileSelector for method chaining
  @Override
  public ProfileSelector setCacheDir(String cacheDir) {
    this.cacheDir = Path.of(cacheDir.replace("~", System.getProperty("user.home")));
    return this;
  }

  /// Closes this profile selector.
  /// VirtualProfileSelector doesn't hold resources that need explicit cleanup.
  ///
  /// @throws Exception If an error occurs while closing
  @Override
  public void close() throws Exception {
    // No resources to clean up for virtual profile selector
  }
}
