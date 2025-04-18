package io.nosqlbench.vectordata.download;

import io.nosqlbench.vectordata.ProfileSelector;
import io.nosqlbench.vectordata.TestDataView;
import io.nosqlbench.vectordata.layout.manifest.DSProfile;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/// Implementation of ProfileSelector for virtual profiles.
///
/// This class provides methods for selecting and configuring profiles from a dataset entry.
/// It handles downloading and caching of datasets as needed.
public class VirtualProfileSelector implements ProfileSelector {
  private final DatasetEntry datasetEntry;
private Path cacheDir = Path.of(System.getProperty("user.home"), ".cache", "jvector");


  /// Creates a new VirtualProfileSelector for the given dataset entry.
  ///
  /// @param datasetEntry The dataset entry to select profiles from
  public VirtualProfileSelector(DatasetEntry datasetEntry) {
    this.datasetEntry = datasetEntry;
  }

  public Set<String> profiles() {
    return datasetEntry.profiles().keySet();
  }

  /// Selects a specific profile by name.
  ///
  /// @param profileName The name of the profile to select
  /// @return A TestDataView for the selected profile
  @Override
  public TestDataView profile(String profileName) {
    DSProfile profile = datasetEntry.profiles().get(profileName);
    if (profile==null) {
      profile = datasetEntry.profiles().get(profileName.toLowerCase());
    }
    if (profile==null) {
      profile = datasetEntry.profiles().get(profileName.toUpperCase());
    }
    if (profile==null) {
      throw new RuntimeException("profile " + profileName + "' not found. Available profiles: " + profiles() + ", but not " + profileName);
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
}
