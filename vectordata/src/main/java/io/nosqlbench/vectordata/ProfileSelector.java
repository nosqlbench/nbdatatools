package io.nosqlbench.vectordata;

/// Interface for selecting and configuring dataset profiles.
///
/// This interface provides methods for selecting specific profiles from a dataset
/// and configuring how they are accessed.
public interface ProfileSelector {
  /// Selects a specific profile by name.
  ///
  /// @param profileName The name of the profile to select
  /// @return A TestDataView for the selected profile
  TestDataView profile(String profileName);
  /// Sets the cache directory for downloaded datasets.
  ///
  /// @param cacheDir The directory to use for caching
  /// @return This ProfileSelector for method chaining
  ProfileSelector setCacheDir(String cacheDir);
}
