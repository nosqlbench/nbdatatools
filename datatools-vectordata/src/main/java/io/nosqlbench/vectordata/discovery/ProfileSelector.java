package io.nosqlbench.vectordata.discovery;

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

import io.nosqlbench.vectordata.discovery.metadata.PredicateTestDataView;
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;

import java.util.Optional;
import java.util.Set;

/// Interface for selecting and configuring dataset profiles.
///
/// This interface provides methods for selecting specific profiles from a dataset
/// and configuring how they are accessed.
public interface ProfileSelector extends AutoCloseable {
  /// Selects a specific profile by name. If a string is provided that has colons in it, then
  /// implementors should take only the last word after the last colon as the effective profile
  /// name. Otherwise if a single word is provided, and it matches the name of the current
  /// dataset entry, then the value "default" should be taken as the effective profile name. If
  /// the word is not recognized as the name of the current dataset entry, then the value is the
  /// effective profile name.
  ///
  /// @param profileName The name of the profile to select
  /// @return A TestDataView for the selected profile
  VectorTestDataView profile(String profileName);
  /// Sets the cache directory for downloaded datasets.
  ///
  /// @param cacheDir The directory to use for caching
  /// @return This ProfileSelector for method chaining
  ProfileSelector setCacheDir(String cacheDir);

  /// Returns the preset profile name if one was bound at construction time.
  /// Implementations that don't pre-bind a profile can rely on the default empty optional.
  ///
  /// @return The preset profile name, or empty if not preset
  default Optional<String> presetProfile() {
    return Optional.empty();
  }

  /// Resolve the preset profile, if one was provided. This is primarily intended for callers who
  /// used a dataset:profile spec and would otherwise have to repeat the profile name. Implementations
  /// can override this for caching or specialized behavior.
  ///
  /// @return the VectorTestDataView for the preset profile
  default VectorTestDataView profile() {
    return presetProfile()
        .map(this::profile)
        .orElseThrow(() -> new IllegalStateException("No preset profile specified"));
  }

  /// Selects a predicate profile by name, returning an empty Optional if this dataset has no
  /// predicate facets for the given profile.
  ///
  /// @param profileName The name of the profile to select
  /// @return An Optional containing the predicate view, or empty if unavailable
  default Optional<PredicateTestDataView<?>> predicateProfile(String profileName) {
    return Optional.empty();
  }

  /// Provides the set of known profile names for this selector.
  /// Implementations should preserve natural ordering when possible.
  ///
  /// @return the set of known profile names
  default Set<String> profileNames() {
    return Set.of();
  }

  /// Closes this profile selector and releases any associated resources.
  /// Default implementation does nothing - subclasses should override if they need cleanup.
  ///
  /// @throws Exception If an error occurs while closing
  @Override
  default void close() throws Exception {
    // Default no-op implementation
  }
}
