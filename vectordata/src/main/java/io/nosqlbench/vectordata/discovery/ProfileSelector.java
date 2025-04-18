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
