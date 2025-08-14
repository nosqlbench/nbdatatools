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
  /// Selects a specific profile by name. If a string is provided that has colons in it, then
  /// implementors should take only the last word after the last colon as the effective profile
  /// name. Otherwise if a single word is provided, and it matches the name of the current
  /// dataset entry, then the value "default" should be taken as the effective profile name. If
  /// the word is not recognized as the name of the current dataset entry, then the value is the
  /// effective profile name.
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
