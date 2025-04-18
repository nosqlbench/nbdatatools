package io.nosqlbench.vectordata;

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


import java.nio.file.Path;

/// Main entry point for accessing vector test data.
///
/// This class provides static methods for loading test data from files or URLs,
/// and for accessing catalogs of available test datasets.
public class VectorTestData {

//  public static TestDataProfile lookup(String path, String profile) {
//    return lookup().find().lookup(name);
//  }
//

  /// Loads test data from a file path specified as a string.
  ///
  /// @param path The path to the test data file
  /// @return A TestDataGroup containing the loaded data
  public static TestDataGroup load(String path) {
    return load(Path.of(path));
  }

  /// Loads test data from a file path.
  ///
  /// @param path The path to the test data file
  /// @return A TestDataGroup containing the loaded data
  public static TestDataGroup load(Path path) {
    return new TestDataGroup(path);
  }

  /// Returns a list of all available test data catalogs.
  ///
  /// @return A TestDataSources object containing all available catalogs
  public static TestDataSources catalogs() {
    return new TestDataSources();
  }

  /// Returns a specific test data catalog by URL.
  ///
  /// @param url The URL of the catalog to load
  /// @return A TestDataSources object containing the specified catalog
  public static TestDataSources catalog(String url) {
    return TestDataSources.ofUrl(url);
  }

  /// Looks up available test data sources.
  ///
  /// @return A TestDataSources object containing available data sources
  public static TestDataSources lookup() {
    return TestDataSources.DEFAULT;
  }
}
