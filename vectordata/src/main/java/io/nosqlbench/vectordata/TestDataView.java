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


import io.nosqlbench.vectordata.internalapi.attributes.DistanceFunction;
import io.nosqlbench.vectordata.internalapi.datasets.api.BaseVectors;
import io.nosqlbench.vectordata.internalapi.datasets.api.NeighborDistances;
import io.nosqlbench.vectordata.internalapi.datasets.api.NeighborIndices;
import io.nosqlbench.vectordata.internalapi.datasets.api.QueryVectors;
import io.nosqlbench.vectordata.internalapi.tokens.SpecToken;

import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/// This is the entry to consuming Vector Test Data from a profile according to the documented spec.
///
/// TestDataView provides access to vector datasets and their associated metadata, including
/// base vectors, query vectors, neighbor indices, and distance metrics. It also provides
/// methods for accessing metadata like license, vendor, and model information.
public interface TestDataView {
  /// Get the base vectors dataset
  /// @return the base vectors dataset
  Optional<BaseVectors> getBaseVectors();

  /// Get the query vectors dataset
  /// @return the query vectors dataset
  Optional<QueryVectors> getQueryVectors();

  /// Get the neighbor indices dataset
  /// @return the neighbor indices dataset
  Optional<NeighborIndices> getNeighborIndices();

  /// Get the neighbor distances dataset
  /// @return the neighbor distances dataset
  Optional<NeighborDistances> getNeighborDistances();

  /// Get the distance function
  /// @return the distance function
  DistanceFunction getDistanceFunction();

  /// Get the license
  /// @return the license
  String getLicense();

  /// Get the URL
  /// @return the URL
  URL getUrl();

  /// Get the model name
  /// @return the model name
  String getModel();

  /// Get the vendor name
  /// @return the vendor name
  String getVendor();

  /// If there is a token with this name, return its value
  /// @param tokenName
  ///     The name of the token from {@link SpecToken}
  /// @return the token value, optionally
  Optional<String> lookupToken(String tokenName);

  /// tokenize a template string with this dataset
  /// @param template
  ///     the template string to tokenize
  /// @return the tokenized string
  Optional<String> tokenize(String template);

  /// Get the name of this test data view
  /// @return the name of this test data view
  public String getName();

  /// Get all available tokens for this test data view
  /// @return a map of token names to token values
  public Map<String, String> getTokens();
}
