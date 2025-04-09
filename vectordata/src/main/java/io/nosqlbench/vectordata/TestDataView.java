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
import io.nosqlbench.vectordata.internalapi.datasets.FloatVectors;
import io.nosqlbench.vectordata.internalapi.datasets.FloatVectorsImpl;
import io.nosqlbench.vectordata.internalapi.datasets.IntVectorConfigs;
import io.nosqlbench.vectordata.internalapi.datasets.IntVectors;
import io.nosqlbench.vectordata.internalapi.datasets.TestDataKind;
import io.nosqlbench.vectordata.internalapi.datasets.views.BaseVectors;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborDistances;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborIndices;
import io.nosqlbench.vectordata.internalapi.datasets.views.QueryVectors;
import io.nosqlbench.vectordata.internalapi.tokens.SpecToken;

import java.net.URL;
import java.util.Map;
import java.util.Optional;

/// This is the entry to consuming Vector Test Data from a profile according to the documented spec.
public interface TestDataView {
  /// Get the base vectors dataset
  /// @return the base vectors dataset
  Optional<BaseVectors> getBaseVectors();

  /// Get the query vectors dataset
  /// @return the query vectors dataset
  Optional<QueryVectors> getQueryVectors();

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

  public String getName();

  public Map<String, String> getTokens();
}
