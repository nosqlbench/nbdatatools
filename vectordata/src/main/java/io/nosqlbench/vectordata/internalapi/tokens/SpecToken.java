package io.nosqlbench.vectordata.internalapi.tokens;

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


import io.nosqlbench.vectordata.TestDataView;
import io.nosqlbench.vectordata.internalapi.datasets.api.NeighborIndices;
import io.nosqlbench.vectordata.internalapi.datasets.api.DatasetView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/// This is the reference implementation of valid tokens used for configuration
/// and templating with the vector test data format.
///
/// The first level of tokens are the canonical tokens. The second level are shortcuts and
/// synonyms. For users, they are treated the same when it comes to token expansion.
/// Library and tool maintainers may choose to treat them differently if needed.
public enum SpecToken implements Function<TestDataView, Optional<String>> {

  /// The number of base vectors in the dataset
  base_vectors(
      d -> d.getBaseVectors().map(DatasetView::getCount).map(String::valueOf), """
      The number of base vectors in the dataset
      """
  ),
  /// The number of query vectors in the dataset
  query_vectors(
      d -> d.getQueryVectors().map(DatasetView::getCount).map(String::valueOf), """
      The number of query vectors in the dataset
      """
  ),
  /// The number of neighborhoods provided; should be the same as query_vectors
  neighbor_indices(
      d -> d.getNeighborIndices().map(DatasetView::getCount).map(String::valueOf),
      """
          The number of neighborhoods provided ; should be the same as query_vectors
          """
  ),
  /// The model used to build this dataset
  model(
      d -> Optional.of(d.getModel()), """
      The model used to build this dataset
      """
  ),
  /// The number of components in the vector space
  dimensions(
      d -> d.getBaseVectors().map(DatasetView::getVectorDimensions).map(String::valueOf), """
      The number of components in the vector space
      """
  ),
  /// The maximum number of neighbors provided for each query vector
  max_k(
      d -> d.getNeighborIndices().map(NeighborIndices::getMaxK).map(String::valueOf), """
      The maximum number of neighbors provided for each query vector
      """
  ),
  /// The vendor of the dataset
  vendor(
      d -> Optional.of(d.getVendor()), """
      The vendor of the dataset
      """
  ),
  /// The distance function used to compute distance between vectors
  distance_function(d -> Optional.of(d.getDistanceFunction()).map(String::valueOf), """
      The distance function used to compute distance between vectors
      """
  );

  private static final <T> Optional<String> reduceRange(
      Collection<T> intype,
      Function<T, ? extends Number> f
  )
  {
    if (intype.isEmpty()) {
      return Optional.empty();
    }
    long min = intype.stream().map(f).mapToLong(Number::longValue).min().orElse(0L);
    long max = intype.stream().map(f).mapToLong(Number::longValue).max().orElse(0L);
    return Optional.of(range(min, max));
  }

  private static final String range(Number min, Number max) {
    if (min.equals(max)) {
      return String.valueOf(min);
    }
    if (min.doubleValue() > max.doubleValue()) {
      throw new RuntimeException(
          "consistency error, min (" + min + ") is more than max(" + max + ")");
    }
    return (String.valueOf(min) + "to" + String.valueOf(max));
  }

  /// Find the matching SpecToken for a given name.
  /// @param tokenName the name of the token to find
  /// @return the matching SpecToken
  public static Optional<SpecToken> lookup(String tokenName) {
    tokenName = tokenName.toLowerCase();
    SpecToken token = null;
    for (SpecToken specToken : values()) {
      if (specToken.name().equals(tokenName)) {
        token = specToken;
        break;
      }
    }
    for (SpecToken.ShortTokens shortToken : ShortTokens.values()) {
      if (shortToken.name().equals(tokenName)) {
        token = shortToken.canonicalToken;
        break;
      }
    }
    return Optional.ofNullable(token);
  }

  /// Get the token value for this token in the given dataset,
  /// whether it be a property of data or shape of data, or an attribute value.
  /// @param vectorData the dataset to get the token value from
  /// @return the token value
  @Override
  public Optional<String> apply(TestDataView vectorData) {
    if (this.accessor != null) {
      Optional<String> result = this.accessor.apply(vectorData);
      return result;
    }
    return Optional.empty();
  }

  private enum ShortTokens {
    b(SpecToken.base_vectors),
    q(SpecToken.query_vectors),
    i(SpecToken.neighbor_indices),
    k(SpecToken.max_k),
    dim(SpecToken.dimensions),
    dims(SpecToken.dimensions),
    base_count(SpecToken.base_vectors),
    maxk(SpecToken.max_k),
    vector_count(SpecToken.base_vectors),
    vectors(SpecToken.base_vectors),
    queries(SpecToken.query_vectors),
    indices(SpecToken.neighbor_indices);

    public final SpecToken canonicalToken;

    ShortTokens(SpecToken canonicalToken) {
      this.canonicalToken = canonicalToken;
    }
  }

  private final Function<TestDataView, Optional<String>> accessor;

  /// The description of the token
  public String description;

  SpecToken(Function<TestDataView, Optional<String>> accessor, String description) {
    this.accessor = accessor;
    this.description = description;
  }

  /// List the valid tokens and their synonyms.
  /// @return a list of valid tokens and their synonyms
  public static List<String> listValidTokens() {
    List<String> docs = new ArrayList<>();
    Map<SpecToken, List<ShortTokens>> readout = new LinkedHashMap<>();
    for (SpecToken token : values()) {
      readout.computeIfAbsent(
          token,
          t -> Arrays.stream(ShortTokens.values()).filter(s -> s.canonicalToken == t).toList()
      );
      for (SpecToken specToken : readout.keySet()) {
        List<ShortTokens> synonyms = readout.get(specToken);
        String synonymDetails = synonyms.isEmpty() ? "" :
            String.join(",", readout.get(specToken).stream().map(ShortTokens::name).toList());
        String doc = """
            TOKEN: DESCRIPTION SYNONYMS
            """.replaceAll("TOKEN", specToken.name())
            .replaceAll("DESCRIPTION", specToken.description)
            .replaceAll("SYNONYMS", synonymDetails);
        docs.add(doc);
      }
    }
    return docs;
  }
}
