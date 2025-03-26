package io.nosqlbench.nbvectors.spec.views;

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


import io.nosqlbench.nbvectors.spec.VectorData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/// This is the reference implementation of valid tokens used for configuration
/// and templating with the vector test data format.
///
/// The first leve of tokens are the canonical tokens. The second level are shortcuts and
/// synonyms. For users, they are treated the same when it comes to token expansion.
/// Library and tool maintainers may choose to treat them differently if needed.
public enum SpecToken implements Function<VectorData, Optional<String>> {

  /// The number of base vectors in the dataset
  base_vectors(
      d -> d.getBaseVectors().getCount() + "", """
      The number of base vectors in the dataset
      """
  ),
  /// The number of query vectors in the dataset
  query_vectors(
      d -> d.getQueryVectors().getCount() + "", """
      The number of query vectors in the dataset
      """
  ),
  /// The number of neighborhoods provided ; should be the same as query_vectors
  neighbor_indices(
      d -> d.getNeighborIndices().getCount() + "", """
      The number of neighborhoods provided ; should be the same as query_vectors
      """
  ),
  /// The model used to build this dataset
  model(
      VectorData::getModel, """
      The model used to build this dataset
      """
  ),
  /// The number of components in the vector space
  dimensions(
      d -> d.getBaseVectors().getVectorDimensions() + "", """
      The number of components in the vector space
      """
  ),
  /// The maximum number of neighbors provided for each query vector
  max_k(
      d -> d.getNeighborIndices().getMaxK() + "", """
      The maximum number of neighbors provided for each query vector
      """
  ),
  /// The vendor of the dataset
  vendor(
      VectorData::getVendor, """
      The vendor of the dataset
      """
  );

  /// Find the matching SpecToken for a given name
  /// @param tokenName
  ///     the name of the token to find
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
  /// whether it be a property of data or shape of data, or an attribute value
  /// @param vectorData
  ///     the dataset to get the token value from
  /// @return the token value
  @Override
  public Optional<String> apply(VectorData vectorData) {
    if (this.accessor != null) {
      return Optional.of(this.accessor.apply(vectorData));
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

  private final Function<VectorData, String> accessor;

  /// The description of the token
  public String description;

  SpecToken(Function<VectorData, String> accessor, String description) {
    this.accessor = accessor;
    this.description = description;
  }

  /// List the valid tokens and their synonyms
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
