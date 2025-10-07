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


import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.impl.hdf5.NeighborIndicesHdf5Impl;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.spec.datasets.impl.hdf5.BaseVectorsHdf5Impl;
import io.nosqlbench.vectordata.spec.datasets.impl.hdf5.NeighborDistancesHdf5Impl;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.spec.datasets.impl.hdf5.QueryVectorsHdf5Impl;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vectordata.spec.tokens.SpecToken;
import io.nosqlbench.vectordata.spec.tokens.Templatizer;
import io.nosqlbench.vectordata.layout.FView;

import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// Implementation of TestDataView that provides access to vector data based on a profile configuration.
///
/// ProfileDataView uses a TestDataGroup and FProfiles configuration to provide access to
/// vector datasets and their associated metadata. It maps profile configurations to the
/// appropriate datasets in the data group.
public class HDF5ProfileDataView implements TestDataView {
  /// The data group containing the datasets
  private final TestDataGroup datagroup;
  /// The profile configuration specifying how to access the datasets
  private final FProfiles profile;

  /// Creates a new ProfileDataView with the specified data group and profile.
  ///
  /// @param group The data group containing the datasets
  /// @param profile The profile configuration specifying how to access the datasets
  public HDF5ProfileDataView(TestDataGroup group, FProfiles profile) {
    this.datagroup = group;
    this.profile = profile;
  }

  /// Gets the base vectors dataset based on the profile configuration.
  ///
  /// @return An Optional containing the base vectors dataset, or empty if not available
  @Override
  public Optional<BaseVectors> getBaseVectors() {
    FView baseView = profile.views().get(TestDataKind.base_vectors.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new BaseVectorsHdf5Impl(
        (Dataset) n,
        profile.views().get(TestDataKind.base_vectors.name()).window()
    ));
//
//    return datagroup.getFirstDataset(TestDataKind.base_vectors.name(), "test")
//        .map(n -> new BaseVectorsImpl(
//            (Dataset) n,
//            profile.views().get(TestDataKind.base_vectors.name()).window()
//        ));
  }

  /// Gets the query vectors dataset based on the profile configuration.
  ///
  /// @return An Optional containing the query vectors dataset, or empty if not available
  @Override
  public Optional<QueryVectors> getQueryVectors() {
    FView baseView = profile.views().get(TestDataKind.query_vectors.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new QueryVectorsHdf5Impl(
        (Dataset) n,
        profile.views().get(TestDataKind.query_vectors.name()).window()
    ));
//    return datagroup.getFirstDataset(TestDataKind.query_vectors.name(), "test")
//        .map(n -> new QueryVectorsImpl(
//            (Dataset) n,
//            profile.views().get(TestDataKind.query_vectors.name()).window()
//        ));
  }

  /// Gets the neighbor indices dataset based on the profile configuration.
  ///
  /// @return An Optional containing the neighbor indices dataset, or empty if not available
  @Override
  public Optional<NeighborIndices> getNeighborIndices() {
    FView baseView = profile.views().get(TestDataKind.neighbor_indices.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new NeighborIndicesHdf5Impl(
        (Dataset) n,
        profile.views().get(TestDataKind.neighbor_indices.name()).window()
    ));

//    return datagroup.getFirstDataset(TestDataKind.neighbor_indices.name(), "ground_truth")
//        .map(n -> new NeighborIndicesImpl(
//            (Dataset) n,
//            profile.views().get(TestDataKind.neighbor_indices.name()).window()
//        ));
  }

  /// Gets the neighbor distances dataset based on the profile configuration.
  ///
  /// @return An Optional containing the neighbor distances dataset, or empty if not available
  @Override
  public Optional<NeighborDistances> getNeighborDistances() {

    FView baseView = profile.views().get(TestDataKind.neighbor_indices.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new NeighborDistancesHdf5Impl(
        (Dataset) n,
        profile.views().get(TestDataKind.neighbor_indices.name()).window()
    ));

//
//    return datagroup.getFirstDataset(TestDataKind.neighbor_distances.name(), "ground_truth")
//        .map(n -> new NeighborDistancesImpl(
//            (Dataset) n,
//            profile.views().get(TestDataKind.neighbor_distances.name()).window()
//        ));
  }

  /// Gets the distance function used for this dataset.
  ///
  /// @return The distance function
  @Override
  public DistanceFunction getDistanceFunction() {
    return datagroup.getDistanceFunction();
  }

  /// Gets the license information for this dataset.
  ///
  /// @return The license text
  @Override
  public String getLicense() {
    return datagroup.getAttribute("license").getData().toString();
  }

  /// Get the vendor
  /// @return the vendor
  public String getVendor() {
    return datagroup.getAttribute("vendor").getData().toString();
  }

  /// Gets the URL associated with this dataset.
  ///
  /// @return The URL, or null if not available
  @Override
  public URL getUrl() {
    Attribute urlAttr = datagroup.getAttribute("url");
    if (urlAttr == null) {
      return null;
    }
    try {
      return new URL(urlAttr.getData().toString());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /// Get the notes
  /// @return the notes
  public Optional<String> getNotes() {
    return Optional.ofNullable(datagroup.getAttribute("notes")).map(Attribute::getData)
        .map(String::valueOf);
  }

  /// Gets the model name associated with this dataset.
  ///
  /// @return The model name
  @Override
  public String getModel() {
    return this.datagroup.getAttribute("model").getData().toString();
  }

  /// Looks up a token value by name.
  ///
  /// @param tokenName The name of the token to look up
  /// @return An Optional containing the token value, or empty if not found
  @Override
  public Optional<String> lookupToken(String tokenName) {
    try {
      return SpecToken.lookup(tokenName).flatMap(t -> t.apply(this));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /// Tokenizes a template string using the tokens available in this dataset.
  ///
  /// @param template The template string to tokenize
  /// @return An Optional containing the tokenized string, or empty if tokenization failed
  @Override
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
  }

  /// Gets the name of this dataset.
  ///
  /// @return The dataset name
  @Override
  public String getName() {
    return datagroup.getName();
  }

  /// Gets all available tokens for this dataset.
  ///
  /// @return A map of token names to token values
  @Override
  public Map<String, String> getTokens() {
    Map<String, String> tokens = new java.util.LinkedHashMap<>();
    for (SpecToken specToken : SpecToken.values()) {
      specToken.apply(this).ifPresent(t -> tokens.put(specToken.name(), t));
    }
    return tokens;
  }

  /// Prebuffers all datasets in this test data view.
  /// For HDF5 files, this prebuffers the whole file for each available dataset.
  ///
  /// @return A future that completes when all prebuffering is done
  @Override
  public CompletableFuture<Void> prebuffer() {
    CompletableFuture<Void> baseVectorsFuture = getBaseVectors()
        .map(baseVectors -> baseVectors.prebuffer())
        .orElse(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> queryVectorsFuture = getQueryVectors()
        .map(queryVectors -> queryVectors.prebuffer())
        .orElse(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> neighborIndicesFuture = getNeighborIndices()
        .map(neighborIndices -> neighborIndices.prebuffer())
        .orElse(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> neighborDistancesFuture = getNeighborDistances()
        .map(neighborDistances -> neighborDistances.prebuffer())
        .orElse(CompletableFuture.completedFuture(null));

    return CompletableFuture.allOf(
        baseVectorsFuture,
        queryVectorsFuture,
        neighborIndicesFuture,
        neighborDistancesFuture
    );
  }

}
