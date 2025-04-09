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


import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.internalapi.attributes.DistanceFunction;
import io.nosqlbench.vectordata.internalapi.datasets.impl.BaseVectorsImpl;
import io.nosqlbench.vectordata.internalapi.datasets.impl.NeighborDistancesImpl;
import io.nosqlbench.vectordata.internalapi.datasets.impl.NeighborIndicesImpl;
import io.nosqlbench.vectordata.internalapi.datasets.TestDataKind;
import io.nosqlbench.vectordata.internalapi.datasets.impl.QueryVectorsImpl;
import io.nosqlbench.vectordata.internalapi.datasets.views.BaseVectors;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborDistances;
import io.nosqlbench.vectordata.internalapi.datasets.views.NeighborIndices;
import io.nosqlbench.vectordata.internalapi.datasets.views.QueryVectors;
import io.nosqlbench.vectordata.internalapi.tokens.SpecToken;
import io.nosqlbench.vectordata.internalapi.tokens.Templatizer;
import io.nosqlbench.vectordata.layout.FSource;
import io.nosqlbench.vectordata.layout.FView;

import java.net.URL;
import java.util.Map;
import java.util.Optional;

public class ProfileDataView implements TestDataView {
  private final TestDataGroup datagroup;
  private final FProfiles profile;

  public ProfileDataView(TestDataGroup group, FProfiles profile) {
    this.datagroup = group;
    this.profile = profile;
  }

  @Override
  public Optional<BaseVectors> getBaseVectors() {
    FView baseView = profile.views().get(TestDataKind.base_vectors.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new BaseVectorsImpl(
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

  @Override
  public Optional<QueryVectors> getQueryVectors() {
    FView baseView = profile.views().get(TestDataKind.query_vectors.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new QueryVectorsImpl(
        (Dataset) n,
        profile.views().get(TestDataKind.query_vectors.name()).window()
    ));
//    return datagroup.getFirstDataset(TestDataKind.query_vectors.name(), "test")
//        .map(n -> new QueryVectorsImpl(
//            (Dataset) n,
//            profile.views().get(TestDataKind.query_vectors.name()).window()
//        ));
  }

  @Override
  public Optional<NeighborIndices> getNeighborIndices() {
    FView baseView = profile.views().get(TestDataKind.neighbor_indices.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new NeighborIndicesImpl(
        (Dataset) n,
        profile.views().get(TestDataKind.neighbor_indices.name()).window()
    ));

//    return datagroup.getFirstDataset(TestDataKind.neighbor_indices.name(), "ground_truth")
//        .map(n -> new NeighborIndicesImpl(
//            (Dataset) n,
//            profile.views().get(TestDataKind.neighbor_indices.name()).window()
//        ));
  }

  @Override
  public Optional<NeighborDistances> getNeighborDistances() {

    FView baseView = profile.views().get(TestDataKind.neighbor_indices.name());
    String dsname = baseView.source().inpath();
    Optional<Dataset> dataset = datagroup.getFirstDataset(dsname, "/sources/" + dsname);
    return dataset.map(n -> new NeighborDistancesImpl(
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

  @Override
  public DistanceFunction getDistanceFunction() {
    return datagroup.getDistanceFunction();
  }

  @Override
  public String getLicense() {
    return datagroup.getAttribute("license").getData().toString();
  }

  /// Get the vendor
  /// @return the vendor
  public String getVendor() {
    return datagroup.getAttribute("vendor").getData().toString();
  }

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

  @Override
  public String getModel() {
    return this.datagroup.getAttribute("model").getData().toString();
  }

  @Override
  public Optional<String> lookupToken(String tokenName) {
    try {
      return SpecToken.lookup(tokenName).flatMap(t -> t.apply(this));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  @Override
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
  }

  @Override
  public String getName() {
    return datagroup.getName();
  }

  @Override
  public Map<String, String> getTokens() {
    Map<String, String> tokens = new java.util.LinkedHashMap<>();
    for (SpecToken specToken : SpecToken.values()) {
      specToken.apply(this).ifPresent(t -> tokens.put(specToken.name(), t));
    }
    return tokens;
  }


}
