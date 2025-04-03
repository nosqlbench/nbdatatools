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

import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.exceptions.HdfInvalidPathException;
import io.nosqlbench.vectordata.local.datasets.FloatVectors;
import io.nosqlbench.vectordata.local.datasets.attrs.RootGroupAttributes;
import io.nosqlbench.vectordata.local.datasets.views.NeighborIndices;
import io.nosqlbench.vectordata.local.datasets.SpecDatasets;
import io.nosqlbench.vectordata.local.attributes.DistanceFunction;
import io.nosqlbench.vectordata.local.datasets.FloatVectorsImpl;
import io.nosqlbench.vectordata.local.datasets.NeighborIndicesImpl;
import io.nosqlbench.vectordata.local.tokens.SpecToken;
import io.nosqlbench.vectordata.local.tokens.Templatizer;

import java.nio.file.Path;

import java.net.URL;
import java.util.Map;
import java.util.Optional;

/// This is the entry to consuming Vector Test Data according to the documented spec.
/// This is the de-facto reference implementation of an accessor API that uses the documented
/// HDF5 data format. When there are any conflicts between this and the provided documentation,
/// then this implement should take precedence.
///
/// In a future edition, the documentation should be derived directly from this reference
/// implementation and the accompanying Javadoc.
public class VectorData implements AutoCloseable {

  private final HdfFile hdfFile;

  /// create a vector data reader
  /// @param path
  ///     the path to the HDF5 file
  public VectorData(Path path) {
    this(new HdfFile(path));
  }

  /// create a vector data reader
  /// @param file
  ///     the HDF5 file
  public VectorData(HdfFile file) {
    this.hdfFile = file;
  }

  /// Get the base vectors dataset
  /// @return the base vectors dataset
  public Optional<FloatVectors> getBaseVectors() {
    return getFirstDataset(SpecDatasets.base_vectors.name(), "train").map(FloatVectorsImpl::new);
  }

  /// Get the query vectors dataset
  /// @return the query vectors dataset
  public Optional<FloatVectors> getQueryVectors() {
    return getFirstDataset(SpecDatasets.query_vectors.name(),"test").map(FloatVectorsImpl::new);
  }

  /// Get the neighbor indices dataset
  /// @return the neighbor indices dataset
  public Optional<NeighborIndices> getNeighborIndices() {
    return getFirstDataset(SpecDatasets.neighbor_indices.name(),"neighbors").map(NeighborIndicesImpl::new);
  }

  /// Get the neighbor distances dataset
  /// @return the neighbor distances dataset
  public Optional<FloatVectors> getNeighborDistances() {
    try {
      Dataset neighborDistancesDs =
          hdfFile.getDatasetByPath(SpecDatasets.neighbor_distances.name());
      return Optional.of(neighborDistancesDs).map(FloatVectorsImpl::new);
    } catch (HdfInvalidPathException e) {
      return Optional.empty();
    }
  }

  /// Get the distance function
  /// @return the distance function
  public DistanceFunction getDistanceFunction() {
    return DistanceFunction.valueOf(hdfFile.getAttribute("distance_function").getData().toString());
  }

  /// Get the license
  /// @return the license
  public String getLicense() {
    return hdfFile.getAttribute("license").getData().toString();
  }

  /// Get the vendor
  /// @return the vendor
  public String getVendor() {
    return hdfFile.getAttribute("vendor").getData().toString();
  }

  /// Get the URL
  /// @return the URL
  public URL getUrl() {
    Attribute urlAttr = hdfFile.getAttribute("url");
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
    return Optional.ofNullable(hdfFile.getAttribute("notes")).map(Attribute::getData)
        .map(String::valueOf);
  }

  /// Get the model name
  /// @return the model name
  public String getModel() {
    return hdfFile.getAttribute("model").getData().toString();
  }

  /// If there is a token with this name, return its value
  /// @param tokenName
  ///     The name of the token from {@link SpecToken}
  /// @return the token value, optionally
  public Optional<String> lookupToken(String tokenName) {
    try {
      return SpecToken.lookup(tokenName).flatMap(t -> t.apply(this));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /// tokenize a template string with this dataset
  /// @param template
  ///     the template string to tokenize
  /// @return the tokenized string
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template, template);
  }

  @Override
  public void close() throws Exception {
    hdfFile.close();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("VectorData()").append("\n").append(hdfFile.getFileAsPath()).append(") {\n");
    sb.append(" model: ").append(getModel()).append("\n");
    sb.append(" license: ").append(getLicense()).append("\n");
    sb.append(" vendor: ").append(getVendor()).append("\n");
    sb.append(" distance_function: ").append(getDistanceFunction()).append("\n");
    sb.append(" url: ").append(getUrl()).append("\n");
    getNotes().ifPresent(n -> sb.append(" notes: ").append(n).append("\n"));
    getBaseVectors().ifPresent(v -> sb.append(" base_vectors: ").append(v.toString()).append("\n"));
    getQueryVectors().ifPresent(v -> sb.append(" query_vectors: ").append(v.toString()).append("\n"));
    getNeighborIndices().ifPresent(v -> sb.append(" neighbor_indices: ").append(v.toString()).append("\n"));
    getNeighborDistances().ifPresent(v -> sb.append(" neighbor_distances: ").append(v.toString()).append("\n"));
    sb.append("}");
    return sb.toString();
  }

  private Optional<Dataset> getFirstDataset(String... names) {
    Dataset dataset = null;
    for (String name : names) {
      try {
        dataset = hdfFile.getDatasetByPath(name);
        return Optional.of(dataset);
      } catch (HdfInvalidPathException ignored) {
      }
    }
    return Optional.empty();
  }

  private Dataset getFirstDatasetRequired(String... names) {
    Optional<Dataset> firstDataset = getFirstDataset(names);
    return firstDataset.orElseThrow(() -> new HdfInvalidPathException(
        "none of the following datasets were found: " + String.join(",", names),
        this.hdfFile.getFileAsPath()
    ));
  }


  public Map<String,String> getTags() {
    return RootGroupAttributes.fromGroup(hdfFile).tags();
  }


}
