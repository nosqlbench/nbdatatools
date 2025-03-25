package io.nosqlbench.nbvectors.spec;

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
import io.nosqlbench.nbvectors.commands.verify_knn.options.DistanceFunction;
import io.nosqlbench.nbvectors.common.Templatizer;
import io.nosqlbench.nbvectors.spec.access.datasets.BaseVectors;
import io.nosqlbench.nbvectors.spec.access.datasets.DatasetView;
import io.nosqlbench.nbvectors.spec.access.datasets.FloatVectorsDataset;
import io.nosqlbench.nbvectors.spec.access.datasets.NeighborDistances;
import io.nosqlbench.nbvectors.spec.access.datasets.NeighborIndices;
import io.nosqlbench.nbvectors.spec.access.datasets.QueryVectors;

import java.nio.file.Path;

import java.net.URL;
import java.util.Optional;

/// This is the entry to consuming Vector Test Data according to the documented spec.
/// This is the de-facto reference implementation of an accessor API that uses the documented
/// HDF5 data format. When there are any conflicts between this and the provided documentation,
/// then this implement should take precedence.
///
/// In a future edition, the documentation should be derived directly from this reference
/// implementation and the accompanying Javadoc.
public class VectorData {

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
  public DatasetView<? extends Number> getBaseVectors() {
    Dataset dataset = getFirstDataset(SpecDatasets.base_vectors.name(), "train");
    Class<?> javaType = dataset.getDataType().getJavaType();
    if (javaType == float.class || javaType == Float.class) {
      return new FloatVectorsDatasetImpl(dataset);
    } else if (javaType == double.class || javaType == Double.class) {
      return new DoubleVectorsDatasetImpl(dataset);
    } else {
      throw new RuntimeException("unsupported vector type: " + javaType);
    }
  }

  private Dataset getFirstDataset(String... names) {
    Dataset dataset = null;
    for (String name : names) {
      try {
        dataset = hdfFile.getDatasetByPath(name);
        return dataset;
      } catch (HdfInvalidPathException ignored) {
      }
    }
    throw new HdfInvalidPathException(
        "none of the following datasets were found: " + String.join(",", names),
        this.hdfFile.getFileAsPath()
    );
  }

  /// Get the query vectors dataset
  /// @return the query vectors dataset
  public QueryVectors getQueryVectors() {
    return new QueryVectors(hdfFile.getDatasetByPath(SpecDatasets.query_vectors.name()));
  }

  /// Get the neighbor indices dataset
  /// @return the neighbor indices dataset
  public NeighborIndices getNeighborIndices() {
    return new NeighborIndices(hdfFile.getDatasetByPath(SpecDatasets.neighbor_indices.name()));
  }

  /// Get the neighbor distances dataset
  /// @return the neighbor distances dataset
  public NeighborDistances getNeighborDistances() {
    return new NeighborDistances(hdfFile.getDatasetByPath(SpecDatasets.neighbor_distances.name()));
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
    return SpecToken.valueOf(tokenName).apply(this);
  }

  /// tokenize a template string with this dataset
  /// @param template the template string to tokenize
  /// @return the tokenized string
  /// @see Templatizer
  public Optional<String> tokenize(String template) {
    return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template, template);
  }
}
