package io.nosqlbench.vectordata.internalapi.datasets;

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


import io.nosqlbench.vectordata.internalapi.datasets.attrs.BaseContentAttributes;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.BaseVectorAttributes;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.NeighborDistancesAttributes;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.NeighborIndicesAttributes;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.QueryPredicatesAttributes;
import io.nosqlbench.vectordata.internalapi.datasets.attrs.QueryVectorsAttributes;

import java.util.Arrays;
import java.util.Optional;

/// The datasets that are part of the vector test data format spec
public enum TestDataKind {

  /// The base vectors dataset, containing all vectors to search, organized by index
  base_vectors(
      "/base_vectors",
      "The base vectors dataset, containing all vectors to search, organized by index",
      BaseVectorAttributes.class
  ),
  /// The optional base content dataset, containing all original content, organized by index
  base_content(
      "/base_content",
      "The optional base content dataset, containing all original content, organized by index",
      BaseContentAttributes.class
  ),
  /// The query vectors dataset, containing all query vectors, organized by index
  query_vectors(
      "/query_vectors",
      "The query vectors dataset, containing all query vectors, organized by index",
      QueryVectorsAttributes.class
  ),
  /// The optional query terms dataset, containing all query terms, organized by index
  query_terms(
      "/query_terms",
      "The optional query terms dataset, containing all query terms, organized by index",
      null
  ),
  /// The optional query filters dataset, containing all query filters, organized by index
  query_filters(
      "/query_filters",
      "The optional query filters dataset, containing all query filters, organized by index",
      null
  ),
  /// The neighbor indices dataset, containing all correct KNN results, organized by index
  neighbor_indices(
      "/neighbor_indices",
      "The neighbor indices dataset, containing all correct KNN results, organized by index",
      NeighborIndicesAttributes.class
  ),
  /// The neighbor distances dataset, containing all correct distances, organized by index
  neighbor_distances(
      "/neighbor_distances",
      "The neighbor distances dataset, containing all correct distances, organized by index",
      NeighborDistancesAttributes.class
  ),
  query_predicates(
      "/query_predicates",
      "The optional query predicates dataset, containing all query predicates, organized by index",
      QueryPredicatesAttributes.class

  );

  /// The path to the dataset
  private final String path;
  /// The description of the dataset
  private final String description;
  /// The attributes of the dataset which are required, may be null, indicating no attributes are
  /// required for a given dataset
  private final Class<? extends Record> attributesClass;

  TestDataKind(String path, String description, Class<? extends Record> attributesClass)
  {
    this.path = path;
    this.description = description;
    this.attributesClass = attributesClass;
  }

  /// get the dataset path
  /// @return the path to the dataset
  public String getPath() {
    return path;
  }

  /// get the dataset description
  /// @return the description of the dataset
  public String getDescription() {
    return description;
  }

  /// get the dataset attributes type
  /// @return the attributes type of the dataset or null if none
  public Class<? extends Record> getAttributesType() {
    return attributesClass;
  }

  public static Optional<TestDataKind> fromOptionalString(String kind) {
    for (TestDataKind value : values()) {
      if (value.name().equalsIgnoreCase(kind)) {
        return Optional.of(value);
      }
    }
    for (OtherNames value : OtherNames.values()) {
      if (value.name().equalsIgnoreCase(kind)) {
        return Optional.of(value.canonical);
      }
    }
    return Optional.empty();
  }

  public static TestDataKind fromString(String kind) {
    return fromOptionalString(kind).orElseThrow(() -> new IllegalArgumentException(
        "kind '" + kind + "' is not a valid dataset kind: " + Arrays.toString(values())));
  }

  private enum OtherNames {
    base(base_vectors),
    train(base_vectors),
    queries(query_vectors),
    test(query_vectors),
    query(query_vectors),
    indices(neighbor_indices),
    neighbors(neighbor_indices),
    ground_truth(neighbor_indices),
    gt(neighbor_indices),
    distances(neighbor_distances);

    public final TestDataKind canonical;

    OtherNames(TestDataKind testDataKind) {
      this.canonical = testDataKind;
    }
  }

}
