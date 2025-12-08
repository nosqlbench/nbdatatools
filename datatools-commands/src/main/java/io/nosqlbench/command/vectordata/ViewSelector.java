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

package io.nosqlbench.command.vectordata;

import io.nosqlbench.vectordata.discovery.TestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DatasetView;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;

import java.util.Locale;
import java.util.Optional;

class ViewSelector {
    static Optional<DatasetView<?>> resolve(TestDataView tdv, String viewName) {
        String v = viewName.toLowerCase(Locale.ROOT);
        switch (v) {
            case "base":
            case "base_vectors":
                return tdv.getBaseVectors().map(DatasetView.class::cast);
            case "query":
            case "query_vectors":
                return tdv.getQueryVectors().map(DatasetView.class::cast);
            case "neighbor_indices":
            case "neighbors":
            case "indices":
                return tdv.getNeighborIndices().map(DatasetView.class::cast);
            case "neighbor_distances":
            case "distances":
                return tdv.getNeighborDistances().map(DatasetView.class::cast);
            default:
                return Optional.empty();
        }
    }

    static String canonicalName(DatasetView<?> view) {
        if (view instanceof BaseVectors) return "base_vectors";
        if (view instanceof QueryVectors) return "query_vectors";
        if (view instanceof NeighborIndices) return "neighbor_indices";
        if (view instanceof NeighborDistances) return "neighbor_distances";
        return "view";
    }
    static java.util.List<String> availableViews(TestDataView tdv) {
        java.util.List<String> names = new java.util.ArrayList<>();
        tdv.getBaseVectors().ifPresent(v -> names.add("base_vectors"));
        tdv.getQueryVectors().ifPresent(v -> names.add("query_vectors"));
        tdv.getNeighborIndices().ifPresent(v -> names.add("neighbor_indices"));
        tdv.getNeighborDistances().ifPresent(v -> names.add("neighbor_distances"));
        return names;
    }
}
