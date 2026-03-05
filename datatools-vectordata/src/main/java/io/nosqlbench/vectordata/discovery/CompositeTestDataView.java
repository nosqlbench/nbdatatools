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

import io.nosqlbench.vectordata.discovery.metadata.MetadataContent;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.discovery.metadata.PredicateTestDataView;
import io.nosqlbench.vectordata.discovery.metadata.Predicates;
import io.nosqlbench.vectordata.discovery.metadata.ResultIndices;
import io.nosqlbench.vectordata.discovery.vector.TestDataView;
import io.nosqlbench.vectordata.discovery.vector.VectorTestDataView;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;

import java.net.URL;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// A {@link TestDataView} that composes a {@link VectorTestDataView} with a
/// {@link PredicateTestDataView}, delegating vector methods to the former and
/// predicate methods to the latter.
///
/// This is used by factory methods in {@link ProfileSelector} implementations
/// when a profile has both vector and predicate data available.
public class CompositeTestDataView implements TestDataView, AutoCloseable {

    private final VectorTestDataView vectorView;
    private final PredicateTestDataView<PNode<?>> predicateView;

    /// Creates a composite view combining vector and predicate data.
    ///
    /// @param vectorView    the vector data delegate
    /// @param predicateView the predicate data delegate
    public CompositeTestDataView(VectorTestDataView vectorView,
                                 PredicateTestDataView<PNode<?>> predicateView) {
        this.vectorView = vectorView;
        this.predicateView = predicateView;
    }

    // --- VectorTestDataView delegation ---

    @Override
    public Optional<BaseVectors> getBaseVectors() {
        return vectorView.getBaseVectors();
    }

    @Override
    public Optional<QueryVectors> getQueryVectors() {
        return vectorView.getQueryVectors();
    }

    @Override
    public Optional<NeighborIndices> getNeighborIndices() {
        return vectorView.getNeighborIndices();
    }

    @Override
    public Optional<NeighborDistances> getNeighborDistances() {
        return vectorView.getNeighborDistances();
    }

    @Override
    public Optional<NeighborIndices> getFilteredNeighborIndices() {
        return vectorView.getFilteredNeighborIndices();
    }

    @Override
    public Optional<NeighborDistances> getFilteredNeighborDistances() {
        return vectorView.getFilteredNeighborDistances();
    }

    @Override
    public DistanceFunction getDistanceFunction() {
        return vectorView.getDistanceFunction();
    }

    @Override
    public String getLicense() {
        return vectorView.getLicense();
    }

    @Override
    public URL getUrl() {
        return vectorView.getUrl();
    }

    @Override
    public String getModel() {
        return vectorView.getModel();
    }

    @Override
    public String getVendor() {
        return vectorView.getVendor();
    }

    @Override
    public Optional<String> lookupToken(String tokenName) {
        return vectorView.lookupToken(tokenName);
    }

    @Override
    public Optional<String> tokenize(String template) {
        return vectorView.tokenize(template);
    }

    @Override
    public String getName() {
        return vectorView.getName();
    }

    @Override
    public Map<String, String> getTokens() {
        return vectorView.getTokens();
    }

    // --- PredicateTestDataView delegation ---

    @Override
    public Optional<PredicateContext> getPredicateContext() {
        return predicateView.getPredicateContext();
    }

    @Override
    public Optional<Predicates<PNode<?>>> getPredicatesView() {
        return predicateView.getPredicatesView();
    }

    @Override
    public Optional<ResultIndices> getResultIndices() {
        return predicateView.getResultIndices();
    }

    @Override
    public Optional<MetadataLayout> getMetadataLayout() {
        return predicateView.getMetadataLayout();
    }

    @Override
    public Optional<MetadataContent> getMetadataContentView() {
        return predicateView.getMetadataContentView();
    }

    // --- Combined prebuffer ---

    @Override
    public CompletableFuture<Void> prebuffer() {
        CompletableFuture<Void> vectorFuture = vectorView.prebuffer();
        CompletableFuture<Void> predicateFuture = predicateView.prebuffer();
        return CompletableFuture.allOf(vectorFuture, predicateFuture);
    }

    @Override
    public void close() throws Exception {
        if (vectorView instanceof AutoCloseable) {
            ((AutoCloseable) vectorView).close();
        }
        if (predicateView instanceof AutoCloseable) {
            ((AutoCloseable) predicateView).close();
        }
    }

    @Override
    public String toString() {
        return "CompositeTestDataView{vector=" + vectorView + ", predicates=" + predicateView + "}";
    }
}
