package io.nosqlbench.vectordata.discovery.metadata;

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

import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateNode;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/// Adapts a [PredicatesASTView] (which works with raw [PredicateNode] AST elements) into a
/// [PredicateTestDataView] parameterized by a target syntax type `T`.
///
/// Each [PNode] element from the underlying AST predicates dataset is transformed
/// through the provided `syntaxAdapter` function to produce elements of type `T`.
/// The element mapping is handled by [MappedPredicates], which delegates to the
/// central [io.nosqlbench.vectordata.spec.datasets.types.MappedDatasetView] adapter.
///
/// @param <T> the target predicate syntax type
public class PredicateTestDataVernacular<T> implements PredicateTestDataView<T> {

    private final Function<PNode, T> syntaxAdapter;
    private final PredicatesASTView astView;

    /// Creates a new vernacular adapter over the given AST view.
    ///
    /// @param syntaxAdapter function that converts a [PredicateNode] to the target type `T`
    /// @param astView       the underlying AST-level predicate test data view
    public PredicateTestDataVernacular(Function<PNode, T> syntaxAdapter, PredicatesASTView astView) {
        this.syntaxAdapter = syntaxAdapter;
        this.astView = astView;
    }

    @Override
    public Optional<Predicates<T>> getPredicatesView() {
        return astView.getPredicatesView().map(source -> new MappedPredicates<>(source, syntaxAdapter));
    }

    @Override
    public Optional<ResultIndices> getResultIndices() {
        return astView.getResultIndices();
    }

    @Override
    public Optional<MetadataLayout> getMetadataLayout() {
        return astView.getMetadataLayout();
    }

    @Override
    public Optional<MetadataContent> getMetadataContentView() {
        return astView.getMetadataContentView();
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        return astView.prebuffer();
    }
}
