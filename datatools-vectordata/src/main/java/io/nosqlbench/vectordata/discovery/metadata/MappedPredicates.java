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

import io.nosqlbench.vectordata.spec.datasets.types.MappedDatasetView;

import java.util.function.Function;

/// A [Predicates] view that maps elements from a source [Predicates] through a function.
///
/// This is a thin subclass of [MappedDatasetView] that adds the [Predicates] marker
/// interface, allowing mapped predicate datasets to be used wherever `Predicates<T>`
/// is expected.
///
/// @param <S> the source element type
/// @param <T> the target element type after mapping
class MappedPredicates<S, T> extends MappedDatasetView<S, T> implements Predicates<T> {

    /// Creates a new mapped predicates view.
    ///
    /// @param source the underlying predicates dataset to delegate to
    /// @param mapper the function applied to each element from the source
    MappedPredicates(Predicates<S> source, Function<S, T> mapper) {
        super(source, mapper);
    }
}
