/// Iterator and iterable utilities for data processing and transformation.
///
/// This package provides specialized iterator and iterable implementations for common data processing patterns,
/// including immutable iteration, flattening nested structures, converting elements, and concatenating multiple
/// iterables.
///
/// ## Key Components
///
/// - {@link io.nosqlbench.nbdatatools.api.iteration.ImmutableListIterator}: Read-only list iterator
/// - {@link io.nosqlbench.nbdatatools.api.iteration.FlatteningIterable}: Flattens nested iterables
/// - {@link io.nosqlbench.nbdatatools.api.iteration.ConvertingIterable}: Converts elements during iteration
/// - {@link io.nosqlbench.nbdatatools.api.iteration.EnumeratedIterable}: Adds enumeration to elements
/// - {@link io.nosqlbench.nbdatatools.api.iteration.ConcatenatingIterable}: Concatenates multiple iterables
///
/// ## Usage Example
///
/// ```java
/// Iterable<String> converted = new ConvertingIterable<>(numbers, Object::toString);
/// Iterable<String> flat = new FlatteningIterable<>(nestedLists);
/// ```
package io.nosqlbench.nbdatatools.api.iteration;

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
