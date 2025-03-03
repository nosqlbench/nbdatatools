package io.nosqlbench.nbvectors.buildhdf5.binders;

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


import io.nosqlbench.nbvectors.buildhdf5.predicates.types.PNode;

/// An adapter must have a way to enhance an object with predicate structure
/// in order for predicates to be meshed with non-predicate operations.
/// The BASE type is generic, but could be either
///
/// 1. A template type, for which the PNode would be used for its predicate template structure
/// rather than its actual values. In this case, [#BASE] and [#ANDPRED] are the same basic type.
/// 2. A non-template type, like an actual operations, but which has the ability to be promoted
/// to a predicate-imbued form, which may be of the same or a different type.
///
/// In any case, the predicate data provided should be added to the base type to return another
/// form of the base type.
public interface Templatizer<BASE,ANDPRED> {
  ANDPRED andPredicate(BASE base, PNode<?> predicate);
}
