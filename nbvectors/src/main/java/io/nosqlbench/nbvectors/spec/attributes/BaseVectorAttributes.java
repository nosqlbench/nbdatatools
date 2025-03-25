package io.nosqlbench.nbvectors.spec.attributes;

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


import io.nosqlbench.nbvectors.commands.verify_knn.options.DistanceFunction;

/// This record type captures the basic metadata requirements of the vector test data spec
/// @see DistanceFunction
/// @param dimensions the number of dimensions in each vector
/// @param count the number of vectors in the dataset
/// @param model the name of the model used to generate the data
/// @param distance_function the distance function used to compute distance between vectors
public record BaseVectorAttributes(
    long count,
    long dimensions,
    String model,
    DistanceFunction distance_function
) {
}
