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


/// This record type captures attribute requirements for hte query vectors dataset
/// @param model the name of the model used to generate the data
/// @param count the number of query vectors
/// @param dimensions the number of dimensions in each query vector
public record QueryVectorsAttributes(
    String model,
    long count,
    int dimensions
) {
}
