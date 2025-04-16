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


import java.util.Map;
import java.util.Optional;

/// a dataset consisting of arrays of float values
/// @see IntVectors
public interface IntVectorConfigs extends Map<String,DatasetView<int[]>> {
  /// get a profile by name
  /// @param name the name of the profile
  /// @return the profile
  public default Optional<DatasetView<int[]>> getProfile(String name) {
    return Optional.ofNullable(get(name));
  }
}

