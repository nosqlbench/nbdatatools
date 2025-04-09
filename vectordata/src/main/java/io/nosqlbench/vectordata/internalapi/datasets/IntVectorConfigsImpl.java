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


import java.util.LinkedHashMap;
import java.util.Map;

/// a dataset wrapper type consisting of config names to dataset views
/// @see DatasetView
public class IntVectorConfigsImpl extends LinkedHashMap<String, DatasetView<int[]>>
    implements IntVectorConfigs
{
  /// create a new integer vector configs wrapper
  /// @param datasets the datasets to wrap
  public IntVectorConfigsImpl(Map<String, DatasetView<int[]>> datasets) {
    super(datasets);
  }

  /// create a new integer vector configs wrapper with a default dataset
  /// @param defaultDataset the default dataset
  /// @return the new wrapper
  public static IntVectorConfigs ofDefault(DatasetView<int[]> defaultDataset) {
    return new IntVectorConfigsImpl(Map.of("default", defaultDataset));
  }
}
