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


import io.jhdf.api.Dataset;

/// Base content to be embedded in the test data
public class BaseContentImpl extends CoreDatasetViewMethods<Object> {

  /// create a base content reader
  /// @param dataset the dataset to read
  public BaseContentImpl(Dataset dataset) {
    super(dataset);
  }
}
