package io.nosqlbench.nbvectors.spec;

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
import io.nosqlbench.nbvectors.spec.access.datasets.DatasetView;

public abstract class BaseVectorsDataset<T> implements DatasetView {
  private final Dataset dataset;


  public BaseVectorsDataset(Dataset dataset) {

    this.dataset = dataset;
  }

  @Override
  public int getCount() {
     return dataset.getDimensions()[0];
  }

  @Override
  public int getVectorDimensions() {
    return dataset.getDimensions()[1];
  }

  @Override
  public Class<?> getDataType() {
    return dataset.getDataType().getJavaType();
  }


}
