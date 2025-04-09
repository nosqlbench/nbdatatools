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
import io.nosqlbench.vectordata.api.Indexed;
import io.nosqlbench.vectordata.layout.FWindow;

public class CoreDatasetViewFixture<T> extends CoreDatasetViewMethods<T>{
  public CoreDatasetViewFixture(Dataset dataset, FWindow window) {
    super(dataset, window);
  }

  @Override
  public FWindow validateWindow(FWindow window) {
    return super.validateWindow(window);
  }

  @Override
  public float[] getFloatVector(long index) {
    return super.getFloatVector(index);
  }

  @Override
  public double[] getDoubleVector(long index) {
    return super.getDoubleVector(index);
  }

  @Override
  public Indexed<T> getIndexedObject(long index) {
    return super.getIndexedObject(index);
  }

  @Override
  public Object[] getDataRange(long startInclusive, long endExclusive) {
    return super.getDataRange(startInclusive, endExclusive);
  }

  @Override
  public Object getRawElement(long index) {
    return super.getRawElement(index);
  }

  @Override
  public T slice(long index) {
    return super.slice(index);
  }

  @Override
  public T[] sliceRange(long startInclusive, long endExclusive) {
    return super.sliceRange(startInclusive, endExclusive);
  }
}
