package io.nosqlbench.command.hdf5.subcommands.tag_hdf5.hdfmodel;

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


import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.NodeType;
import io.jhdf.filter.PipelineFilterWithData;
import io.jhdf.object.datatype.DataType;
import io.jhdf.object.message.DataLayout;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/// Implement a lightweight Dataset type which can do everything
/// it's underlying dataset can do except read or write data.
/// This is intended to support an in-memory model of an HDF5 file structure,
/// suitable for making in-situ changes, since interleaving modifications
/// between a source object graph and a target output stream is substantially
/// more complex. As long as the non-data elements fit into an in-memory model,
/// mutation commands can be applied to it directly, and then rendered to a new file.
///
/// This would have been easier to implement as a derived type, although the hdfBackingStorage
/// field is not accessible to use in a constructor.
public class DatasetDataProxy implements Dataset {
  private final Dataset dataset;

  /// create a dataset proxy
  /// @param dataset the dataset to proxy
  public DatasetDataProxy(
      Dataset dataset
  )
  {
    this.dataset = dataset;
  }

  @Override
  public long getSize() {
    return dataset.getSize();
  }

  @Override
  public long getSizeInBytes() {
    return dataset.getSizeInBytes();
  }

  @Override
  public long getStorageInBytes() {
    return dataset.getStorageInBytes();
  }

  @Override
  public int[] getDimensions() {
    return dataset.getDimensions();
  }

  @Override
  public boolean isScalar() {
    return dataset.isScalar();
  }

  @Override
  public boolean isEmpty() {
    return dataset.isEmpty();
  }

  @Override
  public boolean isCompound() {
    return dataset.isCompound();
  }

  @Override
  public boolean isVariableLength() {
    return dataset.isVariableLength();
  }

  @Override
  public long[] getMaxSize() {
    return dataset.getMaxSize();
  }

  @Override
  public DataLayout getDataLayout() {
    return dataset.getDataLayout();
  }

  @Override
  public Object getData() {
    throw new UnsupportedOperationException("Data access not supported in DatasetProxy");
  }

  @Override
  public Object getDataFlat() {
    throw new UnsupportedOperationException("Data access not supported in DatasetProxy");
  }

  @Override
  public Object getData(long[] sliceOffset, int[] sliceDimensions) {
    throw new UnsupportedOperationException("Data access not supported in DatasetProxy");
  }

  @Override
  public Class<?> getJavaType() {
    return dataset.getJavaType();
  }

  @Override
  public DataType getDataType() {
    return dataset.getDataType();
  }

  @Override
  public Object getFillValue() {
    return dataset.getFillValue();
  }

  @Override
  public List<PipelineFilterWithData> getFilters() {
    return dataset.getFilters();
  }

  @Override
  public Group getParent() {
    return dataset.getParent();
  }

  @Override
  public String getName() {
    return dataset.getName();
  }

  @Override
  public String getPath() {
    return dataset.getPath();
  }

  @Override
  public Map<String, Attribute> getAttributes() {
    return dataset.getAttributes();
  }

  @Override
  public Attribute getAttribute(String name) {
    return dataset.getAttribute(name);
  }

  @Override
  public NodeType getType() {
    return dataset.getType();
  }

  @Override
  public boolean isGroup() {
    return dataset.isGroup();
  }

  @Override
  public File getFile() {
    return dataset.getFile();
  }

  @Override
  public Path getFileAsPath() {
    return dataset.getFileAsPath();
  }

  @Override
  public HdfFile getHdfFile() {
    return dataset.getHdfFile();
  }

  @Override
  public long getAddress() {
    return dataset.getAddress();
  }

  @Override
  public boolean isLink() {
    return dataset.isLink();
  }

  @Override
  public boolean isAttributeCreationOrderTracked() {
    return dataset.isAttributeCreationOrderTracked();
  }
}
