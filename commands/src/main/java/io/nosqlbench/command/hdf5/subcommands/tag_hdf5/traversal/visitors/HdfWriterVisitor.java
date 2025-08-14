package io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.visitors;

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


import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

/// a default base implementation of {@link HdfVisitor}
public class HdfWriterVisitor extends BaseHdfVisitor {

  private final WritableHdfFile out;

  /// create a writer visitor
  /// @param out the file to write to
  public HdfWriterVisitor(WritableHdfFile out) {
    this.out = out;
  }

  /// {@inheritDoc}
  @Override
  public void dataset(Dataset dataset) {
    out.putDataset(dataset.getName(),dataset.getData());
    out.putDataset(dataset.getName(),dataset.getData());
  }

  /// {@inheritDoc}
  @Override
  public void attribute(Node node, Attribute attribute) {
    out.putAttribute(attribute.getName(),attribute.getData());
  }

  /// {@inheritDoc}
  @Override
  public void committedDataType(CommittedDatatype cdt) {
    throw new RuntimeException("Not implemented yet");
  }

  /// {@inheritDoc}
  @Override
  public void enterGroup(Group group) {
    out.putGroup(group.getName());
  }

  /// {@inheritDoc}
  @Override
  public void leaveFile(HdfFile file) {
    out.close();
  }

  /// {@inheritDoc}
  @Override
  public void finish() {
  }

}
