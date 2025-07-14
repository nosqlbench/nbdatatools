package io.nosqlbench.command.hdf5.subcommands.tag_hdf5.traversal.injectors;

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
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.List;

/// Extend this class if you simply want to implement the
/// [HdfVisitorInjector] methods you care about
public class BaseHdfVisitorInjector implements HdfVisitorInjector {

  /// Create the default BaseHdfVisitorInjector
  public BaseHdfVisitorInjector() {}

  /// {@inheritDoc}
  @Override
  public List<Node> enterNode(Node node) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<Node> leaveNode(Node node) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<Dataset> dataset(Dataset dataset) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<Attribute> attribute(Node node, Attribute attribute) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<CommittedDatatype> committedDataType(CommittedDatatype cdt) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<Group> enterGroup(Group group) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<HdfFile> enterFile(HdfFile file) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<HdfFile> leaveFile(HdfFile file) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<Group> leaveGroup(Group group) {
    return List.of();
  }

  /// {@inheritDoc}
  @Override
  public List<Node> finish() {
    return List.of();
  }
}
