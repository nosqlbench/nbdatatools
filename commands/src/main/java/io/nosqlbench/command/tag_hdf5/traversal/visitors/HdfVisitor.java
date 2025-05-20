package io.nosqlbench.command.tag_hdf5.traversal.visitors;

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

/// implementors of this interface know how to visit all {@link Node} types
public interface HdfVisitor {

  /// visit a node before any children
  /// @param node the node to visit
  void enterNode(Node node);

  /// visit a node after all children
  /// @param node the node to visit
  void leaveNode(Node node);

  /// visit a dataset
  /// @param dataset the dataset to visit
  void dataset(Dataset dataset);

  /// visit an attribute
  /// @param node the node to visit
  /// @param attribute the attribute to visit
  void attribute(Node node, Attribute attribute);

  /// visit a committed datatype
  /// @param cdt the committed datatype to visit
  void committedDataType(CommittedDatatype cdt);

  /// visit a group before any children
  /// @param group the group to visit
  void enterGroup(Group group);

  /// visit a file before any children
  /// @param file the file to visit
  void enterFile(HdfFile file);

  /// visit a file after all children
  /// @param file the file to visit
  void leaveFile(HdfFile file);

  /// visit a group after all children
  /// @param group the group to visit
  void leaveGroup(Group group);

  /// finish up any work
  void finish();
}
