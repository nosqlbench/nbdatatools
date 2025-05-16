package io.nosqlbench.nbvectors.commands.tag_hdf5.traversal.injectors;

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

/// An experimental interface for mutating nodes during a node walk
public interface HdfVisitorInjector {

  /// either ignore or modify a node
  /// @param node the node to ignore or modify
  /// @return a list of nodes to replace this node, or null to do nothing
  List<Node> enterNode(Node node);

  /// ignore or modify a node
  /// @return a list of nodes to replace the node, or null to do nothing
  /// @param node the node to ignore or modify
  List<Node> leaveNode(Node node);

  /// ignore or modify a dataset
  /// @return a list of datasets to replace this dataset, or null to do nothing
  /// @param dataset the dataset to replace or ignore
  List<Dataset> dataset(Dataset dataset);

  /// ignore or modify an attribute
  /// @return a list of attributes to replace this attribute, or null to do nothing
  /// @param node the node to modify attributes for or ignore
  /// @param attribute the attribute to replace or ignore
  List<Attribute> attribute(Node node, Attribute attribute);

  /// ignore or modify a committed datatype
  /// @return a list of committed data types to replace this one, or null to do nothign
  /// @param cdt the committed data type to ignore or replace
  List<CommittedDatatype> committedDataType(CommittedDatatype cdt);

  /// ignore or modify a group
  /// @param group the group to ignore or replace
  /// @return a list of groups to replace this group with, or null to do nothing
  List<Group> enterGroup(Group group);

  /// ignore or modify an HDF file
  /// @return a list of HDF files to replace this one with, or null to do nothing
  /// @param file the HDF file to ignore or replace
  List<HdfFile> enterFile(HdfFile file);

  /// ignore or modify an HDF file
  /// @param file the hdf file to ignore or replace
  /// @return a list of HDF files to replace this one with, or null to do nothing
  List<HdfFile> leaveFile(HdfFile file);

  /// ignore or modify a group
  /// @return a list of groups to replace this one with, or nul to do nothing
  /// @param group the group to replace or ignore
  List<Group> leaveGroup(Group group);

  /// finish up any work
  /// @return any nodes to append at the max of buffering
  List<Node> finish();
}
