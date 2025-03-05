package io.nosqlbench.nbvectors.taghdf.traversalv2;

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
import io.jhdf.api.*;

import java.util.List;

/// One challenge to making many changes to an HDF5 file is that the input and output are
/// effectively stream-oriented for large files. The file can't always simply be buffered in
/// memory and then written out. A hybrid approach will be needed to improve this in the future,
/// where the structure of the file is read and retained. Then lightweight structural changes
/// are buffered and sequenced, interleaved with more batch-oriented changes (or none) for datasets.
///
///
/// These methods allow for modification of the HDF5 file structure. By default, no transformations
/// are performed. When a non-null List of elements is returned, they replace the original element.
///
/// All transform methods should return null if they do not wish to transform the subnode in any
/// way.
public interface HdfTransformer {

  /// transform a file node, or do nothing
  /// @return a list of files to replace this one with, or null to do nothing
  /// @param file the file to transform
  default List<HdfFile> transform(HdfFile file) {
    return null;
  }

  /// transform a group node, or do nothing
  /// @param group the group to transform
  /// @return a list of groups to replace this one with, or null to do nothing
  default List<Group> transform(Group group) {
    return null;
  }

  /// transform a dataset node, or do nothing
  /// @param dataset the dataset to transform
  /// @return a list of datasets to replace this one with, or null to do nothing
  default List<Dataset> transform(Dataset dataset) {
    return null;
  }

  /// transform an attribute node, or do nothing
  /// @param attribute the attribute to transform
  /// @return a list of attributes to replace this one with, or null to do nothing
  default List<Attribute> transform(Attribute attribute) {
    return null;
  }

  /// transform a committed datatype node, or do nothing
  /// @param cdt the committed datatype to transform
  /// @return a list of committed datatypes to replace this one with, or null to do nothing
  default List<CommittedDatatype> transform(CommittedDatatype cdt) {
    return null;
  }

  /// transform a node, or do nothing
  /// @param node the node to transform
  /// @return a list of nodes to replace this one with, or null to do nothing
  default List<Node> transform(Node node) {
    return null;
  }

  /// transform an attribute node, or do nothing
  /// @param parent the parent node of the dataset to transform
  /// @param attribute the attribute to transform
  /// @return a list of attributes to replace this one with, or null to do nothing
  default List<Attribute> transformAttribute(Node parent, Attribute attribute) {
    return null;
  }

  /// transform a group node, or do nothing
  /// @param parent the parent node of the group to transform
  /// @param group the group to transform
  /// @return a list of groups to replace this one with, or null to do nothing
  default List<Group> transformGroup(Node parent, Group group) {
    return null;
  }

  ;
}
