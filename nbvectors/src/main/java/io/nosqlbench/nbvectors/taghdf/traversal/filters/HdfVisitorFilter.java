package io.nosqlbench.nbvectors.taghdf.traversal.filters;

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

/// An experimental interface for filtering nodes during a node walk
public interface HdfVisitorFilter {

  /// return false to skip this node
  /// @param node the node to skip or visit
  /// @return false to skip this node
  boolean enterNode(Node node);

  /// return false to skip this dataset
  /// @param dataset the dataset to skip or visit
  /// @return false to skip this dataset
  boolean dataset(Dataset dataset);

  /// return false to skip this attribute
  /// @param attribute the attribute to skip or visit
  /// @return false to skip this attribute
  boolean attribute(Attribute attribute);

  /// return false to skip this committed datatype
  /// @param cdt the committed datatype to skip or visit
  /// @return false to skip this committed datatype
  boolean committedDataType(CommittedDatatype cdt);

  /// return false to skip this group
  /// @param group the group to skip or visit
  /// @return false to skip this group
  boolean enterGroup(Group group);

  /// return false to skip this file
  /// @param file the file to skip or visit
  /// @return false to skip this file
  boolean enterFile(HdfFile file);

}
