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
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfVisitor;

/// This allows you to selectively expose HDF structure to an [HdfVisitor] implementation.
public class BaseHdfVisitorFilter implements HdfVisitorFilter {

  /// {@inheritDoc}
  @Override
  public boolean enterNode(Node node) {
    return true;
  }

  /// {@inheritDoc}
  @Override
  public boolean dataset(Dataset dataset) {
    return true;
  }

  /// {@inheritDoc}
  @Override
  public boolean attribute(Attribute attribute) {
    return true;
  }

  /// {@inheritDoc}
  @Override
  public boolean committedDataType(CommittedDatatype cdt) {
    return true;
  }

  /// {@inheritDoc}
  @Override
  public boolean enterGroup(Group group) {
    return true;
  }

  /// {@inheritDoc}
  @Override
  public boolean enterFile(HdfFile file) {
    return true;
  }

}
