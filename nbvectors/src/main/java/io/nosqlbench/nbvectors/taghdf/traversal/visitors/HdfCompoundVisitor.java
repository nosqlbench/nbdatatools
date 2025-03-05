package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

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

import java.util.ArrayList;
import java.util.List;

/// An implementation of {@link HdfVisitor} which allows for multiple visitors to be configured
/// in the same traversal
public class HdfCompoundVisitor implements HdfVisitor {

  private final List<HdfVisitor> traversers = new ArrayList<>();

  /// Add another visitor to be called as this visitor traverses HDF data
  /// @param traverser a {@link HdfVisitor} to add
  /// @return this compound visitor, for method chaining
  public HdfCompoundVisitor add(HdfVisitor traverser) {
    traversers.add(traverser);
    return this;
  }

  @Override
  public void enterNode(Node node) {
    for (HdfVisitor traverser : traversers) {
      traverser.enterNode(node);
    }
  }

  @Override
  public void leaveNode(Node node) {
    for (HdfVisitor traverser : traversers) {
      traverser.leaveNode(node);
    }
  }

  @Override
  public void dataset(Dataset dataset) {
    for (HdfVisitor traverser : traversers) {
      traverser.dataset(dataset);
    }

  }

  @Override
  public void attribute(Node node, Attribute attribute) {
    for (HdfVisitor traverser : traversers) {
      traverser.attribute(node, attribute);
    }

  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {
    for (HdfVisitor traverser : traversers) {
      traverser.committedDataType(cdt);
    }

  }

  @Override
  public void enterGroup(Group group) {
    for (HdfVisitor traverser : traversers) {
      traverser.enterGroup(group);
    }
  }

  @Override
  public void enterFile(HdfFile file) {
    for (HdfVisitor traverser : traversers) {
      traverser.enterFile(file);
    }

  }

  @Override
  public void leaveFile(HdfFile file) {
    for (HdfVisitor traverser : traversers) {
      traverser.leaveFile(file);
    }

  }

  @Override
  public void leaveGroup(Group group) {
    for (HdfVisitor traverser : traversers) {
      traverser.leaveGroup(group);
    }

  }

  @Override
  public void finish() {
    for (HdfVisitor traverser : traversers) {
      traverser.finish();
    }
  }
}
