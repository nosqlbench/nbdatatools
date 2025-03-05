package io.nosqlbench.nbvectors.taghdf.traversal;

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
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.nbvectors.taghdf.traversal.filters.BaseHdfVisitorFilter;
import io.nosqlbench.nbvectors.taghdf.traversal.filters.HdfVisitorFilter;
import io.nosqlbench.nbvectors.taghdf.traversal.injectors.BaseHdfVisitorInjector;
import io.nosqlbench.nbvectors.taghdf.traversal.injectors.HdfVisitorInjector;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfCompoundVisitor;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfVisitor;

/// This walks the structure of an HDF5 file, calling the appropriate methods on the visitors.
///
/// - {@link HdfVisitor} implementations will see all nodes.
///   - Multiple visitors can be provided via {@link HdfCompoundVisitor}
/// - {@link HdfVisitorFilter} implementations can be used to filter which nodes are seen by the
/// visitors.
/// - {@link HdfVisitorInjector} implementations can be used to inject additional nodes into the
/// traversal.
///
/// Injectors and Filters take effect before other visitors are called. As long as the changes
/// are disjointed between filters and injectors, no conflicts should occur. However, for
/// injectors, if multiple injectors are used in a way that one may depend on new nodes from
/// another, then their implementation will need to be layered accordingly, and order of layering
/// will matter.
public class HdfTraverser {
  private final HdfVisitorFilter filter;

  /// Default traverser which does not filtering or injecting. All [HdfVisitor]s will see all nodes.
  public HdfTraverser() {
    this(new BaseHdfVisitorFilter(), new BaseHdfVisitorInjector());
  }

  /// create a filtering traverser.
  /// {@link HdfVisitor}s will only see nodes which pass the filter.
  /// @param filter
  ///     the filter to use
  public HdfTraverser(HdfVisitorFilter filter) {
    this(filter, new BaseHdfVisitorInjector());
  }

  /// create an injecting traverser.
  /// {@link HdfVisitor}s will see nodes which are injected by the injector.
  /// @param injector
  ///     the injector to use
  public HdfTraverser(HdfVisitorInjector injector) {
    this(new BaseHdfVisitorFilter(), injector);
  }

  /// create a filtering and injecting traverser.
  /// {@link HdfVisitor}s will see nodes which are injected by the injector, and which pass the
  /// filter.
  /// @param filter
  ///     the filter to use
  /// @param injector
  ///     the injector to use
  public HdfTraverser(HdfVisitorFilter filter, HdfVisitorInjector injector) {
    this.filter = filter;
  }

  /// traverse the HDF5 file, calling the appropriate methods on the visitor.
  /// @param node
  ///     the node to traverse
  /// @param traverser
  ///     the visitor to call
  public void traverse(Node node, HdfVisitor traverser) {
    if (filter.enterNode(node)) {

      traverser.enterNode(node);

      switch (node) {
        case HdfFile file -> {
          if (filter.enterFile(file)) {
            traverser.enterFile(file);
            for (Node fileElement : file.getChildren().values()) {
              traverse(fileElement, traverser);
            }
            traverser.leaveFile(file);
          }
        }
        case Group group -> {
          if (filter.enterGroup(group)) {
            traverser.enterGroup(group);
            for (Node groupElement : group.getChildren().values()) {
              traverse(node, traverser);
            }
            traverser.leaveGroup(group);
          }
        }
        case Dataset dataset -> {
          if (filter.dataset(dataset)) {
            traverser.dataset(dataset);
          }
        }
        case CommittedDatatype cdt -> {
          if (filter.committedDataType(cdt)) {
            traverser.committedDataType(cdt);
          }
        }
        default -> throw new RuntimeException("Unrecognized node type: " + node);
      }

      traverser.leaveNode(node);
    }

  }
}
