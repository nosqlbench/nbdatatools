package io.nosqlbench.nbvectors.taghdf.traversal;

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
/// - [HdfVisitor] implementations will see all nodes. Multiple visitors can be provided via
/// [HdfCompoundVisitor]
/// - [HdfVisitorFilter] implementations can be used to filter which nodes are seen by the
/// visitors.
/// - [HdfVisitorInjector] implementations can be used to inject additional nodes into the
/// traversal.]
///
/// Injectors and Filters take effect before other visitors are called. As long as the changes
/// are disjoint between filters and injectors, no conflicts should occur. However, for
/// injectors, if multiple injectors are used in a way that one may depend on new nodes from
/// another, then their implementation will need to be layered accordingly, and order of layering
/// will matter.
public class HdfTraverser {
  private final HdfVisitorInjector injector;
  private final HdfVisitorFilter filter;

  /// Default traverser which does not filtering or injecting. All [HdfVisitor]s will see all nodes.
  public HdfTraverser() {
    this(new BaseHdfVisitorFilter(), new BaseHdfVisitorInjector());
  }
  ///  Filtering traverser. [HdfVisitor]s will only see nodes which pass the filter.
  public HdfTraverser(HdfVisitorFilter filter) {
    this(filter, new BaseHdfVisitorInjector());
  }
  /// Injecting traverser. [HdfVisitor]s will see nodes which are injected by the injector.
  public HdfTraverser(HdfVisitorInjector injector) {
    this(new BaseHdfVisitorFilter(), injector);
  }

  public HdfTraverser(HdfVisitorFilter filter, HdfVisitorInjector injector) {
    this.filter = filter;
    this.injector = injector;
  }

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