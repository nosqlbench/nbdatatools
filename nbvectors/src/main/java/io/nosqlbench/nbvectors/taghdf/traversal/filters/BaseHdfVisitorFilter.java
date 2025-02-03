package io.nosqlbench.nbvectors.taghdf.traversal.filters;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.nbvectors.taghdf.traversal.visitors.HdfVisitor;

/// This allows you to selectively expose HDF structure to
/// an [HdfVisitor] impl.
public class BaseHdfVisitorFilter implements HdfVisitorFilter {
  
  @Override
  public boolean enterNode(Node node) {
    return true;
  }

  @Override
  public boolean leaveNode(Node node) {
    return true;
  }

  @Override
  public boolean dataset(Dataset dataset) {
    return true;
  }

  @Override
  public boolean attribute(Attribute attribute) {
    return true;
  }

  @Override
  public boolean committedDataType(CommittedDatatype cdt) {
    return true;
  }

  @Override
  public boolean enterGroup(Group group) {
    return true;
  }

  @Override
  public boolean enterFile(HdfFile file) {
    return true;
  }

  @Override
  public boolean leaveFile(HdfFile file) {
    return true;
  }

  @Override
  public boolean leaveGroup(Group group) {
    return true;
  }
}
