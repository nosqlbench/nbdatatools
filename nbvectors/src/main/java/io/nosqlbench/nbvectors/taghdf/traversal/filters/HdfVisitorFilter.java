package io.nosqlbench.nbvectors.taghdf.traversal.filters;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public interface HdfVisitorFilter {

  boolean enterNode(Node node);

  boolean leaveNode(Node node);

  boolean dataset(Dataset dataset);

  boolean attribute(Attribute attribute);

  boolean committedDataType(CommittedDatatype cdt);

  boolean enterGroup(Group group);

  boolean enterFile(HdfFile file);

  boolean leaveFile(HdfFile file);

  boolean leaveGroup(Group group);
}
