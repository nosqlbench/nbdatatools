package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public interface HdfVisitor {

  void enterNode(Node node);

  void leaveNode(Node node);

  void dataset(Dataset dataset);

  void attribute(Node node, Attribute attribute);

  void committedDataType(CommittedDatatype cdt);

  void enterGroup(Group group);

  void enterFile(HdfFile file);

  void leaveFile(HdfFile file);

  void leaveGroup(Group group);

  void finish();
}
