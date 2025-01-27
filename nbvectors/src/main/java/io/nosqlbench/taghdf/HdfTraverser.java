package io.nosqlbench.taghdf;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public interface HdfTraverser {

  void enterNode(Node node);

  void leaveNode(Node node);

  void dataset(Dataset dataset);

  void attribute(Attribute attribute);

  void committedDataType(CommittedDatatype cdt);

  void enterGroup(Group group);

  void enterFile(HdfFile file);

  void leaveFile(HdfFile file);

  void leaveGroup(Group group);
}
