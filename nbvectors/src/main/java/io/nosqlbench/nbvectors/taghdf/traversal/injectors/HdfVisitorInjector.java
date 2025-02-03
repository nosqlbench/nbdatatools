package io.nosqlbench.nbvectors.taghdf.traversal.injectors;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.List;

public interface HdfVisitorInjector {

  List<Node> enterNode(Node node);

  List<Node> leaveNode(Node node);

  List<Dataset> dataset(Dataset dataset);

  List<Attribute> attribute(Node node, Attribute attribute);

  List<CommittedDatatype> committedDataType(CommittedDatatype cdt);

  List<Group> enterGroup(Group group);

  List<HdfFile> enterFile(HdfFile file);

  List<HdfFile> leaveFile(HdfFile file);

  List<Group> leaveGroup(Group group);

  List<Node> finish();
}
