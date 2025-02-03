package io.nosqlbench.nbvectors.taghdf.traversal.injectors;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

import java.util.List;

public class BaseHdfVisitorInjector implements HdfVisitorInjector {
  @Override
  public List<Node> enterNode(Node node) {
    return List.of();
  }

  @Override
  public List<Node> leaveNode(Node node) {
    return List.of();
  }

  @Override
  public List<Dataset> dataset(Dataset dataset) {
    return List.of();
  }

  @Override
  public List<Attribute> attribute(Node node, Attribute attribute) {
    return List.of();
  }

  @Override
  public List<CommittedDatatype> committedDataType(CommittedDatatype cdt) {
    return List.of();
  }

  @Override
  public List<Group> enterGroup(Group group) {
    return List.of();
  }

  @Override
  public List<HdfFile> enterFile(HdfFile file) {
    return List.of();
  }

  @Override
  public List<HdfFile> leaveFile(HdfFile file) {
    return List.of();
  }

  @Override
  public List<Group> leaveGroup(Group group) {
    return List.of();
  }

  @Override
  public List<Node> finish() {
    return List.of();
  }
}
