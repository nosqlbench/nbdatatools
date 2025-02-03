package io.nosqlbench.nbvectors.taghdf.traversal.visitors;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public class HdfWriterVisitor extends BaseHdfVisitor {

  private final WritableHdfFile out;

  public HdfWriterVisitor(WritableHdfFile out) {
    this.out = out;
  }

  @Override
  public void dataset(Dataset dataset) {
    out.putDataset(dataset.getName(),dataset.getData());
    out.putDataset(dataset.getName(),dataset.getData());
  }

  @Override
  public void attribute(Node node, Attribute attribute) {
    out.putAttribute(attribute.getName(),attribute.getData());
  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public void enterGroup(Group group) {
    out.putGroup(group.getName());
  }

  @Override
  public void leaveFile(HdfFile file) {
    out.close();
  }

  @Override
  public void finish() {
  }

}
