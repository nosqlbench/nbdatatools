package io.nosqlbench.taghdf;

import io.jhdf.CommittedDatatype;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;

public class HdfWriterTraverser implements HdfTraverser {

  private final WritableHdfFile out;

  public HdfWriterTraverser(WritableHdfFile out) {
    this.out = out;
  }

  @Override
  public void enterNode(Node node) {
  }

  @Override
  public void leaveNode(Node node) {
  }

  @Override
  public void dataset(Dataset dataset) {
    out.putDataset(dataset.getName(),dataset.getData());
  }

  @Override
  public void attribute(Attribute attribute) {
    out.putAttribute(attribute.getName(),attribute.getData());
  }

  @Override
  public void committedDataType(CommittedDatatype cdt) {
  }

  @Override
  public void enterGroup(Group group) {
    out.putGroup(group.getName());
  }

  @Override
  public void enterFile(HdfFile file) {

  }

  @Override
  public void leaveFile(HdfFile file) {
    out.close();
  }

  @Override
  public void leaveGroup(Group group) {

  }
}
